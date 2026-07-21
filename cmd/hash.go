package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/caio/go-tdigest/v5"
)

const (
	hashMaxListpack = "hash-max-listpack-value"
	hashDt          = "hash"
)

type HashMetrics struct {
	tdigest        *tdigest.TDigest
	objCount       int
	fieldCount     int
	hashTableCount uint64
	maxField       string
	avgFieldSize   float64
	maxFieldSize   int
}

func makeHashMetrics() HashMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return HashMetrics{tdigest: t}
}

func (v *ValkeyNode) analyzeHash() error {
	var cursor uint64
	for {
		entry, err := scan(v.getClient(), hashDt, *hashKeyPattern, cursor)
		if err != nil {
			return err
		}
		v.HashMetrics.objCount += len(entry.Elements)
		for _, key := range entry.Elements {
			err = v.analyzeHashField(key)
			if err != nil {
				panic(err)
			}
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	return nil

}

func (v *ValkeyNode) analyzeHashField(hash string) error {
	ctx := context.Background()
	client := v.getClient()
	var cursor uint64
	var isHashtable bool
	maxLpSize, err := strconv.Atoi(v.Config[hashMaxListpack])
	if err != nil {
		return err
	}
	for {
		resp := client.Do(
			ctx,
			client.B().Hscan().Key(hash).Cursor(cursor).Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		fCount := 0
		fTotalSize := 0
		for i := 0; i < len(entry.Elements); i += 2 {
			if fieldPatternRE != nil && !fieldPatternRE.MatchString(entry.Elements[i]) {
				continue
			}
			fCount += 2
			fSize := len(entry.Elements[i])
			vSize := len(entry.Elements[i+1])
			v.HashMetrics.tdigest.Add(float64(fSize))
			v.HashMetrics.tdigest.Add(float64(vSize))
			fTotalSize += fSize + vSize
			isHashtable = (fSize >= maxLpSize || vSize >= maxLpSize)
			if fSize > v.HashMetrics.maxFieldSize {
				v.HashMetrics.maxFieldSize = fSize
				v.HashMetrics.maxField = fmt.Sprintf("%s.%s", hash, entry.Elements[i])
			}
			if vSize > v.HashMetrics.maxFieldSize {
				v.HashMetrics.maxFieldSize = vSize
				v.HashMetrics.maxField = fmt.Sprintf("%s.%s (field name)", hash, entry.Elements[i])
			}
		}
		if fCount > 0 {
			v.HashMetrics.avgFieldSize = float64((fTotalSize + int(float64(v.HashMetrics.fieldCount)*v.HashMetrics.avgFieldSize)) / (v.HashMetrics.fieldCount + fCount))
			v.HashMetrics.fieldCount += fCount
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	if isHashtable {
		v.HashMetrics.hashTableCount++
	}

	return nil
}

func (v *ValkeyNode) getHashDatatypeAnalysis(analysis *Analysis) {
	analysis.init(v.Address)
	analysis.Config[hashMaxListpack] = v.Config[hashMaxListpack]

	analysis.Metrics[hashDt] = map[string]any{
		kObjCount:     v.HashMetrics.objCount,
		kHtKeyCount:   v.HashMetrics.hashTableCount,
		kFieldCount:   v.HashMetrics.fieldCount,
		kMaxField:     v.HashMetrics.maxField,
		kMaxFieldSize: v.HashMetrics.maxFieldSize,
		kAvgFieldSize: v.HashMetrics.avgFieldSize,
		kDistribution: quantileDistribution(v.HashMetrics.tdigest),
	}
}

func (hm *HashMetrics) updateHashStatistics(node *HashMetrics) {
	runningTotalField := (hm.fieldCount + node.fieldCount)
	runningTotalFieldSize := (float64(hm.fieldCount*int(hm.avgFieldSize)) + float64(node.fieldCount*int(node.avgFieldSize)))
	hm.avgFieldSize = float64(runningTotalFieldSize / float64(runningTotalField))
	hm.fieldCount = runningTotalField
	if node.maxFieldSize > hm.maxFieldSize {
		hm.maxFieldSize = node.maxFieldSize
		hm.maxField = node.maxField
	}
	hm.hashTableCount += node.hashTableCount
	hm.objCount += node.objCount
	hm.tdigest.Merge(node.tdigest)
}
