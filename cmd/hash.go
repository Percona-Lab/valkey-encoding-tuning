package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/caio/go-tdigest/v5"
)

const (
	hashMaxListpack = "hash-max-listpack-value"
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
	ctx := context.Background()

	client := v.getClient()
	err := client.Do(ctx, client.B().Readonly().Build()).Error()
	if err != nil {
		panic(err)
	}
	var cursor uint64
	for {
		scanCmd := client.B().Scan().Cursor(cursor)
		if *hashKeyPattern != "" {
			scanCmd.Match(*hashKeyPattern)
		}
		scanCmd.Type("hash")
		resp := client.Do(
			ctx,
			scanCmd.Build(),
		)
		entry, err := resp.AsScanEntry()
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
			fCount++
			fSize := len(entry.Elements[i+1])
			v.HashMetrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			isHashtable = fSize > +maxLpSize
			if fSize > v.HashMetrics.maxFieldSize {
				v.HashMetrics.maxFieldSize = fSize
				v.HashMetrics.maxField = fmt.Sprintf("%s.%s", hash, entry.Elements[i])
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

	analysis.Metrics["hash"] = map[string]any{
		"object_count":        v.HashMetrics.objCount,
		"hashtable_key_count": v.HashMetrics.hashTableCount,
		"field_count":         v.HashMetrics.fieldCount,
		"largest_field":       v.HashMetrics.maxField,
		"largest_field_size":  v.HashMetrics.maxFieldSize,
		"avg_field_size":      v.HashMetrics.avgFieldSize,
		"distribution":        quantileDistribution(v.HashMetrics.tdigest),
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
