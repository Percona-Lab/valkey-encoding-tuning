package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/caio/go-tdigest/v5"
)

const (
	zsetMaxListpackValue   = "zset-max-listpack-value"
	zsetMaxListpackEntries = "zset-max-listpack-entries"
	zsetDt                 = "zset"
)

type ZSetMetrics struct {
	tdigest        *tdigest.TDigest
	objCount       int
	elementCount   int
	skipListCount  uint64
	maxElement     string
	avgElementSize float64
	maxElementSize int
}

func makeZSetMetrics() ZSetMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return ZSetMetrics{tdigest: t}
}

func (v *ValkeyNode) analyzeZSet() error {
	var cursor uint64
	for {
		entry, err := scan(v.getClient(), zsetDt, *zsetKeyPattern, cursor)
		if err != nil {
			return err
		}
		v.ZSetMetrics.objCount += len(entry.Elements)
		for _, key := range entry.Elements {
			err = v.analyzeZSetMembers(key)
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

func (v *ValkeyNode) analyzeZSetMembers(zset string) error {
	ctx := context.Background()
	client := v.getClient()
	var cursor uint64
	var isHashtable bool
	metrics := &v.ZSetMetrics
	maxLpSize, err := strconv.Atoi(v.Config[zsetMaxListpackValue])
	if err != nil {
		return err
	}
	for {
		resp := client.Do(
			ctx,
			client.B().Zscan().Key(zset).Cursor(cursor).Noscores().Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		mCount := 0
		fTotalSize := 0
		for i := 0; i < len(entry.Elements); i++ {
			mCount++
			fSize := len(entry.Elements[i])
			metrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			isHashtable = fSize >= maxLpSize
			if fSize > metrics.maxElementSize {
				metrics.maxElementSize = fSize
				metrics.maxElement = fmt.Sprintf("%s.%s", zset, entry.Elements[i])
			}
		}
		if mCount > 0 {
			metrics.avgElementSize = float64((fTotalSize + int(float64(metrics.elementCount)*metrics.avgElementSize)) / (metrics.elementCount + mCount))
			metrics.elementCount += mCount
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	if isHashtable {
		metrics.skipListCount++
	}
	return nil
}

func (v *ValkeyNode) getZSetDatatypeAnalysis(analysis *Analysis) {
	analysis.init(v.Address)
	analysis.Config[zsetMaxListpackValue] = v.Config[zsetMaxListpackValue]
	analysis.Config[zsetMaxListpackEntries] = v.Config[zsetMaxListpackEntries]
	metrics := v.ZSetMetrics
	analysis.Metrics[zsetDt] = map[string]any{
		kObjCount:       metrics.objCount,
		kSlKeyCount:     metrics.skipListCount,
		kElementsCount:  metrics.elementCount,
		kMaxElement:     metrics.maxElement,
		kMaxElementSize: metrics.maxElementSize,
		kAvgElementSize: metrics.avgElementSize,
		kDistribution:   quantileDistribution(metrics.tdigest),
	}
}

func (zm *ZSetMetrics) updateZSetStatistics(node *ZSetMetrics) {
	runningTotalField := (zm.elementCount + node.elementCount)
	runningTotalFieldSize := (float64(zm.elementCount*int(zm.avgElementSize)) + float64(node.elementCount*int(node.avgElementSize)))
	zm.avgElementSize = float64(runningTotalFieldSize / float64(runningTotalField))
	zm.elementCount = runningTotalField
	if node.maxElementSize > zm.maxElementSize {
		zm.maxElementSize = node.maxElementSize
		zm.maxElement = node.maxElement
	}
	zm.skipListCount += node.skipListCount
	zm.objCount += node.objCount
	zm.tdigest.Merge(node.tdigest)
}
