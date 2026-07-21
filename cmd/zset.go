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
	objCnt         int
	elementCnt     int
	skipListCnt    uint64
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
	for ok := true; ok; ok = (cursor != 0) {
		entry, err := scan(v.getClient(), zsetDt, *zsetKeyPattern, cursor)
		if err != nil {
			return err
		}
		v.ZSetMetrics.objCnt += len(entry.Elements)
		for _, key := range entry.Elements {
			err = v.analyzeZSetMembers(key)
			if err != nil {
				panic(err)
			}
		}
		cursor = entry.Cursor
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
	for ok := true; ok; ok = (cursor != 0) {
		resp := client.Do(
			ctx,
			client.B().Zscan().Key(zset).Cursor(cursor).Noscores().Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		eleCnt := 0
		fTotalSize := 0
		for i := 0; i < len(entry.Elements); i++ {
			eleCnt++
			fSize := len(entry.Elements[i])
			metrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			isHashtable = fSize >= maxLpSize
			if fSize > metrics.maxElementSize {
				metrics.maxElementSize = fSize
				metrics.maxElement = fmt.Sprintf("%s.%s", zset, entry.Elements[i])
			}
		}
		if eleCnt > 0 {
			metrics.avgElementSize = float64((fTotalSize + int(float64(metrics.elementCnt)*metrics.avgElementSize)) / (metrics.elementCnt + eleCnt))
			metrics.elementCnt += eleCnt
		}
		cursor = entry.Cursor
	}
	if isHashtable {
		metrics.skipListCnt++
	}
	return nil
}

func (v *ValkeyNode) getZSetDatatypeAnalysis(analysis *Analysis) {
	analysis.init(v.Address)
	analysis.Config[zsetMaxListpackValue] = v.Config[zsetMaxListpackValue]
	analysis.Config[zsetMaxListpackEntries] = v.Config[zsetMaxListpackEntries]
	metrics := v.ZSetMetrics
	analysis.Metrics[zsetDt] = map[string]any{
		kObjCnt:         metrics.objCnt,
		kSlKeyCnt:       metrics.skipListCnt,
		kElementsCnt:    metrics.elementCnt,
		kMaxElement:     metrics.maxElement,
		kMaxElementSize: metrics.maxElementSize,
		kAvgElementSize: metrics.avgElementSize,
		kDistribution:   quantileDistribution(metrics.tdigest),
	}
}

func (zm *ZSetMetrics) updateZSetStatistics(node *ZSetMetrics) {
	runningTotalField := (zm.elementCnt + node.elementCnt)
	runningTotalFieldSize := (float64(zm.elementCnt*int(zm.avgElementSize)) + float64(node.elementCnt*int(node.avgElementSize)))
	zm.avgElementSize = float64(runningTotalFieldSize / float64(runningTotalField))
	zm.elementCnt = runningTotalField
	if node.maxElementSize > zm.maxElementSize {
		zm.maxElementSize = node.maxElementSize
		zm.maxElement = node.maxElement
	}
	zm.skipListCnt += node.skipListCnt
	zm.objCnt += node.objCnt
	zm.tdigest.Merge(node.tdigest)
}
