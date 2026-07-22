package main

import (
	"context"
	"fmt"
	"strconv"
)

const (
	zsetMaxListpackValue   = "zset-max-listpack-value"
	zsetMaxListpackEntries = "zset-max-listpack-entries"
	zsetDt                 = "zset"
)

type ZSetMetrics struct {
	elementStats sizeStats
	objCnt       int
	skipListCnt  uint64
}

func makeZSetMetrics() ZSetMetrics {
	return ZSetMetrics{elementStats: makeSizeStats()}
}

func (v *ValkeyNode) analyzeZSet() error {
	var cursor uint64
	for ok := true; ok; ok = (cursor != 0) {
		entry, err := scan(v.getClient(), zsetDt, v.opts().ZSetKeyPattern, cursor)
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
		for i := 0; i < len(entry.Elements); i++ {
			fSize := len(entry.Elements[i])
			metrics.elementStats.add(fmt.Sprintf("%s.%s", zset, entry.Elements[i]), fSize)
			isHashtable = fSize >= maxLpSize
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
		kElementsCnt:    metrics.elementStats.count,
		kMaxElement:     metrics.elementStats.maxItem,
		kMaxElementSize: metrics.elementStats.maxSize,
		kAvgElementSize: metrics.elementStats.avgSize,
		kDistribution:   quantileDistribution(metrics.elementStats.tdigest),
	}
}

func (zm *ZSetMetrics) updateZSetStatistics(node *ZSetMetrics) {
	zm.elementStats.merge(&node.elementStats)
	zm.skipListCnt += node.skipListCnt
	zm.objCnt += node.objCnt
}
