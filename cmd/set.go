package main

import (
	"context"
	"fmt"
	"strconv"
)

const (
	setMaxListpackValue   = "set-max-listpack-value"
	setMaxListpackEntries = "set-max-listpack-entries"
	setDt                 = "set"
)

type SetMetrics struct {
	elementStats sizeStats
	objCnt       int
	// number keys encoded as hashtable
	htCnt uint64
}

func makeSetMetrics() SetMetrics {
	return SetMetrics{elementStats: makeSizeStats()}
}

func (v *ValkeyNode) analyzeSet() error {
	return v.analyze(setDt,
		func(count int) {
			v.SetMetrics.objCnt += count
		},
		v.analyzeSetMembers,
	)
}

func (v *ValkeyNode) analyzeSetMembers(set string) error {
	ctx := context.Background()
	client := v.getClient()
	var cursor uint64
	var isHashtable bool
	maxLpSize, err := strconv.Atoi(v.Config[setMaxListpackValue])
	if err != nil {
		return err
	}
	for ok := true; ok; ok = (cursor != 0) {
		resp := client.Do(
			ctx,
			client.B().Sscan().Key(set).Cursor(cursor).Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		for i := 0; i < len(entry.Elements); i++ {
			fSize := len(entry.Elements[i])
			v.SetMetrics.elementStats.add(fmt.Sprintf("%s.%s", set, entry.Elements[i]), fSize)
			isHashtable = fSize >= maxLpSize
		}
		cursor = entry.Cursor
	}
	if isHashtable {
		v.SetMetrics.htCnt++
	}
	return nil
}

func (v *ValkeyNode) getSetDatatypeAnalysis(analysis *Analysis) {
	analysis.init(v.Address)
	analysis.Config[setMaxListpackValue] = v.Config[setMaxListpackValue]
	analysis.Config[setMaxListpackEntries] = v.Config[setMaxListpackEntries]
	metrics := v.SetMetrics
	analysis.Metrics[setDt] = map[string]any{
		kObjCnt:         metrics.objCnt,
		kHtKeyCnt:       metrics.htCnt,
		kElementsCnt:    metrics.elementStats.count,
		kMaxElement:     metrics.elementStats.maxItem,
		kMaxElementSize: metrics.elementStats.maxSize,
		kAvgElementSize: metrics.elementStats.avgSize,
		kDistribution:   quantileDistribution(metrics.elementStats.tdigest),
	}
}

func (sm *SetMetrics) updateSetStatistics(node *SetMetrics) {
	sm.elementStats.merge(&node.elementStats)
	sm.htCnt += node.htCnt
	sm.objCnt += node.objCnt
}
