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
	tdigest       *tdigest.TDigest
	objCount      int
	memberCount   int
	skipListCount uint64
	maxField      string
	avgFieldSize  float64
	maxFieldSize  int
}

func makeZSetMetrics() ZSetMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return ZSetMetrics{tdigest: t}
}

func (v *ValkeyNode) analyzeZSet() error {
	ctx := context.Background()

	client := v.getClient()
	err := client.Do(ctx, client.B().Readonly().Build()).Error()
	if err != nil {
		panic(err)
	}
	var cursor uint64
	for {
		scanCmd := client.B().Scan().Cursor(cursor)
		if *zsetKeyPattern != "" {
			scanCmd.Match(*zsetKeyPattern)
		}
		scanCmd.Type(setDt)
		resp := client.Do(
			ctx,
			scanCmd.Build(),
		)
		entry, err := resp.AsScanEntry()
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
	metrics := v.ZSetMetrics
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
			if fSize > metrics.maxFieldSize {
				metrics.maxFieldSize = fSize
				metrics.maxField = fmt.Sprintf("%s.%s", zset, entry.Elements[i])
			}
		}
		if mCount > 0 {
			metrics.avgFieldSize = float64((fTotalSize + int(float64(metrics.memberCount)*metrics.avgFieldSize)) / (metrics.memberCount + mCount))
			metrics.memberCount += mCount
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
	analysis.Config[setMaxListpackValue] = v.Config[setMaxListpackValue]
	analysis.Config[setMaxListpackEntries] = v.Config[setMaxListpackEntries]
	metrics := v.ZSetMetrics
	analysis.Metrics[setDt] = map[string]any{
		"object_count":       metrics.objCount,
		"skiplist_key_count": metrics.skipListCount,
		"items_count":        metrics.memberCount,
		"largest_field":      metrics.maxField,
		"largest_field_size": metrics.maxFieldSize,
		"avg_field_size":     metrics.avgFieldSize,
		"distribution":       quantileDistribution(metrics.tdigest),
	}
}

func (zm *ZSetMetrics) updateZSetStatistics(node *ZSetMetrics) {
	runningTotalField := (zm.memberCount + node.memberCount)
	runningTotalFieldSize := (float64(zm.memberCount*int(zm.avgFieldSize)) + float64(node.memberCount*int(node.avgFieldSize)))
	zm.avgFieldSize = float64(runningTotalFieldSize / float64(runningTotalField))
	zm.memberCount = runningTotalField
	if node.maxFieldSize > zm.maxFieldSize {
		zm.maxFieldSize = node.maxFieldSize
		zm.maxField = node.maxField
	}
	zm.skipListCount += node.skipListCount
	zm.objCount += node.objCount
	zm.tdigest.Merge(node.tdigest)
}
