package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/caio/go-tdigest/v5"
)

const (
	setMaxListpackValue   = "set-max-listpack-value"
	setMaxListpackEntries = "set-max-listpack-entries"
	setDt                 = "set"
)

type SetMetrics struct {
	tdigest        *tdigest.TDigest
	objCount       int
	memberCount    int
	hashTableCount uint64
	maxField       string
	avgFieldSize   float64
	maxFieldSize   int
}

func makeSetMetrics() SetMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return SetMetrics{tdigest: t}
}

func (v *ValkeyNode) analyzeSet() error {
	ctx := context.Background()

	client := v.getClient()
	err := client.Do(ctx, client.B().Readonly().Build()).Error()
	if err != nil {
		panic(err)
	}
	var cursor uint64
	for {
		scanCmd := client.B().Scan().Cursor(cursor)
		if *setKeyPattern != "" {
			scanCmd.Match(*setKeyPattern)
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
		v.SetMetrics.objCount += len(entry.Elements)
		for _, key := range entry.Elements {
			err = v.analyzeSetMembers(key)
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

func (v *ValkeyNode) analyzeSetMembers(set string) error {
	ctx := context.Background()
	client := v.getClient()
	var cursor uint64
	var isHashtable bool
	maxLpSize, err := strconv.Atoi(v.Config[setMaxListpackValue])
	if err != nil {
		return err
	}
	for {
		resp := client.Do(
			ctx,
			client.B().Sscan().Key(set).Cursor(cursor).Build(),
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
			v.SetMetrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			isHashtable = fSize >= maxLpSize
			if fSize > v.SetMetrics.maxFieldSize {
				v.SetMetrics.maxFieldSize = fSize
				v.SetMetrics.maxField = fmt.Sprintf("%s.%s", set, entry.Elements[i])
			}
		}
		if mCount > 0 {
			v.SetMetrics.avgFieldSize = float64((fTotalSize + int(float64(v.SetMetrics.memberCount)*v.SetMetrics.avgFieldSize)) / (v.SetMetrics.memberCount + mCount))
			v.SetMetrics.memberCount += mCount
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	if isHashtable {
		v.SetMetrics.hashTableCount++
	}
	return nil
}

func (v *ValkeyNode) getSetDatatypeAnalysis(analysis *Analysis) {
	analysis.init(v.Address)
	analysis.Config[setMaxListpackValue] = v.Config[setMaxListpackValue]
	analysis.Config[setMaxListpackEntries] = v.Config[setMaxListpackEntries]

	analysis.Metrics[setDt] = map[string]any{
		"object_count":        v.SetMetrics.objCount,
		"hashtable_key_count": v.SetMetrics.hashTableCount,
		"items_count":         v.SetMetrics.memberCount,
		"largest_field":       v.SetMetrics.maxField,
		"largest_field_size":  v.SetMetrics.maxFieldSize,
		"avg_field_size":      v.SetMetrics.avgFieldSize,
		"distribution":        quantileDistribution(v.SetMetrics.tdigest),
	}
}

func (sm *SetMetrics) updateSetStatistics(node *SetMetrics) {
	runningTotalField := (sm.memberCount + node.memberCount)
	runningTotalFieldSize := (float64(sm.memberCount*int(sm.avgFieldSize)) + float64(node.memberCount*int(node.avgFieldSize)))
	sm.avgFieldSize = float64(runningTotalFieldSize / float64(runningTotalField))
	sm.memberCount = runningTotalField
	if node.maxFieldSize > sm.maxFieldSize {
		sm.maxFieldSize = node.maxFieldSize
		sm.maxField = node.maxField
	}
	sm.hashTableCount += node.hashTableCount
	sm.objCount += node.objCount
	sm.tdigest.Merge(node.tdigest)
}
