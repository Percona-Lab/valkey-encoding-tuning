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
	elementCount   int
	hashTableCount uint64
	maxElement     string
	avgElementSize float64
	maxElementSize int
}

func makeSetMetrics() SetMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return SetMetrics{tdigest: t}
}

func (v *ValkeyNode) analyzeSet() error {
	var cursor uint64
	for {
		entry, err := scan(v.getClient(), setDt, *setKeyPattern, cursor)
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
			if fSize > v.SetMetrics.maxElementSize {
				v.SetMetrics.maxElementSize = fSize
				v.SetMetrics.maxElement = fmt.Sprintf("%s.%s", set, entry.Elements[i])
			}
		}
		if mCount > 0 {
			v.SetMetrics.avgElementSize = float64((fTotalSize + int(float64(v.SetMetrics.elementCount)*v.SetMetrics.avgElementSize)) / (v.SetMetrics.elementCount + mCount))
			v.SetMetrics.elementCount += mCount
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
	metrics := v.SetMetrics
	analysis.Metrics[setDt] = map[string]any{
		kObjCount:       metrics.objCount,
		kHtKeyCount:     metrics.hashTableCount,
		kElementsCount:  metrics.elementCount,
		kMaxElement:     metrics.maxElement,
		kMaxElementSize: metrics.maxElementSize,
		kAvgElementSize: metrics.avgElementSize,
		kDistribution:   quantileDistribution(metrics.tdigest),
	}
}

func (sm *SetMetrics) updateSetStatistics(node *SetMetrics) {
	runningTotalField := (sm.elementCount + node.elementCount)
	runningTotalFieldSize := (float64(sm.elementCount*int(sm.avgElementSize)) + float64(node.elementCount*int(node.avgElementSize)))
	sm.avgElementSize = float64(runningTotalFieldSize / float64(runningTotalField))
	sm.elementCount = runningTotalField
	if node.maxElementSize > sm.maxElementSize {
		sm.maxElementSize = node.maxElementSize
		sm.maxElement = node.maxElement
	}
	sm.hashTableCount += node.hashTableCount
	sm.objCount += node.objCount
	sm.tdigest.Merge(node.tdigest)
}
