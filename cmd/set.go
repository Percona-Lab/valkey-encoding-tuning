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
	tdigest    *tdigest.TDigest
	objCnt     int
	elementCnt int
	// number keys encoded as hashtable
	htCnt          uint64
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
	for ok := true; ok; ok = (cursor != 0) {
		entry, err := scan(v.getClient(), setDt, v.opts().SetKeyPattern, cursor)
		if err != nil {
			return err
		}
		v.SetMetrics.objCnt += len(entry.Elements)
		for _, key := range entry.Elements {
			err = v.analyzeSetMembers(key)
			if err != nil {
				panic(err)
			}
		}
		cursor = entry.Cursor
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
	for ok := true; ok; ok = (cursor != 0) {
		resp := client.Do(
			ctx,
			client.B().Sscan().Key(set).Cursor(cursor).Build(),
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
			v.SetMetrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			isHashtable = fSize >= maxLpSize
			if fSize > v.SetMetrics.maxElementSize {
				v.SetMetrics.maxElementSize = fSize
				v.SetMetrics.maxElement = fmt.Sprintf("%s.%s", set, entry.Elements[i])
			}
		}
		if eleCnt > 0 {
			v.SetMetrics.avgElementSize = float64((fTotalSize + int(float64(v.SetMetrics.elementCnt)*v.SetMetrics.avgElementSize)) / (v.SetMetrics.elementCnt + eleCnt))
			v.SetMetrics.elementCnt += eleCnt
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
		kElementsCnt:    metrics.elementCnt,
		kMaxElement:     metrics.maxElement,
		kMaxElementSize: metrics.maxElementSize,
		kAvgElementSize: metrics.avgElementSize,
		kDistribution:   quantileDistribution(metrics.tdigest),
	}
}

func (sm *SetMetrics) updateSetStatistics(node *SetMetrics) {
	runningTotalField := (sm.elementCnt + node.elementCnt)
	runningTotalFieldSize := (float64(sm.elementCnt*int(sm.avgElementSize)) + float64(node.elementCnt*int(node.avgElementSize)))
	sm.avgElementSize = float64(runningTotalFieldSize / float64(runningTotalField))
	sm.elementCnt = runningTotalField
	if node.maxElementSize > sm.maxElementSize {
		sm.maxElementSize = node.maxElementSize
		sm.maxElement = node.maxElement
	}
	sm.htCnt += node.htCnt
	sm.objCnt += node.objCnt
	sm.tdigest.Merge(node.tdigest)
}
