package main

import (
	"context"
	"fmt"
	"strconv"
)

const (
	hashMaxListpack = "hash-max-listpack-value"
	hashDt          = "hash"
)

type HashMetrics struct {
	fieldStats sizeStats
	objCnt     int
	// number of keys encoded as hashtables
	htCnt uint64
}

func makeHashMetrics() HashMetrics {
	return HashMetrics{fieldStats: makeSizeStats()}
}

func (v *ValkeyNode) analyzeHash(db int64) error {
	return v.analyze(db, hashDt,
		func(count int) {
			v.HashMetrics.objCnt += count
		},
		v.analyzeHashField,
	)
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
	for ok := true; ok; ok = (cursor != 0) {
		resp := client.Do(
			ctx,
			client.B().Hscan().Key(hash).Cursor(cursor).Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		for i := 0; i < len(entry.Elements); i += 2 {
			if v.opts().FieldPatternRE != nil && !v.opts().FieldPatternRE.MatchString(entry.Elements[i]) {
				continue
			}
			fSize := len(entry.Elements[i])
			vSize := len(entry.Elements[i+1])
			v.HashMetrics.fieldStats.add(fmt.Sprintf("%s.%s (field name)", hash, entry.Elements[i]), fSize)
			v.HashMetrics.fieldStats.add(fmt.Sprintf("%s.%s (field value)", hash, entry.Elements[i]), vSize)
			isHashtable = (fSize >= maxLpSize || vSize >= maxLpSize)
		}
		cursor = entry.Cursor
	}
	if isHashtable {
		v.HashMetrics.htCnt++
	}

	return nil
}

func (v *ValkeyNode) getHashDatatypeAnalysis(analysis *Analysis) {
	analysis.init(v.Address)
	analysis.Config[hashMaxListpack] = v.Config[hashMaxListpack]

	analysis.Metrics[hashDt] = map[string]any{
		kObjCnt:       v.HashMetrics.objCnt,
		kHtKeyCnt:     v.HashMetrics.htCnt,
		kFieldCnt:     v.HashMetrics.fieldStats.count,
		kMaxField:     v.HashMetrics.fieldStats.maxItem,
		kMaxFieldSize: v.HashMetrics.fieldStats.maxSize,
		kAvgFieldSize: v.HashMetrics.fieldStats.avgSize,
		kDistribution: quantileDistribution(v.HashMetrics.fieldStats.tdigest),
	}
}

func (hm *HashMetrics) updateHashStatistics(node *HashMetrics) {
	hm.fieldStats.merge(&node.fieldStats)
	hm.htCnt += node.htCnt
	hm.objCnt += node.objCnt
}
