package main

import (
	"context"
	"fmt"
	"strconv"
)

const (
	hashMaxListpack = "hash-max-listpack-value"
	hashMaxEntries  = "hash-max-listpack-entries"
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
	entryCount := 0
	maxLpSize, err := strconv.Atoi(v.Config[hashMaxListpack])
	maxLpEntries, err := strconv.Atoi(v.Config[hashMaxEntries])
	fstats := &v.HashMetrics.fieldStats
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
		entryCount += len(entry.Elements) / 2
		for i := 0; i < len(entry.Elements); i += 2 {
			if v.opts().FieldPatternRE != nil && !v.opts().FieldPatternRE.MatchString(entry.Elements[i]) {
				continue
			}
			fSize := len(entry.Elements[i])
			vSize := len(entry.Elements[i+1])
			fstats.add(fmt.Sprintf("%s.%s (field name)", hash, entry.Elements[i]), fSize)
			fstats.add(fmt.Sprintf("%s.%s (field value)", hash, entry.Elements[i]), vSize)
			isHashtable = (fSize >= maxLpSize || vSize >= maxLpSize)
		}
		cursor = entry.Cursor
	}
	if entryCount > fstats.maxFieldCount {
		fstats.maxFieldCount = entryCount
		fstats.maxFieldCountItem = hash
	}
	if !isHashtable {
		isHashtable = entryCount > maxLpEntries
	}
	if isHashtable {
		v.HashMetrics.htCnt++
	}

	return nil
}

func (v *ValkeyNode) getHashDatatypeAnalysis(analysis *Analysis) {
	analysis.init(v.Address)
	analysis.Config[hashMaxListpack] = v.Config[hashMaxListpack]
	analysis.Config[hashMaxEntries] = v.Config[hashMaxEntries]
	analysis.Metrics[hashDt] = map[string]any{
		kObjCnt:         v.HashMetrics.objCnt,
		kHtKeyCnt:       v.HashMetrics.htCnt,
		kFieldCnt:       v.HashMetrics.fieldStats.count,
		kMaxElement:     v.HashMetrics.fieldStats.maxSizeItem,
		kMaxElementSize: v.HashMetrics.fieldStats.maxSize,
		kMaxEntriesCnt:  v.HashMetrics.fieldStats.maxFieldCount,
		kMaxEntries:     v.HashMetrics.fieldStats.maxFieldCountItem,
		kAvgElementSize: v.HashMetrics.fieldStats.avgSize,
		kDistribution:   quantileDistribution(v.HashMetrics.fieldStats.tdigest),
	}
}

func (hm *HashMetrics) updateHashStatistics(node *HashMetrics) {
	hm.fieldStats.merge(&node.fieldStats)
	hm.htCnt += node.htCnt
	hm.objCnt += node.objCnt
}
