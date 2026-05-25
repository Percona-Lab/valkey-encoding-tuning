package main

import (
	"context"
	"fmt"
)

func (v *ValkeyNode) analyzeHash() error {
	if err := v.ensureMetrics(); err != nil {
		return err
	}
	ctx := context.Background()

	client := v.getClient()
	err := client.Do(ctx, client.B().Readonly().Build()).Error()
	if err != nil {
		panic(err)
	}
	var cursor uint64
	for {
		scanCmd := client.B().Scan().Cursor(cursor)
		if *keyPattern != "" {
			scanCmd.Match(*keyPattern)
		}
		scanCmd.Type("hash")
		resp := client.Do(
			ctx,
			scanCmd.Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		v.metrics.hashObjCount += len(entry.Elements)
		for _, key := range entry.Elements {
			err = v.analyzeHashField(key)
			if err != nil {
				panic(err)
			}
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	v.printNodeAnalysis()
	return nil

}

func (v *ValkeyNode) analyzeHashField(hash string) error {
	if err := v.ensureMetrics(); err != nil {
		return err
	}
	ctx := context.Background()
	client := v.getClient()
	var cursor uint64
	for {
		resp := client.Do(
			ctx,
			client.B().Hscan().Key(hash).Cursor(cursor).Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		fCount := 0
		fTotalSize := 0
		for i := 0; i < len(entry.Elements); i += 2 {
			if fieldPatternRE != nil && !fieldPatternRE.MatchString(entry.Elements[i]) {
				continue
			}
			fCount++
			fSize := len(entry.Elements[i+1])
			v.metrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			if fSize >= v.maxListPackSize {
				v.metrics.hashTableObjCount++
			}
			if fSize > v.metrics.maxFieldSize {
				v.metrics.maxFieldSize = fSize
				v.metrics.maxField = fmt.Sprintf("%s.%s", hash, entry.Elements[i])
			}
		}
		if fCount > 0 {
			v.metrics.avgFieldSize = float64((fTotalSize + int(float64(v.metrics.hashFieldCount)*v.metrics.avgFieldSize)) / (v.metrics.hashFieldCount + fCount))
			v.metrics.hashFieldCount += fCount
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	return nil
}
