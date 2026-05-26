package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/caio/go-tdigest/v5"
)

const (
	hashMaxListpack = "hash-max-listpack-value"
)

type HashMetrics struct {
	tdigest        *tdigest.TDigest
	objCount       int
	fieldCount     int
	hashTableCount uint64
	maxField       string
	avgFieldSize   float64
	maxFieldSize   int
}

func makeHashMetrics() HashMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return HashMetrics{tdigest: t}
}

func (v *ValkeyNode) analyzeHash() error {
	ctx := context.Background()

	client := v.getClient()
	err := client.Do(ctx, client.B().Readonly().Build()).Error()
	if err != nil {
		panic(err)
	}
	var cursor uint64
	for {
		scanCmd := client.B().Scan().Cursor(cursor)
		if *hashKeyPattern != "" {
			scanCmd.Match(*hashKeyPattern)
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
		v.HashMetrics.objCount += len(entry.Elements)
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
	v.printHashDatatypeAnalysis()
	return nil

}

func (v *ValkeyNode) analyzeHashField(hash string) error {
	ctx := context.Background()
	client := v.getClient()
	var cursor uint64
	maxLpSize, err := strconv.Atoi(v.Config[hashMaxListpack])
	if err != nil {
		return err
	}
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
			v.HashMetrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			if fSize >= maxLpSize {
				v.HashMetrics.hashTableCount++
			}
			if fSize > v.HashMetrics.maxFieldSize {
				v.HashMetrics.maxFieldSize = fSize
				v.HashMetrics.maxField = fmt.Sprintf("%s.%s", hash, entry.Elements[i])
			}
		}
		if fCount > 0 {
			v.HashMetrics.avgFieldSize = float64((fTotalSize + int(float64(v.HashMetrics.fieldCount)*v.HashMetrics.avgFieldSize)) / (v.HashMetrics.fieldCount + fCount))
			v.HashMetrics.fieldCount += fCount
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (v *ValkeyNode) printHashDatatypeAnalysis() {
	if !*printOutput {
		return
	}
	fmt.Println("-------------------")
	fmt.Printf("Analysis for node %s (%s=%s):\n", v.Address, hashMaxListpack, v.Config[hashMaxListpack])
	fmt.Printf("- hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", v.HashMetrics.hashTableCount, v.HashMetrics.objCount, (float64(v.HashMetrics.hashTableCount) / float64(v.HashMetrics.objCount) * 100))
	fmt.Printf("- hash fields count: %d\n", v.HashMetrics.fieldCount)
	fmt.Printf("- largest hash field: %s, size:%d \n", v.HashMetrics.maxField, v.HashMetrics.maxFieldSize)
	fmt.Printf("- avg field size: %.2f\n", v.HashMetrics.avgFieldSize)
	fmt.Printf(`- hash fields' size distribution:
+ Quartile 1 (P25): %.2f
+ Quartile 2 (P50): %.2f
+ Quartile 3 (P75): %.2f
+ Quartile 4 (P99): %.2f
`, v.HashMetrics.tdigest.Quantile(.25),
		v.HashMetrics.tdigest.Quantile(0.5),
		v.HashMetrics.tdigest.Quantile(0.75),
		v.HashMetrics.tdigest.Quantile(0.99))
}

func (v *ValkeyNode) updateHashStatistics(node *ValkeyNode) {
	runningTotalField := (v.HashMetrics.fieldCount + node.HashMetrics.fieldCount)
	runningTotalFieldSize := (float64(v.HashMetrics.fieldCount*int(v.HashMetrics.avgFieldSize)) + float64(node.HashMetrics.fieldCount*int(node.HashMetrics.avgFieldSize)))
	v.HashMetrics.avgFieldSize = float64(runningTotalFieldSize / float64(runningTotalField))
	v.HashMetrics.fieldCount = runningTotalField
	if node.HashMetrics.maxFieldSize > v.HashMetrics.maxFieldSize {
		v.HashMetrics.maxFieldSize = node.HashMetrics.maxFieldSize
		v.HashMetrics.maxField = node.HashMetrics.maxField
	}
	v.HashMetrics.hashTableCount += node.HashMetrics.hashTableCount
	v.HashMetrics.objCount += node.HashMetrics.objCount
	v.HashMetrics.tdigest.Merge(node.HashMetrics.tdigest)
}
