package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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

func (v *ValkeyNode) getHashDatatypeAnalysis() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "## Node %s\n", v.Address)
	fmt.Fprintln(&sb, "### Config")
	fmt.Fprintf(&sb, "- %s=%s\n", hashMaxListpack, v.Config[hashMaxListpack])
	fmt.Fprintln(&sb, "### Analysis")
	if v.HashMetrics.objCount == 0 {
		fmt.Fprintln(&sb, "N/A (no keys found)")
		return sb.String()
	}
	fmt.Fprintf(&sb, "- hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", v.HashMetrics.hashTableCount, v.HashMetrics.objCount, (float64(v.HashMetrics.hashTableCount) / float64(v.HashMetrics.objCount) * 100))
	fmt.Fprintf(&sb, "- hash fields count: %d\n", v.HashMetrics.fieldCount)
	fmt.Fprintf(&sb, "- largest hash field: %s, size:%d \n", v.HashMetrics.maxField, v.HashMetrics.maxFieldSize)
	fmt.Fprintf(&sb, "- avg field size: %.2f\n", v.HashMetrics.avgFieldSize)
	fmt.Fprintf(&sb, `- hash fields' size distribution:
+ Quartile 1 (P25): %.2f
+ Quartile 2 (P50): %.2f
+ Quartile 3 (P75): %.2f
+ Quartile 4 (P99): %.2f
`, v.HashMetrics.tdigest.Quantile(.25),
		v.HashMetrics.tdigest.Quantile(0.5),
		v.HashMetrics.tdigest.Quantile(0.75),
		v.HashMetrics.tdigest.Quantile(0.99))
	return sb.String()
}

func (hm *HashMetrics) updateHashStatistics(node *HashMetrics) {
	runningTotalField := (hm.fieldCount + node.fieldCount)
	runningTotalFieldSize := (float64(hm.fieldCount*int(hm.avgFieldSize)) + float64(node.fieldCount*int(node.avgFieldSize)))
	hm.avgFieldSize = float64(runningTotalFieldSize / float64(runningTotalField))
	hm.fieldCount = runningTotalField
	if node.maxFieldSize > hm.maxFieldSize {
		hm.maxFieldSize = node.maxFieldSize
		hm.maxField = node.maxField
	}
	hm.hashTableCount += node.hashTableCount
	hm.objCount += node.objCount
	hm.tdigest.Merge(node.tdigest)
}
