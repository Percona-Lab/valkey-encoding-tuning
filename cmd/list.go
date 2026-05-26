package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/caio/go-tdigest/v5"
)

const (
	listMaxListpackSize = "list-max-listpack-size"
	listCompressDepth   = "list-compress-depth"
)

// since >1 node converts List to quicklist (still function as listpack as head & tail is uncompressed), we don't need total nodeCount
type ListMetrics struct {
	// for obj size or element count distribution, depending on the setting of `list-max-listpack-size` (positive or negative number)
	// for obj size distribution (default), affecting the number of nodes
	// for element count distribution, affecting the number of nodes if `list-max-listpack-size` is positive number
	tdigest         *tdigest.TDigest
	objCount        int64
	avgNodeCount    int64
	maxNodeCount    int64
	avgObjSize      int64
	maxObjSize      int64
	avgElementCount int64
	maxElementCount int64
}

var optimizationLevel = map[string]int{
	"-1": 4 * 1024,
	"-2": 8 * 1024,
	"-3": 16 * 1024,
	"-4": 32 * 1024,
	"-5": 64 * 1024,
}

func makeListMetrics() ListMetrics {
	t, err := tdigest.New()
	if err != nil {
		panic(err)
	}
	return ListMetrics{tdigest: t}
}
func (v *ValkeyNode) analyzeList() error {
	ctx := context.Background()
	client := v.getClient()
	err := client.Do(ctx,
		client.B().Readonly().Build(),
	).Error()
	if err != nil {
		panic(err)
	}
	var cursor uint64
	for {
		scanCmd := client.B().Scan().Cursor(cursor)
		if *listKeyPattern != "" {
			scanCmd.Match(*listKeyPattern)
		}
		scanCmd.Type("list")
		resp := client.Do(
			ctx,
			scanCmd.Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}

		for _, key := range entry.Elements {
			err := v.analyzeListKey(key)
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

func (v *ValkeyNode) analyzeListKey(key string) error {
	v.ListMetrics.objCount++
	ctx := context.Background()
	client := v.getClient()
	// count the number of elements
	// whatever the value of `list-compress-depth` is, there will always be at least 2 nodes stored as uncompressed
	// if there are >= 3 nodes and low op/s, then it can be compressed, level 1
	// if there are more nodes, high op/s then can set compression to higher. Leave more nodes uncompressed
	// so need the element count to check if compression is suitable
	count, err := client.Do(
		ctx,
		client.B().Llen().Key(key).Build(),
	).AsInt64()
	if err != nil {
		return err
	}
	// get key size in bytes
	ksize, err := client.Do(
		ctx,
		client.B().MemoryUsage().Key(key).Build(),
	).AsInt64()

	if err != nil {
		return err
	}

	var nodeCount int64
	// List datatype create new node depending on the number of element per node, or node size
	isMaxSizeByElementSize := strings.HasPrefix(v.Config[listMaxListpackSize], "-")
	if isMaxSizeByElementSize {
		maxNodeSize := optimizationLevel[v.Config[listMaxListpackSize]]
		nodeCount = ksize / int64(maxNodeSize)
		v.ListMetrics.tdigest.Add(float64(ksize))
	} else {
		maxNodeSize, err := strconv.Atoi(v.Config[listMaxListpackSize])
		if err != nil {
			return err
		}
		nodeCount = count / int64(maxNodeSize)
		v.ListMetrics.tdigest.Add(float64(count))
	}

	if v.ListMetrics.maxNodeCount < nodeCount {
		v.ListMetrics.maxNodeCount = nodeCount
	}
	v.ListMetrics.avgNodeCount = (nodeCount + int64(v.ListMetrics.avgNodeCount)) / v.ListMetrics.objCount

	if v.ListMetrics.maxObjSize < ksize {
		v.ListMetrics.maxObjSize = ksize
	}
	v.ListMetrics.avgObjSize = (ksize + v.ListMetrics.avgObjSize) / v.ListMetrics.objCount

	if v.ListMetrics.maxElementCount < count {
		v.ListMetrics.maxElementCount = count
	}
	v.ListMetrics.avgElementCount = (count + v.ListMetrics.avgElementCount) / v.ListMetrics.objCount

	return nil
}

func (v *ValkeyNode) printListDatatypeAnalysis() {
	if !*printOutput {
		return
	}
	var sizeDistrType string
	if strings.HasPrefix(v.Config[listMaxListpackSize], "-") {
		sizeDistrType = "element size"
	} else {
		sizeDistrType = "element count"
	}
	fmt.Println("-------------------")
	fmt.Printf("Analysis for node %s (%s=%s):\n", v.Address, listMaxListpackSize, v.Config[listMaxListpackSize])
	fmt.Printf("- list keys found: %d \n", v.ListMetrics.objCount)
	fmt.Printf("- largest node count:%d \n", v.ListMetrics.maxNodeCount)
	fmt.Printf("- avg node count: %d\n", v.ListMetrics.avgNodeCount)
	fmt.Printf("- largest element count:%d \n", v.ListMetrics.maxElementCount)
	fmt.Printf("- avg element count: %d\n", v.ListMetrics.avgElementCount)
	fmt.Printf("- List size distribution (by %s):\n", sizeDistrType)
	fmt.Printf(`
+ Quartile 1 (P25): %.2f
+ Quartile 2 (P50): %.2f
+ Quartile 3 (P75): %.2f
+ Quartile 4 (P99): %.2f
`, v.HashMetrics.tdigest.Quantile(.25),
		v.HashMetrics.tdigest.Quantile(0.5),
		v.HashMetrics.tdigest.Quantile(0.75),
		v.HashMetrics.tdigest.Quantile(0.99))
}
