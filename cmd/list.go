package main

import (
	"context"
	"fmt"
)

const (
	listMaxListpackSize = "list-max-listpack-size"
	listCompressDepth   = "list-compress-depth"
)

var optimizationLevel = map[string]int{
	"-1": 4 * 1024,
	"-2": 8 * 1024,
	"-3": 16 * 1024,
	"-4": 32 * 1024,
	"-5": 64 * 1024,
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
			analysis, err := v.analyzeListKey(key)
			// TODO: do something with the output
			fmt.Println(analysis)
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

func (v *ValkeyNode) analyzeListKey(key string) (map[string]int64, error) {
	ctx := context.Background()
	client := v.getClient()
	// count the number of elements
	count, err := client.Do(
		ctx,
		client.B().Llen().Key(key).Build(),
	).AsInt64()
	if err != nil {
		return nil, err
	}
	// get key size in bytes
	ksize, err := client.Do(
		ctx,
		client.B().MemoryUsage().Key(key).Build(),
	).AsInt64()
	if err != nil {
		return nil, err
	}
	// since nodes can be compressed, and there are no read-only option for list. avg size is only estimated
	avgElementSize := ksize / count

	maxNodeSize := optimizationLevel[v.Config[listMaxListpackSize]]
	nodeCount := ksize / int64(maxNodeSize)
	// whatever the value of `list-compress-depth` is, there will always be at least 2 nodes stored as uncompressed

	// if there are >= 3 nodes and low op/s, then it can be compressed, level 1
	// if there are more nodes, high op/s then can set compression to higher. Leave more nodes uncompressed

	return map[string]int64{
		"element-count":    count,
		"size":             ksize,
		"avg-element-size": avgElementSize,
		"node-count":       nodeCount,
	}, nil
}
