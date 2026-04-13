package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/valkey-io/valkey-go"
)

const (
	listpackMaxConfig = "hash-max-listpack-value"
)

type ValkeyNodeMetrics struct {
	hashObjCount      uint64
	hashFieldCount    uint64
	hashTableObjCount uint64
	maxField          string
	maxFieldSize      int
}
type ValkeyNode struct {
	Username        string
	Password        string
	Address         string
	Config          map[string]string
	metrics         ValkeyNodeMetrics
	maxListPackSize int
}

func (v *ValkeyNode) getNodeConfig() error {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{v.Address}})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	config, err := client.Do(ctx, client.B().ConfigGet().Parameter(listpackMaxConfig).Build()).AsStrMap()
	if err != nil {
		return err
	}
	v.maxListPackSize, err = strconv.Atoi(config[listpackMaxConfig])
	if err != nil {
		return err
	}
	return nil
}

func (v *ValkeyNode) analyzeHashField(client valkey.Client, hash string) error {
	ctx := context.Background()
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
		for i := 0; i < len(entry.Elements); i += 2 {
			fLen := len(entry.Elements[i+1])
			if fLen >= v.maxListPackSize {
				v.metrics.hashTableObjCount++
			}
			if fLen > v.metrics.maxFieldSize {
				v.metrics.maxFieldSize = fLen
				v.metrics.maxField = fmt.Sprintf("%s.%s", hash, entry.Elements[i])
			}
		}
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (v *ValkeyNode) analyze() error {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{v.Address}})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	var cursor uint64
	for {
		resp := client.Do(
			ctx,
			client.B().Scan().Cursor(cursor).Type("hash").Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return err
		}
		for _, key := range entry.Elements {
			v.metrics.hashObjCount++
			v.analyzeHashField(client, key)
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	fmt.Printf("hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", v.metrics.hashTableObjCount, v.metrics.hashObjCount, (float64(v.metrics.hashTableObjCount) / float64(v.metrics.hashObjCount) * 100))
	fmt.Printf("hash fields count: %d\n", v.metrics.hashFieldCount)
	fmt.Printf("largest hash field: %s, size:%d \n", v.metrics.maxField, v.metrics.maxFieldSize)

	return nil

}

func main() {
	v := ValkeyNode{
		Address: "127.0.0.1:6379",
	}
	v.getNodeConfig()
	v.analyze()
}
