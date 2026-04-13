package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/valkey-io/valkey-go"
)

const (
	listpackMaxConfig = "hash-max-listpack-value"
	errNotClusterMode = "This instance has cluster support disabled"
)

type ValkeyNodeMetrics struct {
	hashObjCount      uint64
	hashFieldCount    int
	hashTableObjCount uint64
	maxField          string
	avgFieldSize      float64
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
		fCount := len(entry.Elements) / 2
		fTotalSize := 0
		for i := 0; i < len(entry.Elements); i += 2 {
			fSize := len(entry.Elements[i+1])
			fTotalSize += fSize
			if fSize >= v.maxListPackSize {
				v.metrics.hashTableObjCount++
			}
			if fSize > v.metrics.maxFieldSize {
				v.metrics.maxFieldSize = fSize
				v.metrics.maxField = fmt.Sprintf("%s.%s", hash, entry.Elements[i])
			}
		}
		v.metrics.avgFieldSize = float64((fTotalSize + int(float64(v.metrics.hashFieldCount)*v.metrics.avgFieldSize)) / (v.metrics.hashFieldCount + fCount))
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
	fmt.Printf("Analysis for node %s (%s=%d):\n", v.Address, listpackMaxConfig, v.maxListPackSize)
	fmt.Printf("- hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", v.metrics.hashTableObjCount, v.metrics.hashObjCount, (float64(v.metrics.hashTableObjCount) / float64(v.metrics.hashObjCount) * 100))
	fmt.Printf("- hash fields count: %d\n", v.metrics.hashFieldCount)
	fmt.Printf("- largest hash field: %s, size:%d \n", v.metrics.maxField, v.metrics.maxFieldSize)
	fmt.Printf("- avg field size: %.2f\n", v.metrics.avgFieldSize)

	return nil

}

func main() {
	var bootstrapAddress = flag.String("address", "127.0.0.1:6379", "Valkey node address to connect to, will automatically detect other nodes if it is part of a cluster")
	var bootstrapPassword = flag.String("password", "", "Password of the Valkey user")
	var bootstrapUsername = flag.String("username", "", "name of the Valkey user")
	flag.Parse()

	v := ValkeyNode{
		Address:  *bootstrapAddress,
		Username: *bootstrapUsername,
		Password: *bootstrapPassword,
	}
	var nodes []ValkeyNode

	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{v.Address}})
	if err != nil {
		panic(err)
	}
	clusterNodes, err := client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
	if err != nil {
		if err.Error() != errNotClusterMode {
			panic(err)
		}
		nodes = append(nodes, v)
	} else {
		for et := range strings.SplitSeq(clusterNodes, "\n") {
			nodeDetails := strings.Split(et, " ")
			if len(nodeDetails) < 8 {
				continue
			}
			if strings.Contains(nodeDetails[2], "master") {
				continue
			}
			node := ValkeyNode{
				Username: v.Username,
				Password: v.Password,
				Address:  strings.Split(nodeDetails[1], "@")[0],
			}
			nodes = append(nodes, node)
		}
	}

	clusterSummary := ValkeyNode{}
	for _, v := range nodes {
		v.getNodeConfig()
		v.analyze()
		runningTotalField := (clusterSummary.metrics.hashFieldCount + v.metrics.hashFieldCount)
		runningTotalFieldSize := (float64(clusterSummary.metrics.hashFieldCount*int(clusterSummary.metrics.avgFieldSize)) + float64(v.metrics.hashFieldCount*int(v.metrics.avgFieldSize)))
		clusterSummary.metrics.avgFieldSize = float64(runningTotalFieldSize / float64(runningTotalField))
		clusterSummary.metrics.hashFieldCount = runningTotalField
		if v.metrics.maxFieldSize > clusterSummary.metrics.maxFieldSize {
			clusterSummary.metrics.maxFieldSize = v.metrics.maxFieldSize
			clusterSummary.metrics.maxField = v.metrics.maxField
		}
		clusterSummary.metrics.hashTableObjCount += v.metrics.hashTableObjCount
	}
	fmt.Printf("Analysis for cluster:\n")
	fmt.Printf("- hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", clusterSummary.metrics.hashTableObjCount, clusterSummary.metrics.hashObjCount, (float64(clusterSummary.metrics.hashTableObjCount) / float64(clusterSummary.metrics.hashObjCount) * 100))
	fmt.Printf("- hash fields count: %d\n", clusterSummary.metrics.hashFieldCount)
	fmt.Printf("- largest hash field: %s, size:%d \n", clusterSummary.metrics.maxField, clusterSummary.metrics.maxFieldSize)
	fmt.Printf("- avg field size: %.2f\n", clusterSummary.metrics.avgFieldSize)

}
