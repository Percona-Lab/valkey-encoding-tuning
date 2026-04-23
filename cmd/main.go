package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/caio/go-tdigest"
	"github.com/valkey-io/valkey-go"
)

const (
	listpackMaxConfig = "hash-max-listpack-value"
	errNotClusterMode = "This instance has cluster support disabled"
)

type ValkeyNodeMetrics struct {
	tdigest           *tdigest.TDigest
	hashObjCount      int
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
			v.metrics.tdigest.Add(float64(fSize))
			fTotalSize += fSize
			if fSize >= v.maxListPackSize {
				v.metrics.hashTableObjCount++
				// fmt.Printf("- %s exceeded hash-max-listpack-value by %d\n", fmt.Sprintf("%s.%s", hash, entry.Elements[i]), fSize-v.maxListPackSize)
			}
			if fSize > v.metrics.maxFieldSize {
				v.metrics.maxFieldSize = fSize
				v.metrics.maxField = fmt.Sprintf("%s.%s", hash, entry.Elements[i])
			}
		}
		v.metrics.avgFieldSize = float64((fTotalSize + int(float64(v.metrics.hashFieldCount)*v.metrics.avgFieldSize)) / (v.metrics.hashFieldCount + fCount))
		v.metrics.hashFieldCount += fCount
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	return nil
}

func (v *ValkeyNode) analyze() error {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{v.Address},
		ForceSingleClient: true,
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	err = client.Do(ctx, client.B().Readonly().Build()).Error()
	if err != nil {
		panic(err)
	}
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
		// fmt.Printf("in_cursor=%d out_cursor=%d keys=%d\n",
		// cursor, entry.Cursor, len(entry.Elements))
		v.metrics.hashObjCount += len(entry.Elements)
		for _, key := range entry.Elements {
			err = v.analyzeHashField(client, key)
			if err != nil {
				panic(err)
			}
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	fmt.Println("-------------------")
	fmt.Printf("Analysis for node %s (%s=%d):\n", v.Address, listpackMaxConfig, v.maxListPackSize)
	fmt.Printf("- hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", v.metrics.hashTableObjCount, v.metrics.hashObjCount, (float64(v.metrics.hashTableObjCount) / float64(v.metrics.hashObjCount) * 100))
	fmt.Printf("- hash fields count: %d\n", v.metrics.hashFieldCount)
	fmt.Printf("- largest hash field: %s, size:%d \n", v.metrics.maxField, v.metrics.maxFieldSize)
	fmt.Printf("- avg field size: %.2f\n", v.metrics.avgFieldSize)
	fmt.Printf(`- hash fields' size distribution:
+ Quartile 1 (P25): %.2f
+ Quartile 2 (P50): %.2f
+ Quartile 3 (P75): %.2f
+ Quartile 4 (P99): %.2f
`, v.metrics.tdigest.Quantile(.25),
		v.metrics.tdigest.Quantile(0.5),
		v.metrics.tdigest.Quantile(0.75),
		v.metrics.tdigest.Quantile(0.99))
	return nil

}

func getClusterNodes(bootstrapNode ValkeyNode) []ValkeyNode {
	var nodes []ValkeyNode

	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{bootstrapNode.Address},
		Username:    bootstrapNode.Username,
		Password:    bootstrapNode.Password,
	})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	clusterNodes, err := client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
	if err != nil {
		if err.Error() != errNotClusterMode {
			panic(err)
		}
		nodes = append(nodes, bootstrapNode)
	} else {
		for et := range strings.SplitSeq(clusterNodes, "\n") {
			nodeDetails := strings.Split(et, " ")
			if len(nodeDetails) < 8 {
				continue
			}
			flags := nodeDetails[2]
			if !strings.Contains(flags, "master") {
				continue
			}
			t, _ := tdigest.New()
			node := ValkeyNode{
				Username: bootstrapNode.Username,
				Password: bootstrapNode.Password,
				Address:  strings.Split(nodeDetails[1], "@")[0],
				metrics: ValkeyNodeMetrics{
					tdigest: t,
				},
			}
			nodes = append(nodes, node)
		}
	}
	return nodes
}
func analyzeCluster(bootstrapNode ValkeyNode) ValkeyNode {
	nodes := getClusterNodes(bootstrapNode)
	t, _ := tdigest.New()
	cs := ValkeyNode{
		metrics: ValkeyNodeMetrics{
			tdigest: t,
		},
	}
	for _, v := range nodes {
		v.getNodeConfig()
		v.analyze()
		runningTotalField := (cs.metrics.hashFieldCount + v.metrics.hashFieldCount)
		runningTotalFieldSize := (float64(cs.metrics.hashFieldCount*int(cs.metrics.avgFieldSize)) + float64(v.metrics.hashFieldCount*int(v.metrics.avgFieldSize)))
		cs.metrics.avgFieldSize = float64(runningTotalFieldSize / float64(runningTotalField))
		cs.metrics.hashFieldCount = runningTotalField
		if v.metrics.maxFieldSize > cs.metrics.maxFieldSize {
			cs.metrics.maxFieldSize = v.metrics.maxFieldSize
			cs.metrics.maxField = v.metrics.maxField
		}
		cs.metrics.hashTableObjCount += v.metrics.hashTableObjCount
		cs.metrics.hashObjCount += v.metrics.hashObjCount
		cs.metrics.tdigest.Merge(v.metrics.tdigest)
	}

	fmt.Println("-----------------")
	fmt.Printf("Analysis for cluster:\n")
	fmt.Printf("- hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", cs.metrics.hashTableObjCount, cs.metrics.hashObjCount, (float64(cs.metrics.hashTableObjCount) / float64(cs.metrics.hashObjCount) * 100))
	fmt.Printf("- hash fields count: %d\n", cs.metrics.hashFieldCount)
	fmt.Printf("- largest hash field: %s, size:%d \n", cs.metrics.maxField, cs.metrics.maxFieldSize)
	fmt.Printf("- avg field size: %.2f\n", cs.metrics.avgFieldSize)
	fmt.Printf(`- hash fields' size distribution:
+ Quartile 1 (P25): %.2f
+ Quartile 2 (P50): %.2f
+ Quartile 3 (P75): %.2f
+ Quartile 4 (P99): %.2f
`, cs.metrics.tdigest.Quantile(.25),
		cs.metrics.tdigest.Quantile(0.5),
		cs.metrics.tdigest.Quantile(0.75),
		cs.metrics.tdigest.Quantile(0.99))
	return cs
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
	analyzeCluster(v)
}
