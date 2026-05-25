package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strings"

	"github.com/caio/go-tdigest/v5"
	"github.com/valkey-io/valkey-go"
)

const (
	listpackMaxConfig = "hash-max-listpack-value"
	errNotClusterMode = "This instance has cluster support disabled"
)

var (
	bootstrapAddress  *string
	bootstrapUsername *string
	bootstrapPassword *string
	keyPattern        *string
	fieldPattern      *string
	fieldPatternRE    *regexp.Regexp
	printOutput       *bool
	flagsInitialized  *flag.FlagSet
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
	Address         string
	Client          valkey.Client
	Config          map[string]string
	metrics         ValkeyNodeMetrics
	maxListPackSize int
}

func (v *ValkeyNode) getClient() valkey.Client {
	if v.Client == nil {
		v.Client = createClient(v.Address)
	}
	return v.Client
}

func (v *ValkeyNode) Close() {
	if v.Client != nil {
		v.Client.Close()
		v.Client = nil
	}
}

func (v *ValkeyNode) ensureMetrics() error {
	if v.metrics.tdigest != nil {
		return nil
	}
	t, err := tdigest.New()
	if err != nil {
		return err
	}
	v.metrics.tdigest = t
	return nil
}

func (v *ValkeyNode) printNodeAnalysis() {
	if !*printOutput {
		return
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
}

func createClient(address string) valkey.Client {
	var clientOption valkey.ClientOption
	if strings.Contains(address, ":") {
		clientOption = valkey.ClientOption{
			InitAddress:       []string{address},
			ForceSingleClient: true,
		}
	} else {
		clientOption = valkey.MustParseURL("unix://" + address)
		clientOption.ForceSingleClient = true
	}
	if bootstrapUsername != nil && *bootstrapUsername != "" {
		clientOption.Username = *bootstrapUsername
	}
	if bootstrapPassword != nil && *bootstrapPassword != "" {
		clientOption.Password = *bootstrapPassword
	}
	client, err := valkey.NewClient(clientOption)
	if err != nil {
		panic(err)
	}
	return client
}

func getClusterNodes(bootstrapNode ValkeyNode) []ValkeyNode {
	var nodes []ValkeyNode

	ctx := context.Background()
	client := createClient(bootstrapNode.Address)
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
			t, err := tdigest.New()
			if err != nil {
				panic(err)
			}
			node := ValkeyNode{
				Address: strings.Split(nodeDetails[1], "@")[0],
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
		v.analyzeHash()
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
	cs.printNodeAnalysis()
	return cs
}

func initFlags() {
	if flagsInitialized == flag.CommandLine {
		return
	}
	bootstrapAddress = flag.String("address", "127.0.0.1:6379", "Valkey node address to connect to, will automatically detect other nodes if it is part of a cluster")
	bootstrapPassword = flag.String("password", "", "Password of the Valkey user")
	bootstrapUsername = flag.String("username", "", "Name of the Valkey user")
	keyPattern = flag.String("key-pattern", "", "Pattern (glob style) of the keys to be analyzed")
	fieldPattern = flag.String("field-pattern", "", "Pattern (regex style) of the hash fields to be analyzed")
	printOutput = flag.Bool("print-output", true, "Print output to stdout")
	flagsInitialized = flag.CommandLine
}
func parseArguments() {
	if *fieldPattern != "" {
		fieldPatternRE = regexp.MustCompile(*fieldPattern)
	} else {
		fieldPatternRE = nil
	}
}

func main() {
	initFlags()
	flag.Parse()
	parseArguments()
	v := ValkeyNode{
		Address: *bootstrapAddress,
	}
	analyzeCluster(v)
}
