package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/caio/go-tdigest/v5"
	"github.com/valkey-io/valkey-go"
	"regexp"
	"strconv"
	"strings"
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
	Config          map[string]string
	metrics         ValkeyNodeMetrics
	maxListPackSize int
}

func (v *ValkeyNode) getNodeConfig() error {
	ctx := context.Background()
	client := createClient(v.Address)
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

func (v *ValkeyNode) analyze() error {
	ctx := context.Background()

	client := createClient(v.Address)
	defer client.Close()
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
	v.printNodeAnalysis()
	return nil

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
	cs.printNodeAnalysis()
	return cs
}

func registerStringFlag(name string, value string, usage string) *string {
	var output string
	if flag.Lookup(name) == nil {
		flag.StringVar(&output, name, value, usage)
	} else {
		output = flag.Lookup(name).Value.(flag.Getter).Get().(string)
	}
	return &output
}
func registerBoolFlag(name string, value bool, usage string) *bool {
	var output bool
	if flag.Lookup(name) == nil {
		flag.BoolVar(&output, name, value, usage)
	} else {
		output = flag.Lookup(name).Value.(flag.Getter).Get().(bool)
	}
	return &output

}
func initFlags() {
	bootstrapAddress = registerStringFlag("address", "127.0.0.1:6379", "Valkey node address to connect to, will automatically detect other nodes if it is part of a cluster")
	bootstrapPassword = registerStringFlag("password", "", "Password of the Valkey user")
	bootstrapUsername = registerStringFlag("username", "", "Name of the Valkey user")
	keyPattern = registerStringFlag("key-pattern", "", "Pattern (glob style) of the keys to be analyzed")
	fieldPattern = registerStringFlag("field-pattern", "", "Pattern (regex style) of the hash fields to be analyzed")
	printOutput = registerBoolFlag("print-output", true, "Print output to stdout")
}
func parseArguments() {
	if *fieldPattern != "" {
		fieldPatternRE = regexp.MustCompile(*fieldPattern)
	} else {
		fieldPatternRE = nil
	}
}

func main() {
	flag.Parse()
	initFlags()
	parseArguments()
	v := ValkeyNode{
		Address: *bootstrapAddress,
	}
	analyzeCluster(v)
}
