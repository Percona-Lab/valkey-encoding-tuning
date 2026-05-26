package main

import (
	"context"
	"flag"
	"regexp"
	"strings"

	"github.com/valkey-io/valkey-go"
)

const (
	errNotClusterMode = "This instance has cluster support disabled"
)

var (
	bootstrapAddress  *string
	bootstrapUsername *string
	bootstrapPassword *string
	hashKeyPattern    *string
	listKeyPattern    *string
	fieldPattern      *string
	fieldPatternRE    *regexp.Regexp
	printOutput       *bool
	flagsInitialized  *flag.FlagSet
)

type ValkeyNode struct {
	Address     string
	Client      valkey.Client
	Config      map[string]string
	HashMetrics HashMetrics
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

func makeValkeyNode(address string) ValkeyNode {
	return ValkeyNode{
		Address:     address,
		HashMetrics: makeHashMetrics(),
	}
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
			node := makeValkeyNode(strings.Split(nodeDetails[1], "@")[0])
			nodes = append(nodes, node)
		}
	}
	return nodes
}
func analyzeCluster(bootstrapNode ValkeyNode) ValkeyNode {
	nodes := getClusterNodes(bootstrapNode)
	cs := makeValkeyNode("")
	for _, v := range nodes {
		v.getNodeConfig()
		v.analyzeHash()
		cs.updateHashStatistics(&v)
	}
	cs.printHashDatatypeAnalysis()
	return cs
}

func initFlags() {
	if flagsInitialized == flag.CommandLine {
		return
	}
	bootstrapAddress = flag.String("address", "127.0.0.1:6379", "Valkey node address to connect to, will automatically detect other nodes if it is part of a cluster")
	bootstrapPassword = flag.String("password", "", "Password of the Valkey user")
	bootstrapUsername = flag.String("username", "", "Name of the Valkey user")
	hashKeyPattern = flag.String("key-pattern", "", "Pattern (glob style) of the keys to be analyzed")
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
	v := makeValkeyNode(*bootstrapAddress)
	analyzeCluster(v)
}
