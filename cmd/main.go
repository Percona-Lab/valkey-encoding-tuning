package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strings"

	"github.com/valkey-io/valkey-go"
)

const (
	errNotClusterMode = "This instance has cluster support disabled"
)

var (
	options          = defaultOptions()
	flagsInitialized *flag.FlagSet
)

type Options struct {
	Address        string
	Username       string
	Password       string
	HashKeyPattern string
	ListKeyPattern string
	SetKeyPattern  string
	ZSetKeyPattern string
	FieldPattern   string
	FieldPatternRE *regexp.Regexp
	PrintOutput    bool
	OutputFile     string
}

type ValkeyNode struct {
	Address     string
	Options     *Options
	Client      valkey.Client
	Config      map[string]string
	HashMetrics HashMetrics
	ListMetrics ListMetrics
	SetMetrics  SetMetrics
	ZSetMetrics ZSetMetrics
}

func (v *ValkeyNode) getClient() valkey.Client {
	if v.Client == nil {
		v.Client = createClientWithOptions(v.Address, v.opts())
	}
	return v.Client
}

func (v *ValkeyNode) opts() *Options {
	if v.Options == nil {
		v.Options = &options
	}
	return v.Options
}

func (v *ValkeyNode) Close() {
	if v.Client != nil {
		v.Client.Close()
		v.Client = nil
	}
}

func createClient(address string) valkey.Client {
	return createClientWithOptions(address, &options)
}

func createClientWithOptions(address string, opts *Options) valkey.Client {
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
	if opts != nil && opts.Username != "" {
		clientOption.Username = opts.Username
	}
	if opts != nil && opts.Password != "" {
		clientOption.Password = opts.Password
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
		Options:     &options,
		HashMetrics: makeHashMetrics(),
		ListMetrics: makeListMetrics(),
		SetMetrics:  makeSetMetrics(),
		ZSetMetrics: makeZSetMetrics(),
	}
}

func getClusterNodes(bootstrapNode ValkeyNode) []ValkeyNode {
	var nodes []ValkeyNode

	ctx := context.Background()
	bootstrapOptions := bootstrapNode.opts()
	client := createClientWithOptions(bootstrapNode.Address, bootstrapOptions)
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
			node.Options = bootstrapOptions
			nodes = append(nodes, node)
		}
	}
	return nodes
}
func analyzeCluster(bootstrapNode ValkeyNode) ValkeyNode {
	nodes := getClusterNodes(bootstrapNode)
	isCluster := len(nodes) > 1
	analyses := make([]Analysis, 0)

	cs := makeValkeyNode("")
	for _, v := range nodes {
		v.getNodeConfig()

		var analysis Analysis
		v.analyzeHash()
		v.getHashDatatypeAnalysis(&analysis)
		cs.HashMetrics.updateHashStatistics(&v.HashMetrics)

		v.analyzeList()
		v.getListDatatypeAnalysis(&analysis)
		cs.ListMetrics.updateListStatistics(&v.ListMetrics)

		v.analyzeSet()
		v.getSetDatatypeAnalysis(&analysis)
		cs.SetMetrics.updateSetStatistics(&v.SetMetrics)

		v.analyzeZSet()
		v.getZSetDatatypeAnalysis(&analysis)
		cs.ZSetMetrics.updateZSetStatistics(&v.ZSetMetrics)

		analyses = append(analyses, analysis)
	}
	var clusterAnalysis Analysis
	if isCluster {
		cs.getHashDatatypeAnalysis(&clusterAnalysis)
		cs.getListDatatypeAnalysis(&clusterAnalysis)
		cs.getSetDatatypeAnalysis(&clusterAnalysis)
		cs.getZSetDatatypeAnalysis(&clusterAnalysis)
	}

	bootstrapOptions := bootstrapNode.opts()
	if bootstrapOptions.PrintOutput {

		fmt.Println("# Hash Datatype Analysis")
		for _, analysis := range analyses {
			fmt.Println(analysis.renderHashMarkdown())
		}
		if isCluster {
			fmt.Println(clusterAnalysis.renderHashMarkdown())
		}

		fmt.Println("# List Datatype Analysis")
		for _, analysis := range analyses {
			fmt.Println(analysis.renderListMarkdown())
		}
		if isCluster {
			fmt.Println(clusterAnalysis.renderListMarkdown())
		}

		fmt.Println("# Set Datatype Analysis")
		for _, analysis := range analyses {
			fmt.Println(analysis.renderSetMarkdown())
		}
		if isCluster {
			fmt.Println(clusterAnalysis.renderSetMarkdown())
		}

		fmt.Println("# Sorted Set Datatype Analysis")
		for _, analysis := range analyses {
			fmt.Println(analysis.renderZSetMarkdown())
		}
		if isCluster {
			fmt.Println(clusterAnalysis.renderZSetMarkdown())
		}
	}
	if bootstrapOptions.OutputFile != "" {
		err := writeJson(bootstrapOptions.OutputFile,
			map[string]any{
				"nodes":   analyses,
				"cluster": clusterAnalysis,
			},
		)
		if err != nil {
			panic(err)
		}
	}
	return cs
}

func defaultOptions() Options {
	return Options{
		Address:     "127.0.0.1:6379",
		PrintOutput: true,
	}
}

func initFlags() {
	if flagsInitialized == flag.CommandLine {
		return
	}
	options = defaultOptions()
	flag.StringVar(&options.Address, "address", options.Address, "Valkey node address to connect to, will automatically detect other nodes if it is part of a cluster")
	flag.StringVar(&options.Password, "password", "", "Password of the Valkey user")
	flag.StringVar(&options.Username, "username", "", "Name of the Valkey user")
	flag.StringVar(&options.HashKeyPattern, "hash-key-pattern", "", "Pattern (glob style) of the HASH keys to be analyzed")
	flag.StringVar(&options.ListKeyPattern, "list-key-pattern", "", "Pattern (glob style) of the LIST keys to be analyzed")
	flag.StringVar(&options.SetKeyPattern, "set-key-pattern", "", "Pattern (glob style) of the SET keys to be analyzed")
	flag.StringVar(&options.ZSetKeyPattern, "zset-key-pattern", "", "Pattern (glob style) of the SORTED SET keys to be analyzed")
	flag.StringVar(&options.FieldPattern, "field-pattern", "", "Pattern (regex style) of the hash fields to be analyzed")
	flag.BoolVar(&options.PrintOutput, "print-output", options.PrintOutput, "Print output to stdout")
	flag.StringVar(&options.OutputFile, "output-file", "", "Output file name")
	flagsInitialized = flag.CommandLine
}
func parseArguments() {
	if options.FieldPattern != "" {
		options.FieldPatternRE = regexp.MustCompile(options.FieldPattern)
	} else {
		options.FieldPatternRE = nil
	}
}

func main() {
	initFlags()
	flag.Parse()
	parseArguments()
	v := makeValkeyNode(options.Address)
	analyzeCluster(v)
}
