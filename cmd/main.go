package main

import (
	"context"
	"flag"
	"fmt"
	"os"
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

func getClusterNodes(bootstrapNode ValkeyNode) ([]ValkeyNode, error) {
	var nodes []ValkeyNode

	ctx := context.Background()
	bootstrapOptions := bootstrapNode.opts()
	client := createClientWithOptions(bootstrapNode.Address, bootstrapOptions)
	defer client.Close()
	clusterNodes, err := client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
	if err != nil {
		if err.Error() != errNotClusterMode {
			return nil, err
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
	return nodes, nil
}

type clusterAnalysisResult struct {
	Summary   ValkeyNode
	Output    AnalysisOutput
	IsCluster bool
}

func analyzeClusterData(bootstrapNode ValkeyNode) (clusterAnalysisResult, error) {
	nodes, err := getClusterNodes(bootstrapNode)
	if err != nil {
		return clusterAnalysisResult{}, err
	}
	isCluster := len(nodes) > 1
	analyses := make([]Analysis, 0)

	cs := makeValkeyNode("")
	for _, v := range nodes {
		analysis, err := analyzeNode(&v)
		if err != nil {
			return clusterAnalysisResult{}, err
		}
		cs.HashMetrics.updateHashStatistics(&v.HashMetrics)
		cs.ListMetrics.updateListStatistics(&v.ListMetrics)
		cs.SetMetrics.updateSetStatistics(&v.SetMetrics)
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

	return clusterAnalysisResult{
		Summary: cs,
		Output: AnalysisOutput{
			Nodes:   analyses,
			Cluster: &clusterAnalysis,
		},
		IsCluster: isCluster,
	}, nil
}

func analyzeNode(v *ValkeyNode) (Analysis, error) {
	if err := v.getNodeConfig(); err != nil {
		return Analysis{}, fmt.Errorf("get config for node %s: %w", v.Address, err)
	}

	var analysis Analysis
	if err := v.analyzeHash(); err != nil {
		return Analysis{}, fmt.Errorf("analyze hash keys on node %s: %w", v.Address, err)
	}
	v.getHashDatatypeAnalysis(&analysis)

	if err := v.analyzeList(); err != nil {
		return Analysis{}, fmt.Errorf("analyze list keys on node %s: %w", v.Address, err)
	}
	v.getListDatatypeAnalysis(&analysis)

	if err := v.analyzeSet(); err != nil {
		return Analysis{}, fmt.Errorf("analyze set keys on node %s: %w", v.Address, err)
	}
	v.getSetDatatypeAnalysis(&analysis)

	if err := v.analyzeZSet(); err != nil {
		return Analysis{}, fmt.Errorf("analyze zset keys on node %s: %w", v.Address, err)
	}
	v.getZSetDatatypeAnalysis(&analysis)

	return analysis, nil
}

func renderClusterAnalysis(output AnalysisOutput, isCluster bool) {
	fmt.Println("# Hash Datatype Analysis")
	for _, analysis := range output.Nodes {
		fmt.Println(analysis.renderHashMarkdown())
	}
	if isCluster {
		fmt.Println(output.Cluster.renderHashMarkdown())
	}

	fmt.Println("# List Datatype Analysis")
	for _, analysis := range output.Nodes {
		fmt.Println(analysis.renderListMarkdown())
	}
	if isCluster {
		fmt.Println(output.Cluster.renderListMarkdown())
	}

	fmt.Println("# Set Datatype Analysis")
	for _, analysis := range output.Nodes {
		fmt.Println(analysis.renderSetMarkdown())
	}
	if isCluster {
		fmt.Println(output.Cluster.renderSetMarkdown())
	}

	fmt.Println("# Sorted Set Datatype Analysis")
	for _, analysis := range output.Nodes {
		fmt.Println(analysis.renderZSetMarkdown())
	}
	if isCluster {
		fmt.Println(output.Cluster.renderZSetMarkdown())
	}
}

func writeClusterAnalysis(opts *Options, output AnalysisOutput) error {
	if opts.OutputFile == "" {
		return nil
	}
	return writeJson(opts.OutputFile, output)
}

func runClusterAnalysis(bootstrapNode ValkeyNode) (ValkeyNode, error) {
	result, err := analyzeClusterData(bootstrapNode)
	if err != nil {
		return ValkeyNode{}, err
	}
	bootstrapOptions := bootstrapNode.opts()
	if bootstrapOptions.PrintOutput {
		renderClusterAnalysis(result.Output, result.IsCluster)
	}
	if err := writeClusterAnalysis(bootstrapOptions, result.Output); err != nil {
		return ValkeyNode{}, err
	}
	return result.Summary, nil
}

func analyzeCluster(bootstrapNode ValkeyNode) ValkeyNode {
	summary, err := runClusterAnalysis(bootstrapNode)
	if err != nil {
		panic(err)
	}
	return summary
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
	if _, err := runClusterAnalysis(v); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
