package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
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
	Databases      []int64
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
	return createClientWithDatabase(address, opts, 0)
}

func createClientWithDatabase(address string, opts *Options, db int64) valkey.Client {
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
	clientOption.SelectDB = int(db)
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
	Database  int64
	Summary   ValkeyNode
	Output    AnalysisOutput
	IsCluster bool
}

func analyzeClusterData(bootstrapNode ValkeyNode) ([]clusterAnalysisResult, error) {
	nodes, err := getClusterNodes(bootstrapNode)
	if err != nil {
		return nil, err
	}
	isCluster := len(nodes) > 1
	results := make([]clusterAnalysisResult, 0, len(options.Databases))
	for _, db := range options.Databases {
		analyses := make([]Analysis, 0)
		cs := makeValkeyNode("")
		for _, v := range nodes {
			analysis, err := analyzeNode(&v, db)
			if err != nil {
				return nil, err
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
		result := clusterAnalysisResult{
			Database: db,
			Summary:  cs,
			Output: AnalysisOutput{
				Database: db,
				Nodes:    analyses,
				Cluster:  &clusterAnalysis,
			},
			IsCluster: isCluster,
		}
		results = append(results, result)
	}
	return results, nil
}

func analyzeNode(v *ValkeyNode, db int64) (Analysis, error) {
	if err := v.getNodeConfig(); err != nil {
		return Analysis{}, fmt.Errorf("get config for node %s: %w", v.Address, err)
	}

	var analysis Analysis
	if err := v.analyzeHash(db); err != nil {
		return Analysis{}, fmt.Errorf("analyze hash keys for database '%d' on node %s: %w", db, v.Address, err)
	}
	v.getHashDatatypeAnalysis(&analysis)

	if err := v.analyzeList(db); err != nil {
		return Analysis{}, fmt.Errorf("analyze list keys for database '%d'  on node %s: %w", db, v.Address, err)
	}
	v.getListDatatypeAnalysis(&analysis)

	if err := v.analyzeSet(db); err != nil {
		return Analysis{}, fmt.Errorf("analyze set keys for database '%d' on node %s: %w", db, v.Address, err)
	}
	v.getSetDatatypeAnalysis(&analysis)

	if err := v.analyzeZSet(db); err != nil {
		return Analysis{}, fmt.Errorf("analyze zset keys for database '%d' on node %s: %w", db, v.Address, err)
	}
	v.getZSetDatatypeAnalysis(&analysis)

	return analysis, nil
}

func renderClusterAnalysis(output AnalysisOutput, isCluster bool) {
	fmt.Printf("# DB %d Analysis\n", output.Database)
	fmt.Println("## Hash Datatype")
	for _, analysis := range output.Nodes {
		fmt.Println(analysis.renderHashMarkdown())
	}
	if isCluster {
		fmt.Println(output.Cluster.renderHashMarkdown())
	}

	fmt.Println("## List Datatype")
	for _, analysis := range output.Nodes {
		fmt.Println(analysis.renderListMarkdown())
	}
	if isCluster {
		fmt.Println(output.Cluster.renderListMarkdown())
	}

	fmt.Println("## Set Datatype")
	for _, analysis := range output.Nodes {
		fmt.Println(analysis.renderSetMarkdown())
	}
	if isCluster {
		fmt.Println(output.Cluster.renderSetMarkdown())
	}

	fmt.Println("## Sorted Set Datatype")
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

func runClusterAnalysis(bootstrapNode ValkeyNode) ([]ValkeyNode, error) {
	results, err := analyzeClusterData(bootstrapNode)
	if err != nil {
		return nil, err
	}
	output := make([]ValkeyNode, len(results))
	bootstrapOptions := bootstrapNode.opts()
	for i, result := range results {
		if bootstrapOptions.PrintOutput {
			renderClusterAnalysis(result.Output, result.IsCluster)
		}
		if err := writeClusterAnalysis(bootstrapOptions, result.Output); err != nil {
			return nil, err
		}
		output[i] = result.Summary
	}
	return output, nil
}

func analyzeCluster(bootstrapNode ValkeyNode) []ValkeyNode {
	summaries, err := runClusterAnalysis(bootstrapNode)
	if err != nil {
		panic(err)
	}
	return summaries
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
	flag.Func("database", "Comma-separated list of database to analyze, default to '0'", func(s string) error {
		if strings.Contains(s, ",") {
			dbs := strings.Split(s, ",")
			for _, db := range dbs {
				i, err := strconv.Atoi(db)
				if err != nil {
					return err
				}
				options.Databases = append(options.Databases, int64(i))
			}
		} else {
			db, err := strconv.Atoi(s)
			if err != nil {
				return err
			}
			options.Databases = append(options.Databases, int64(db))
		}
		return nil
	})
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
	if len(options.Databases) == 0 {
		options.Databases = []int64{0}
	} else {
		// remove duplicate & sort
		slices.Sort(options.Databases)
		options.Databases = slices.Compact(options.Databases)
	}
	v := makeValkeyNode(options.Address)
	if _, err := runClusterAnalysis(v); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
