package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
)

type Analysis struct {
	Address string            `json:"address"`
	Config  map[string]string `json:"config"`
	Metrics map[string]any    `json:"metrics"`
}

type AnalysisOutput struct {
	Nodes   []Analysis `json:"nodes"`
	Cluster *Analysis  `json:"cluster,omitempty"`
}

type quantiler interface {
	Quantile(float64) float64
}

func quantileDistribution(q quantiler) []float64 {
	distribution := make([]float64, 10)
	for i := range distribution {
		distribution[i] = q.Quantile(float64(i+1) / 10)
		if math.IsNaN(distribution[i]) {
			distribution[i] = -1
		}
	}
	return distribution
}

func (a *Analysis) init(address string) {
	a.Address = address
	if a.Config == nil {
		a.Config = make(map[string]string)
	}
	if a.Metrics == nil {
		a.Metrics = make(map[string]any)
	}
}

func (a Analysis) datatypeMetrics(datatype string) map[string]any {
	metrics, _ := a.Metrics[datatype].(map[string]any)
	return metrics
}

func (a Analysis) renderHashMarkdown() string {
	var sb strings.Builder
	metrics := a.datatypeMetrics("hash")
	fmt.Fprintf(&sb, "## Node %s\n", a.Address)
	fmt.Fprintln(&sb, "### Config")
	fmt.Fprintf(&sb, "- %s=%s\n", hashMaxListpack, a.Config[hashMaxListpack])
	fmt.Fprintln(&sb, "### Analysis")

	objCount, _ := metrics["object_count"].(int)
	if objCount == 0 {
		fmt.Fprintln(&sb, "N/A (no keys found)")
		return sb.String()
	}

	hashTableCount := metrics["hashtable_key_count"].(uint64)
	fmt.Fprintf(&sb, "- hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", hashTableCount, objCount, (float64(hashTableCount) / float64(objCount) * 100))
	fmt.Fprintf(&sb, "- hash fields count: %d\n", metrics["field_count"].(int))
	fmt.Fprintf(&sb, "- largest hash field: %s, size:%d \n", metrics["largest_field"].(string), metrics["largest_field_size"].(int))
	fmt.Fprintf(&sb, "- avg field size: %.2f\n", metrics["avg_field_size"].(float64))
	fmt.Fprintln(&sb, "- hash fields' size distribution:")
	for i, value := range metrics["distribution"].([]float64) {
		fmt.Fprintf(&sb, "+ P%d: %.2f\n", (i+1)*10, value)
	}
	return sb.String()
}

func (a Analysis) renderListMarkdown() string {
	var sb strings.Builder
	metrics := a.datatypeMetrics("list")
	fmt.Fprintf(&sb, "## Node %s\n", a.Address)
	fmt.Fprintln(&sb, "### Config")
	fmt.Fprintf(&sb, "- %s=%s\n", listMaxListpackSize, a.Config[listMaxListpackSize])
	fmt.Fprintf(&sb, "- %s=%s\n", listCompressDepth, a.Config[listCompressDepth])
	fmt.Fprintln(&sb, "### Analysis")

	objCount, _ := metrics["object_count"].(int64)
	if objCount == 0 {
		fmt.Fprintln(&sb, "N/A (no keys found)")
		return sb.String()
	}

	fmt.Fprintf(&sb, "- list keys found: %d \n", objCount)
	fmt.Fprintf(&sb, "- estimated largest node count:%d \n", metrics["estimated_largest_node_count"].(int64))
	fmt.Fprintf(&sb, "- estimated avg node count: %d\n", metrics["estimated_avg_node_count"].(int64))
	fmt.Fprintf(&sb, "- max element count:%d \n", metrics["max_element_count"].(int64))
	fmt.Fprintf(&sb, "- avg element count: %d\n", metrics["avg_element_count"].(int64))
	fmt.Fprintf(&sb, "- List size distribution (by %s):\n", metrics["size_distribution_type"].(string))
	for i, value := range metrics["distribution"].([]float64) {
		fmt.Fprintf(&sb, "+ P%d: %.2f\n", (i+1)*10, value)
	}
	return sb.String()
}

func (a Analysis) renderSetMarkdown() string {
	var sb strings.Builder
	metrics := a.datatypeMetrics(setDt)
	fmt.Fprintf(&sb, "## Node %s\n", a.Address)
	fmt.Fprintln(&sb, "### Config")
	fmt.Fprintf(&sb, "- %s=%s\n", setMaxListpackValue, a.Config[setMaxListpackValue])
	fmt.Fprintf(&sb, "- %s=%s\n", setMaxListpackEntries, a.Config[setMaxListpackEntries])
	fmt.Fprintln(&sb, "### Analysis")

	objCount, _ := metrics["object_count"].(int)
	if objCount == 0 {
		fmt.Fprintln(&sb, "N/A (no keys found)")
		return sb.String()
	}

	hashTableCount := metrics["hashtable_key_count"].(uint64)
	fmt.Fprintf(&sb, "- hashtable keys found: %d/%d (%.2f%% of all set keys)\n", hashTableCount, objCount, (float64(hashTableCount) / float64(objCount) * 100))
	fmt.Fprintf(&sb, "- set elements count: %d\n", metrics["items_count"].(int))
	fmt.Fprintf(&sb, "- largest set element: %s, size:%d \n", metrics["largest_element"].(string), metrics["largest_element_size"].(int))
	fmt.Fprintf(&sb, "- average element size: %.2f\n", metrics["avg_element_size"].(float64))
	fmt.Fprintln(&sb, "- set elements' size distribution:")
	for i, value := range metrics["distribution"].([]float64) {
		fmt.Fprintf(&sb, "+ P%d: %.2f\n", (i+1)*10, value)
	}
	return sb.String()
}

func (a Analysis) renderZSetMarkdown() string {
	var sb strings.Builder
	metrics := a.datatypeMetrics(zsetDt)
	fmt.Fprintf(&sb, "## Node %s\n", a.Address)
	fmt.Fprintln(&sb, "### Config")
	fmt.Fprintf(&sb, "- %s=%s\n", zsetMaxListpackValue, a.Config[zsetMaxListpackValue])
	fmt.Fprintf(&sb, "- %s=%s\n", zsetMaxListpackEntries, a.Config[zsetMaxListpackEntries])
	fmt.Fprintln(&sb, "### Analysis")

	objCount, _ := metrics["object_count"].(int)
	if objCount == 0 {
		fmt.Fprintln(&sb, "N/A (no keys found)")
		return sb.String()
	}

	skipListCount := metrics["skiplist_key_count"].(uint64)
	fmt.Fprintf(&sb, "- skiplist keys found: %d/%d (%.2f%% of all zset keys)\n", skipListCount, objCount, (float64(skipListCount) / float64(objCount) * 100))
	fmt.Fprintf(&sb, "- zset elements count: %d\n", metrics["items_count"].(int))
	fmt.Fprintf(&sb, "- largest zset element: %s, size:%d \n", metrics["largest_element"].(string), metrics["largest_element_size"].(int))
	fmt.Fprintf(&sb, "- average element size: %.2f\n", metrics["avg_element_size"].(float64))
	fmt.Fprintln(&sb, "- zset elements' size distribution:")
	for i, value := range metrics["distribution"].([]float64) {
		fmt.Fprintf(&sb, "+ P%d: %.2f\n", (i+1)*10, value)
	}
	return sb.String()
}

func writeJson(filename string, output any) error {
	b, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filename, b, 0644)
}
