package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
)

func TestWriteJsonWritesIndentedAnalysisOutput(t *testing.T) {
	g := NewWithT(t)
	filename := filepath.Join(t.TempDir(), "analysis.json")

	output := AnalysisOutput{
		Nodes: []Analysis{
			{
				Address: "node-1",
				Config:  map[string]string{hashMaxListpack: "64"},
				Metrics: map[string]any{
					"hash": map[string]any{
						"object_count": 1,
						"distribution": []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
					},
				},
			},
		},
	}

	g.Expect(writeJson(filename, output)).To(Succeed())

	data, err := os.ReadFile(filename)
	g.Expect(err).To(BeNil())
	g.Expect(data).To(HaveSuffix("\n"))

	var decoded AnalysisOutput
	g.Expect(json.Unmarshal(data, &decoded)).To(Succeed())
	g.Expect(decoded.Nodes).To(HaveLen(1))
	g.Expect(decoded.Nodes[0].Address).To(Equal("node-1"))
}

func TestRenderSetMarkdown(t *testing.T) {
	g := NewWithT(t)
	analysis := Analysis{
		Address: "node-1",
		Config: map[string]string{
			setMaxListpackValue:   "64",
			setMaxListpackEntries: "128",
		},
		Metrics: map[string]any{
			setDt: map[string]any{
				"object_count":        2,
				"hashtable_key_count": uint64(1),
				"items_count":         4,
				"largest_field":       "set:1.large",
				"largest_field_size":  5,
				"avg_field_size":      float64(3),
				"distribution":        []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
	}

	output := analysis.renderSetMarkdown()

	g.Expect(output).To(ContainSubstring("- set-max-listpack-value=64"))
	g.Expect(output).To(ContainSubstring("- set-max-listpack-entries=128"))
	g.Expect(output).To(ContainSubstring("- hashtable keys found: 1/2 (50.00% of all set keys)"))
	g.Expect(output).To(ContainSubstring("- set members count: 4"))
	g.Expect(output).To(ContainSubstring("- largest set member: set:1.large, size:5"))
	g.Expect(strings.Count(output, "+ P")).To(Equal(10))
}

func TestRenderSetMarkdownWithNoKeys(t *testing.T) {
	g := NewWithT(t)
	analysis := Analysis{
		Address: "node-1",
		Config: map[string]string{
			setMaxListpackValue:   "64",
			setMaxListpackEntries: "128",
		},
		Metrics: map[string]any{
			setDt: map[string]any{
				"object_count": 0,
			},
		},
	}

	output := analysis.renderSetMarkdown()

	g.Expect(output).To(ContainSubstring("N/A (no keys found)"))
}

func TestRenderZSetMarkdown(t *testing.T) {
	g := NewWithT(t)
	analysis := Analysis{
		Address: "node-1",
		Config: map[string]string{
			zsetMaxListpackValue:   "64",
			zsetMaxListpackEntries: "128",
		},
		Metrics: map[string]any{
			zsetDt: map[string]any{
				"object_count":       2,
				"skiplist_key_count": uint64(1),
				"items_count":        4,
				"largest_field":      "zset:1.large",
				"largest_field_size": 5,
				"avg_field_size":     float64(3),
				"distribution":       []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
	}

	output := analysis.renderZSetMarkdown()

	g.Expect(output).To(ContainSubstring("- zset-max-listpack-value=64"))
	g.Expect(output).To(ContainSubstring("- zset-max-listpack-entries=128"))
	g.Expect(output).To(ContainSubstring("- skiplist keys found: 1/2 (50.00% of all zset keys)"))
	g.Expect(output).To(ContainSubstring("- zset members count: 4"))
	g.Expect(output).To(ContainSubstring("- largest zset member: zset:1.large, size:5"))
	g.Expect(strings.Count(output, "+ P")).To(Equal(10))
}

func TestRenderZSetMarkdownWithNoKeys(t *testing.T) {
	g := NewWithT(t)
	analysis := Analysis{
		Address: "node-1",
		Config: map[string]string{
			zsetMaxListpackValue:   "64",
			zsetMaxListpackEntries: "128",
		},
		Metrics: map[string]any{
			zsetDt: map[string]any{
				"object_count": 0,
			},
		},
	}

	output := analysis.renderZSetMarkdown()

	g.Expect(output).To(ContainSubstring("N/A (no keys found)"))
}
