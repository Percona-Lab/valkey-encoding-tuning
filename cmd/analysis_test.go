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
						kObjCount:     1,
						kDistribution: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
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
				kObjCount:       2,
				kHtKeyCount:     uint64(1),
				kElementsCount:  4,
				kMaxElement:     "set:1.large",
				kMaxElementSize: 5,
				kAvgElementSize: float64(3),
				kDistribution:   []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
	}

	output := analysis.renderSetMarkdown()

	g.Expect(output).To(ContainSubstring("- set-max-listpack-value=64"))
	g.Expect(output).To(ContainSubstring("- set-max-listpack-entries=128"))
	g.Expect(output).To(ContainSubstring("- hashtable keys found: 1/2 (50.00% of all set keys)"))
	g.Expect(output).To(ContainSubstring("- set elements count: 4"))
	g.Expect(output).To(ContainSubstring("- largest set element: set:1.large, size:5"))
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
				kObjCount: 0,
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
				kObjCount:       2,
				kSlKeyCount:     uint64(1),
				kElementsCount:  4,
				kMaxElement:     "zset:1.large",
				kMaxElementSize: 5,
				kAvgElementSize: float64(3),
				kDistribution:   []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
	}

	output := analysis.renderZSetMarkdown()

	g.Expect(output).To(ContainSubstring("- zset-max-listpack-value=64"))
	g.Expect(output).To(ContainSubstring("- zset-max-listpack-entries=128"))
	g.Expect(output).To(ContainSubstring("- skiplist keys found: 1/2 (50.00% of all zset keys)"))
	g.Expect(output).To(ContainSubstring("- zset elements count: 4"))
	g.Expect(output).To(ContainSubstring("- largest zset element: zset:1.large, size:5"))
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
				kObjCount: 0,
			},
		},
	}

	output := analysis.renderZSetMarkdown()

	g.Expect(output).To(ContainSubstring("N/A (no keys found)"))
}
