package main

import (
	"encoding/json"
	"os"
	"path/filepath"
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
