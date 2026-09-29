package main

import (
	"testing"

	. "github.com/onsi/gomega"
)

func setupHashTestNode(t *testing.T, hashKeysCount int) ValkeyNode {
	t.Helper()
	initTestFlags(t)

	g := NewWithT(t)
	address := createValkeyInstance(true)
	g.Eventually(address).To(BeAnExistingFile())
	setTestFlag(t, "username", "default")
	setTestFlag(t, "password", defaultPassword)
	client := createClient(address)
	generateTestData(t, address, 0, hashKeysCount)

	v := makeValkeyNode(address)
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})
	return v
}

func TestAnalyzeNode(t *testing.T) {
	hashKeysCount := 1000
	g := NewWithT(t)
	v := setupHashTestNode(t, hashKeysCount)

	setTestFlag(t, "print-output", "false")
	parseArguments()

	g.Expect(v.getNodeConfig()).To(Succeed())
	g.Expect(v.analyzeHash(0)).To(Succeed())
	g.Expect(v.HashMetrics.objCnt).To(Equal(hashKeysCount))
}

func TestAnalyzeWithKeyFilterMatchedPattern(t *testing.T) {
	hashKeysCount := 1000
	g := NewWithT(t)
	v := setupHashTestNode(t, hashKeysCount)

	setTestFlag(t, "hash-key-pattern", "{db0}:item*")
	setTestFlag(t, "print-output", "false")
	parseArguments()

	g.Expect(v.getNodeConfig()).To(Succeed())
	g.Expect(v.analyzeHash(0)).To(Succeed())
	g.Expect(v.HashMetrics.objCnt).To(Equal(hashKeysCount))
}

func TestAnalyzeWithKeyFilterNotMatchingPattern(t *testing.T) {
	hashKeysCount := 1000
	g := NewWithT(t)
	v := setupHashTestNode(t, hashKeysCount)

	setTestFlag(t, "print-output", "false")
	setTestFlag(t, "hash-key-pattern", "item-not-exists*")
	parseArguments()
	g.Expect((v.HashMetrics.objCnt)).To(Equal(0))

	g.Expect(v.analyzeHash(0)).To(Succeed())
	g.Expect((v.HashMetrics.objCnt)).To(Equal(0))
}

func TestAnalyzeWithFieldFilterMatchedPattern(t *testing.T) {
	hashKeysCount := 1000
	g := NewWithT(t)
	v := setupHashTestNode(t, hashKeysCount)

	setTestFlag(t, "field-pattern", "nam.+")
	setTestFlag(t, "print-output", "false")
	parseArguments()
	g.Expect(v.getNodeConfig()).To(Succeed())
	g.Expect(v.analyzeHash(0)).To(Succeed())
	g.Expect(v.HashMetrics.fieldStats.count).To(Equal(hashKeysCount * 2))
	g.Expect(v.HashMetrics.fieldStats.maxSizeItem).To(ContainSubstring(".name"))
}

func TestAnalyzeWithFieldNotMatchingFilter(t *testing.T) {
	hashKeysCount := 1000
	g := NewWithT(t)
	v := setupHashTestNode(t, hashKeysCount)

	setTestFlag(t, "field-pattern", "namo.+")
	setTestFlag(t, "print-output", "false")
	parseArguments()
	g.Expect(v.getNodeConfig()).To(Succeed())
	g.Expect(v.analyzeHash(0)).To(Succeed())
	g.Expect(v.HashMetrics.fieldStats.count).To(Equal(0))
	g.Expect(v.HashMetrics.fieldStats.maxSizeItem).To(BeEmpty())
}

func TestGetHashDatatypeAnalysisPopulatesMaxFieldCount(t *testing.T) {
	g := NewWithT(t)
	v := makeValkeyNode("node-1")
	v.HashMetrics = makeHashMetrics()
	v.Config = map[string]string{
		hashMaxListpack: "64",
		hashMaxEntries:  "512",
	}
	v.HashMetrics.objCnt = 1
	v.HashMetrics.fieldStats.count = 4
	v.HashMetrics.htCnt = 1
	v.HashMetrics.fieldStats.maxSizeItem = "hash:1.large (field value)"
	v.HashMetrics.fieldStats.maxSize = 42
	v.HashMetrics.fieldStats.maxFieldCount = 3
	v.HashMetrics.fieldStats.maxFieldCountItem = "hash:1"
	v.HashMetrics.fieldStats.avgSize = 6

	var analysis Analysis
	v.getHashDatatypeAnalysis(&analysis)

	g.Expect(analysis.Address).To(Equal("node-1"))
	g.Expect(analysis.Config[hashMaxListpack]).To(Equal("64"))
	g.Expect(analysis.Config[hashMaxEntries]).To(Equal("512"))
	hashMetrics := analysis.Metrics[hashDt].(map[string]any)
	g.Expect(hashMetrics[kObjCnt]).To(Equal(1))
	g.Expect(hashMetrics[kFieldCnt]).To(Equal(4))
	g.Expect(hashMetrics[kHtKeyCnt]).To(Equal(uint64(1)))
	g.Expect(hashMetrics[kMaxElement]).To(Equal("hash:1.large (field value)"))
	g.Expect(hashMetrics[kMaxElementSize]).To(Equal(42))
	g.Expect(hashMetrics[kMaxEntriesCnt]).To(Equal(3))
	g.Expect(hashMetrics[kMaxEntries]).To(Equal("hash:1"))
	g.Expect(hashMetrics[kAvgElementSize]).To(Equal(float64(6)))
	g.Expect(hashMetrics[kDistribution]).To(HaveLen(10))
}

func TestUpdateHashStatisticsMergesMaxFieldCount(t *testing.T) {
	g := NewWithT(t)
	cluster := makeHashMetrics()
	cluster.objCnt = 1
	cluster.fieldStats.count = 2
	cluster.fieldStats.avgSize = 2
	cluster.htCnt = 1
	cluster.fieldStats.maxSizeItem = "hash:1.small"
	cluster.fieldStats.maxSize = 5
	cluster.fieldStats.maxFieldCount = 2
	cluster.fieldStats.maxFieldCountItem = "hash:1"
	cluster.fieldStats.tdigest.Add(2)

	node := makeHashMetrics()
	node.objCnt = 2
	node.fieldStats.count = 3
	node.fieldStats.avgSize = 6
	node.htCnt = 2
	node.fieldStats.maxSizeItem = "hash:2.large"
	node.fieldStats.maxSize = 12
	node.fieldStats.maxFieldCount = 4
	node.fieldStats.maxFieldCountItem = "hash:2"
	node.fieldStats.tdigest.Add(6)

	cluster.updateHashStatistics(&node)

	g.Expect(cluster.objCnt).To(Equal(3))
	g.Expect(cluster.fieldStats.count).To(Equal(5))
	g.Expect(cluster.fieldStats.avgSize).To(Equal(4.4))
	g.Expect(cluster.htCnt).To(Equal(uint64(3)))
	g.Expect(cluster.fieldStats.maxSizeItem).To(Equal("hash:2.large"))
	g.Expect(cluster.fieldStats.maxSize).To(Equal(12))
	g.Expect(cluster.fieldStats.maxFieldCount).To(Equal(4))
	g.Expect(cluster.fieldStats.maxFieldCountItem).To(Equal("hash:2"))
	g.Expect(cluster.fieldStats.tdigest.Count()).To(Equal(uint64(2)))
}
