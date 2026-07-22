package main

import (
	"context"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

func setupSetTestNode(t *testing.T) (ValkeyNode, valkey.Client) {
	t.Helper()

	address := createValkeyInstance(false)
	client := createClient(address)
	v := makeValkeyNode(address)
	v.SetMetrics = makeSetMetrics()
	v.Config = map[string]string{
		setMaxListpackValue:   "64",
		setMaxListpackEntries: "128",
	}
	t.Cleanup(func() {
		v.Close()
		cleanupValkeyInstance(address, client)
	})
	return v, client
}

func saddTestSet(t *testing.T, client valkey.Client, key string, members ...string) {
	t.Helper()

	args := append([]string{"SADD", key}, members...)
	err := client.Do(
		context.Background(),
		client.B().Arbitrary(args...).Build(),
	).Error()
	if err != nil {
		t.Fatalf("sadd %q: %v", key, err)
	}
}

func TestMakeSetMetricsInitializesTDigest(t *testing.T) {
	g := NewWithT(t)

	metrics := makeSetMetrics()

	g.Expect(metrics.elementStats.tdigest).NotTo(BeNil())
	g.Expect(metrics.elementStats.tdigest.Count()).To(Equal(uint64(0)))
}

func TestAnalyzeSetScansOnlySetKeys(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupSetTestNode(t)

	saddTestSet(t, client, "set:1", "a", "b")
	saddTestSet(t, client, "set:2", "c", "d")
	err := client.Do(
		context.Background(),
		client.B().Hset().Key("hash:1").FieldValue().FieldValue("field", "value").Build(),
	).Error()
	g.Expect(err).To(BeNil())

	g.Expect(v.analyzeSet()).To(Succeed())
	g.Expect(v.SetMetrics.objCnt).To(Equal(2))
}

func TestAnalyzeSetWithKeyFilterMatchedPattern(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupSetTestNode(t)
	setTestFlag(t, "set-key-pattern", "set:matched:*")

	saddTestSet(t, client, "set:matched:1", "a", "b")
	saddTestSet(t, client, "set:matched:2", "c", "d")
	saddTestSet(t, client, "set:other:1", "d")

	g.Expect(v.analyzeSet()).To(Succeed())
	g.Expect(v.SetMetrics.objCnt).To(Equal(2))
}

func TestAnalyzeSetWithKeyFilterNotMatchingPattern(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupSetTestNode(t)
	setTestFlag(t, "set-key-pattern", "missing:*")

	saddTestSet(t, client, "set:1", "a")
	saddTestSet(t, client, "set:2", "b")

	g.Expect(v.analyzeSet()).To(Succeed())
	g.Expect(v.SetMetrics.objCnt).To(Equal(0))
}

func TestAnalyzeSetMembersUpdatesMemberMetrics(t *testing.T) {
	g := NewWithT(t)
	v, client := setupSetTestNode(t)
	shortMember := "a"
	mediumMember := "medium"
	longMember := strings.Repeat("x", 16)
	otherMember := "zz"
	saddTestSet(t, client, "set:1", shortMember, mediumMember, longMember, otherMember)

	g.Expect(v.analyzeSetMembers("set:1")).To(Succeed())

	g.Expect(v.SetMetrics.elementStats.count).To(Equal(4))
	g.Expect(v.SetMetrics.elementStats.maxSize).To(Equal(len(longMember)))
	g.Expect(v.SetMetrics.elementStats.maxItem).To(Equal("set:1." + longMember))
	g.Expect(v.SetMetrics.elementStats.avgSize).To(Equal(float64((len(shortMember) + len(mediumMember) + len(longMember) + len(otherMember)) / 4)))
	g.Expect(v.SetMetrics.elementStats.tdigest.Count()).To(Equal(uint64(4)))
}

func TestAnalyzeSetMembersDoesNotPanicWithOddMemberCount(t *testing.T) {
	g := NewWithT(t)
	v, client := setupSetTestNode(t)
	saddTestSet(t, client, "set:1", "a", "b", "c")

	g.Expect(func() {
		g.Expect(v.analyzeSetMembers("set:1")).To(Succeed())
	}).NotTo(Panic())
}

func TestAnalyzeSetMembersCountsHashtableCandidates(t *testing.T) {
	g := NewWithT(t)
	v, client := setupSetTestNode(t)
	v.Config[setMaxListpackValue] = "4"
	saddTestSet(t, client, "set:1", "aaaa", "bbbb", "ccccc", "dddddd")

	g.Expect(v.analyzeSetMembers("set:1")).To(Succeed())

	g.Expect(v.SetMetrics.htCnt).To(Equal(uint64(1)))
}

func TestAnalyzeSetMembersReturnsErrorForInvalidSetMaxListpackValue(t *testing.T) {
	g := NewWithT(t)
	v, client := setupSetTestNode(t)
	v.Config[setMaxListpackValue] = "invalid"
	saddTestSet(t, client, "set:1", "a")

	g.Expect(v.analyzeSetMembers("set:1")).To(HaveOccurred())
}

func TestGetSetDatatypeAnalysisPopulatesStruct(t *testing.T) {
	g := NewWithT(t)
	v := makeValkeyNode("node-1")
	v.SetMetrics = makeSetMetrics()
	v.Config = map[string]string{
		setMaxListpackValue:   "64",
		setMaxListpackEntries: "128",
	}
	v.SetMetrics.objCnt = 1
	v.SetMetrics.elementStats.count = 2
	v.SetMetrics.htCnt = 1
	v.SetMetrics.elementStats.maxItem = "set:1.large"
	v.SetMetrics.elementStats.maxSize = 5
	v.SetMetrics.elementStats.avgSize = 3

	var analysis Analysis
	v.getSetDatatypeAnalysis(&analysis)

	g.Expect(analysis.Address).To(Equal("node-1"))
	g.Expect(analysis.Config[setMaxListpackValue]).To(Equal("64"))
	g.Expect(analysis.Config[setMaxListpackEntries]).To(Equal("128"))
	setMetrics := analysis.Metrics[setDt].(map[string]any)
	g.Expect(setMetrics[kObjCnt]).To(Equal(1))
	g.Expect(setMetrics[kElementsCnt]).To(Equal(2))
	g.Expect(setMetrics[kHtKeyCnt]).To(Equal(uint64(1)))
	g.Expect(setMetrics[kMaxElement]).To(Equal("set:1.large"))
	g.Expect(setMetrics[kMaxElementSize]).To(Equal(5))
	g.Expect(setMetrics[kAvgElementSize]).To(Equal(float64(3)))
	g.Expect(setMetrics[kDistribution]).To(HaveLen(10))
}

func TestUpdateSetStatisticsMergesNodeMetrics(t *testing.T) {
	g := NewWithT(t)
	cluster := makeSetMetrics()
	cluster.objCnt = 1
	cluster.elementStats.count = 2
	cluster.elementStats.avgSize = 2
	cluster.htCnt = 1
	cluster.elementStats.maxItem = "set:1.small"
	cluster.elementStats.maxSize = 5
	cluster.elementStats.tdigest.Add(2)

	node := makeSetMetrics()
	node.objCnt = 2
	node.elementStats.count = 3
	node.elementStats.avgSize = 6
	node.htCnt = 2
	node.elementStats.maxItem = "set:2.large"
	node.elementStats.maxSize = 12
	node.elementStats.tdigest.Add(6)

	cluster.updateSetStatistics(&node)

	g.Expect(cluster.objCnt).To(Equal(3))
	g.Expect(cluster.elementStats.count).To(Equal(5))
	g.Expect(cluster.elementStats.avgSize).To(Equal(4.4))
	g.Expect(cluster.htCnt).To(Equal(uint64(3)))
	g.Expect(cluster.elementStats.maxItem).To(Equal("set:2.large"))
	g.Expect(cluster.elementStats.maxSize).To(Equal(12))
	g.Expect(cluster.elementStats.tdigest.Count()).To(Equal(uint64(2)))
}
