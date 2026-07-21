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

	g.Expect(metrics.tdigest).NotTo(BeNil())
	g.Expect(metrics.tdigest.Count()).To(Equal(uint64(0)))
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
	g.Expect(v.SetMetrics.objCount).To(Equal(2))
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
	g.Expect(v.SetMetrics.objCount).To(Equal(2))
}

func TestAnalyzeSetWithKeyFilterNotMatchingPattern(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupSetTestNode(t)
	setTestFlag(t, "set-key-pattern", "missing:*")

	saddTestSet(t, client, "set:1", "a")
	saddTestSet(t, client, "set:2", "b")

	g.Expect(v.analyzeSet()).To(Succeed())
	g.Expect(v.SetMetrics.objCount).To(Equal(0))
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

	g.Expect(v.SetMetrics.memberCount).To(Equal(4), "expect SetMetrics.memberCount to be %d, got %d", 4, v.SetMetrics.memberCount)
	g.Expect(v.SetMetrics.maxElementSize).To(Equal(len(longMember)))
	g.Expect(v.SetMetrics.maxElement).To(Equal("set:1." + longMember))
	g.Expect(v.SetMetrics.avgElementSize).To(Equal(float64((len(shortMember) + len(mediumMember) + len(longMember) + len(otherMember)) / 4)))
	g.Expect(v.SetMetrics.tdigest.Count()).To(Equal(uint64(4)))
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

	g.Expect(v.SetMetrics.hashTableCount).To(Equal(uint64(1)))
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
	v.SetMetrics.objCount = 1
	v.SetMetrics.memberCount = 2
	v.SetMetrics.hashTableCount = 1
	v.SetMetrics.maxElement = "set:1.large"
	v.SetMetrics.maxElementSize = 5
	v.SetMetrics.avgElementSize = 3

	var analysis Analysis
	v.getSetDatatypeAnalysis(&analysis)

	g.Expect(analysis.Address).To(Equal("node-1"))
	g.Expect(analysis.Config[setMaxListpackValue]).To(Equal("64"))
	g.Expect(analysis.Config[setMaxListpackEntries]).To(Equal("128"))
	setMetrics := analysis.Metrics[setDt].(map[string]any)
	g.Expect(setMetrics["object_count"]).To(Equal(1))
	g.Expect(setMetrics["items_count"]).To(Equal(2))
	g.Expect(setMetrics["hashtable_key_count"]).To(Equal(uint64(1)))
	g.Expect(setMetrics["largest_element"]).To(Equal("set:1.large"))
	g.Expect(setMetrics["largest_element_size"]).To(Equal(5))
	g.Expect(setMetrics["avg_element_size"]).To(Equal(float64(3)))
	g.Expect(setMetrics["distribution"]).To(HaveLen(10))
}

func TestUpdateSetStatisticsMergesNodeMetrics(t *testing.T) {
	g := NewWithT(t)
	cluster := makeSetMetrics()
	cluster.objCount = 1
	cluster.memberCount = 2
	cluster.avgElementSize = 2
	cluster.hashTableCount = 1
	cluster.maxElement = "set:1.small"
	cluster.maxElementSize = 5
	cluster.tdigest.Add(2)

	node := makeSetMetrics()
	node.objCount = 2
	node.memberCount = 3
	node.avgElementSize = 6
	node.hashTableCount = 2
	node.maxElement = "set:2.large"
	node.maxElementSize = 12
	node.tdigest.Add(6)

	cluster.updateSetStatistics(&node)

	g.Expect(cluster.objCount).To(Equal(3))
	g.Expect(cluster.memberCount).To(Equal(5))
	g.Expect(cluster.avgElementSize).To(Equal(4.4))
	g.Expect(cluster.hashTableCount).To(Equal(uint64(3)))
	g.Expect(cluster.maxElement).To(Equal("set:2.large"))
	g.Expect(cluster.maxElementSize).To(Equal(12))
	g.Expect(cluster.tdigest.Count()).To(Equal(uint64(2)))
}
