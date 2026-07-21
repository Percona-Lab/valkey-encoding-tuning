package main

import (
	"context"
	"strconv"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

func setupZSetTestNode(t *testing.T) (ValkeyNode, valkey.Client) {
	t.Helper()

	address := createValkeyInstance(false)
	client := createClient(address)
	v := makeValkeyNode(address)
	v.Config = map[string]string{
		zsetMaxListpackValue:   "64",
		zsetMaxListpackEntries: "128",
	}
	t.Cleanup(func() {
		v.Close()
		cleanupValkeyInstance(address, client)
	})
	return v, client
}

func zaddTestZSet(t *testing.T, client valkey.Client, key string, members ...string) {
	t.Helper()

	args := []string{"ZADD", key}
	for i, member := range members {
		args = append(args, strconv.Itoa(i+1), member)
	}
	err := client.Do(
		context.Background(),
		client.B().Arbitrary(args...).Build(),
	).Error()
	if err != nil {
		t.Fatalf("zadd %q: %v", key, err)
	}
}

func TestMakeZSetMetricsInitializesTDigest(t *testing.T) {
	g := NewWithT(t)

	metrics := makeZSetMetrics()

	g.Expect(metrics.tdigest).NotTo(BeNil())
	g.Expect(metrics.tdigest.Count()).To(Equal(uint64(0)))
}

func TestAnalyzeZSetScansOnlyZSetKeys(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupZSetTestNode(t)

	zaddTestZSet(t, client, "zset:1", "a", "b")
	zaddTestZSet(t, client, "zset:2", "c", "d")
	saddTestSet(t, client, "set:1", "a", "b")

	g.Expect(func() {
		g.Expect(v.analyzeZSet()).To(Succeed())
	}).NotTo(Panic())
	g.Expect(v.ZSetMetrics.objCount).To(Equal(2))
}

func TestAnalyzeZSetWithKeyFilterMatchedPattern(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupZSetTestNode(t)
	setTestFlag(t, "zset-key-pattern", "zset:matched:*")

	zaddTestZSet(t, client, "zset:matched:1", "a", "b")
	zaddTestZSet(t, client, "zset:matched:2", "c", "d")
	zaddTestZSet(t, client, "zset:other:1", "e")

	g.Expect(v.analyzeZSet()).To(Succeed())
	g.Expect(v.ZSetMetrics.objCount).To(Equal(2))
}

func TestAnalyzeZSetWithKeyFilterNotMatchingPattern(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupZSetTestNode(t)
	setTestFlag(t, "zset-key-pattern", "missing:*")

	zaddTestZSet(t, client, "zset:1", "a")
	zaddTestZSet(t, client, "zset:2", "b")

	g.Expect(v.analyzeZSet()).To(Succeed())
	g.Expect(v.ZSetMetrics.objCount).To(Equal(0))
}

func TestAnalyzeZSetMembersUpdatesMemberMetrics(t *testing.T) {
	g := NewWithT(t)
	v, client := setupZSetTestNode(t)
	shortMember := "a"
	mediumMember := "medium"
	longMember := strings.Repeat("x", 16)
	otherMember := "zz"
	zaddTestZSet(t, client, "zset:1", shortMember, mediumMember, longMember, otherMember)

	g.Expect(v.analyzeZSetMembers("zset:1")).To(Succeed())

	g.Expect(v.ZSetMetrics.elementCount).To(Equal(4))
	g.Expect(v.ZSetMetrics.maxElementSize).To(Equal(len(longMember)))
	g.Expect(v.ZSetMetrics.maxElement).To(Equal("zset:1." + longMember))
	g.Expect(v.ZSetMetrics.avgElementSize).To(Equal(float64((len(shortMember) + len(mediumMember) + len(longMember) + len(otherMember)) / 4)))
	g.Expect(v.ZSetMetrics.tdigest.Count()).To(Equal(uint64(4)))
}

func TestAnalyzeZSetMembersCountsSkiplistCandidates(t *testing.T) {
	g := NewWithT(t)
	v, client := setupZSetTestNode(t)
	v.Config[zsetMaxListpackValue] = "4"
	zaddTestZSet(t, client, "zset:1", "aaa", "bbbbb")

	g.Expect(v.analyzeZSetMembers("zset:1")).To(Succeed())

	g.Expect(v.ZSetMetrics.skipListCount).To(Equal(uint64(1)))
}

func TestAnalyzeZSetMembersReturnsErrorForInvalidZSetMaxListpackValue(t *testing.T) {
	g := NewWithT(t)
	v, client := setupZSetTestNode(t)
	v.Config[zsetMaxListpackValue] = "invalid"
	zaddTestZSet(t, client, "zset:1", "a")

	g.Expect(v.analyzeZSetMembers("zset:1")).To(HaveOccurred())
}

func TestGetZSetDatatypeAnalysisPopulatesStruct(t *testing.T) {
	g := NewWithT(t)
	v := makeValkeyNode("node-1")
	v.Config = map[string]string{
		zsetMaxListpackValue:   "64",
		zsetMaxListpackEntries: "128",
	}
	v.ZSetMetrics.objCount = 1
	v.ZSetMetrics.elementCount = 2
	v.ZSetMetrics.skipListCount = 1
	v.ZSetMetrics.maxElement = "zset:1.large"
	v.ZSetMetrics.maxElementSize = 5
	v.ZSetMetrics.avgElementSize = 3

	var analysis Analysis
	v.getZSetDatatypeAnalysis(&analysis)

	g.Expect(analysis.Address).To(Equal("node-1"))
	g.Expect(analysis.Config[zsetMaxListpackValue]).To(Equal("64"))
	g.Expect(analysis.Config[zsetMaxListpackEntries]).To(Equal("128"))
	zsetMetrics, ok := analysis.Metrics[zsetDt].(map[string]any)
	g.Expect(ok).To(BeTrue())
	if !ok {
		return
	}
	g.Expect(zsetMetrics[kObjCount]).To(Equal(1))
	g.Expect(zsetMetrics[kElementsCount]).To(Equal(2))
	g.Expect(zsetMetrics[kSlKeyCount]).To(Equal(uint64(1)))
	g.Expect(zsetMetrics[kMaxElement]).To(Equal("zset:1.large"))
	g.Expect(zsetMetrics[kMaxElementSize]).To(Equal(5))
	g.Expect(zsetMetrics[kAvgElementSize]).To(Equal(float64(3)))
	g.Expect(zsetMetrics[kDistribution]).To(HaveLen(10))
}

func TestUpdateZSetStatisticsMergesNodeMetrics(t *testing.T) {
	g := NewWithT(t)
	cluster := makeZSetMetrics()
	cluster.objCount = 1
	cluster.elementCount = 2
	cluster.avgElementSize = 2
	cluster.skipListCount = 1
	cluster.maxElement = "zset:1.small"
	cluster.maxElementSize = 5
	cluster.tdigest.Add(2)

	node := makeZSetMetrics()
	node.objCount = 2
	node.elementCount = 3
	node.avgElementSize = 6
	node.skipListCount = 2
	node.maxElement = "zset:2.large"
	node.maxElementSize = 12
	node.tdigest.Add(6)

	cluster.updateZSetStatistics(&node)

	g.Expect(cluster.objCount).To(Equal(3))
	g.Expect(cluster.elementCount).To(Equal(5))
	g.Expect(cluster.avgElementSize).To(Equal(4.4))
	g.Expect(cluster.skipListCount).To(Equal(uint64(3)))
	g.Expect(cluster.maxElement).To(Equal("zset:2.large"))
	g.Expect(cluster.maxElementSize).To(Equal(12))
	g.Expect(cluster.tdigest.Count()).To(Equal(uint64(2)))
}
