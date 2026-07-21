package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

func setupListTestNode(t *testing.T) (ValkeyNode, valkey.Client) {
	t.Helper()

	address := createValkeyInstance(false)
	client := createClient(address)
	v := makeValkeyNode(address)
	v.Config = map[string]string{
		listMaxListpackSize: "-2",
	}
	t.Cleanup(func() {
		v.Close()
		cleanupValkeyInstance(address, client)
	})
	return v, client
}

func rpushTestList(t *testing.T, client valkey.Client, key string, elements ...string) {
	t.Helper()

	err := client.Do(
		context.Background(),
		client.B().Rpush().Key(key).Element(elements...).Build(),
	).Error()
	if err != nil {
		t.Fatalf("rpush %q: %v", key, err)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	original := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = original

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return buf.String()
}

func TestMakeListMetricsInitializesTDigest(t *testing.T) {
	g := NewWithT(t)

	metrics := makeListMetrics()

	g.Expect(metrics.tdigest).NotTo(BeNil())
	g.Expect(metrics.tdigest.Count()).To(Equal(uint64(0)))
}

func TestAnalyzeListScansOnlyListKeys(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	setTestFlag(t, "print-output", "false")

	rpushTestList(t, client, "list:1", "a", "b")
	rpushTestList(t, client, "list:2", "c")
	err := client.Do(
		context.Background(),
		client.B().Hset().Key("hash:1").FieldValue().FieldValue("field", "value").Build(),
	).Error()
	g.Expect(err).To(BeNil())

	g.Expect(v.analyzeList()).To(Succeed())
	g.Expect(v.ListMetrics.objCnt).To(Equal(int64(2)))
}

func TestAnalyzeListWithKeyFilterMatchedPattern(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	setTestFlag(t, "list-key-pattern", "list:matched:*")

	rpushTestList(t, client, "list:matched:1", "a")
	rpushTestList(t, client, "list:matched:2", "b")
	rpushTestList(t, client, "list:other:1", "c")

	g.Expect(v.analyzeList()).To(Succeed())
	g.Expect(v.ListMetrics.objCnt).To(Equal(int64(2)))
}

func TestAnalyzeListWithKeyFilterNotMatchingPattern(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	setTestFlag(t, "list-key-pattern", "missing:*")

	rpushTestList(t, client, "list:1", "a")
	rpushTestList(t, client, "list:2", "b")

	g.Expect(v.analyzeList()).To(Succeed())
	g.Expect(v.ListMetrics.objCnt).To(Equal(int64(0)))
}

func TestAnalyzeListKeyUpdatesObjectCount(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	rpushTestList(t, client, "list:1", "a", "b")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.objCnt).To(Equal(int64(1)))
}

func TestAnalyzeListKeyUpdatesElementCountMetrics(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	rpushTestList(t, client, "list:1", "a", "b", "c")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.maxElementCnt).To(Equal(int64(3)))
	g.Expect(v.ListMetrics.avgElementCnt).To(Equal(int64(3)))
}

func TestAnalyzeListKeyUpdatesObjectSizeMetrics(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	rpushTestList(t, client, "list:1", "a", "b", "c")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.maxObjSize).To(BeNumerically(">", 0))
	g.Expect(v.ListMetrics.avgObjSize).To(BeNumerically(">", 0))
}

func TestAnalyzeListKeyUsesObjectSizeDistributionWhenListMaxListpackSizeIsNegative(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	v.Config[listMaxListpackSize] = "-2"
	rpushTestList(t, client, "list:1", "a", "b", "c")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.tdigest.Count()).To(Equal(uint64(1)))
	g.Expect(v.ListMetrics.tdigest.Quantile(0.5)).To(BeNumerically(">", 0))
}

func TestAnalyzeListKeyUsesElementCountDistributionWhenListMaxListpackSizeIsPositive(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	v.Config[listMaxListpackSize] = "10"
	rpushTestList(t, client, "list:1", "a", "b", "c")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.tdigest.Count()).To(Equal(uint64(1)))
	g.Expect(v.ListMetrics.tdigest.Quantile(0.5)).To(Equal(float64(3)))
}

func TestAnalyzeListKeyCalculatesNodeCountFromOptimizationLevel(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	v.Config[listMaxListpackSize] = "-5"
	rpushTestList(t, client, "list:1", "a", "b", "c")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	expectedNodeCount := v.ListMetrics.maxObjSize / int64(optimizationLevel["-5"])
	if expectedNodeCount < 1 {
		expectedNodeCount = 1
	}
	g.Expect(v.ListMetrics.maxNodeCnt).To(Equal(expectedNodeCount))
}

func TestAnalyzeListKeySetsMinimumNodeCountToOne(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	v.Config[listMaxListpackSize] = "10"
	rpushTestList(t, client, "list:1", "a")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.maxNodeCnt).To(Equal(int64(1)))
	g.Expect(v.ListMetrics.avgNodeCnt).To(Equal(int64(1)))
}

func TestAnalyzeListKeyCalculatesMultipleNodesByElementCount(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	v.Config[listMaxListpackSize] = "2"
	rpushTestList(t, client, "list:1", "a", "b", "c", "d", "e")

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.maxNodeCnt).To(Equal(int64(3)))
	g.Expect(v.ListMetrics.avgNodeCnt).To(Equal(int64(3)))
}

func TestAnalyzeListKeyCalculatesMultipleNodesByObjectSize(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	v.Config[listMaxListpackSize] = "-1"
	largeElement := strings.Repeat("x", optimizationLevel["-1"])
	rpushTestList(t, client, "list:1", largeElement, largeElement, largeElement)

	g.Expect(v.analyzeListKey("list:1")).To(Succeed())

	g.Expect(v.ListMetrics.maxNodeCnt).To(BeNumerically(">", 2))
	g.Expect(v.ListMetrics.avgNodeCnt).To(BeNumerically(">", 2))
}

func TestAnalyzeListKeyReturnsErrorForInvalidPositiveListMaxListpackSize(t *testing.T) {
	g := NewWithT(t)
	v, client := setupListTestNode(t)
	v.Config[listMaxListpackSize] = "invalid"
	rpushTestList(t, client, "list:1", "a")

	g.Expect(v.analyzeListKey("list:1")).To(HaveOccurred())
}

func TestGetListDatatypeAnalysisPopulatesStructWithoutPrinting(t *testing.T) {
	initTestFlags(t)
	g := NewWithT(t)
	v := makeValkeyNode("node-1")
	v.Config = map[string]string{listMaxListpackSize: "-2"}
	setTestFlag(t, "print-output", "false")

	var analysis Analysis
	output := captureStdout(t, func() {
		v.getListDatatypeAnalysis(&analysis)
	})

	g.Expect(output).To(BeEmpty())
	g.Expect(analysis.Address).To(Equal("node-1"))
	g.Expect(analysis.Config[listMaxListpackSize]).To(Equal("-2"))
	listMetrics := analysis.Metrics[listDt].(map[string]any)
	g.Expect(listMetrics[kObjCnt]).To(Equal(int64(0)))
	g.Expect(listMetrics[kDistribution]).To(HaveLen(10))
}
