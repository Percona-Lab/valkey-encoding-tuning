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
	generateTestData(client, hashKeysCount)

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
	g.Expect(v.analyzeHash()).To(Succeed())
	g.Expect(v.HashMetrics.objCnt).To(Equal(hashKeysCount))
}

func TestAnalyzeWithKeyFilterMatchedPattern(t *testing.T) {
	hashKeysCount := 1000
	g := NewWithT(t)
	v := setupHashTestNode(t, hashKeysCount)

	setTestFlag(t, "hash-key-pattern", "item*")
	setTestFlag(t, "print-output", "false")
	parseArguments()

	g.Expect(v.getNodeConfig()).To(Succeed())
	g.Expect(v.analyzeHash()).To(Succeed())
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

	g.Expect(v.analyzeHash()).To(Succeed())
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
	g.Expect(v.analyzeHash()).To(Succeed())
	g.Expect(v.HashMetrics.fieldStats.count).To(Equal(hashKeysCount * 2))
	g.Expect(v.HashMetrics.fieldStats.maxItem).To(ContainSubstring(".name"))
}

func TestAnalyzeWithFieldNotMatchingFilter(t *testing.T) {
	hashKeysCount := 1000
	g := NewWithT(t)
	v := setupHashTestNode(t, hashKeysCount)

	setTestFlag(t, "field-pattern", "namo.+")
	setTestFlag(t, "print-output", "false")
	parseArguments()
	g.Expect(v.getNodeConfig()).To(Succeed())
	g.Expect(v.analyzeHash()).To(Succeed())
	g.Expect(v.HashMetrics.fieldStats.count).To(Equal(0))
	g.Expect(v.HashMetrics.fieldStats.maxItem).To(BeEmpty())
}
