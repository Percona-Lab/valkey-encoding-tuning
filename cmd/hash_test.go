package main

import (
	"testing"

	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

func TestAnalyzeNode(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		g := NewWithT(t)
		address = createValkeyInstance(true)
		g.Eventually(address).To(BeAnExistingFile())
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	}) {
		return
	}

	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		v := makeValkeyNode(address)
		setTestFlag(t, "print-output", "false")
		parseArguments()

		g.Expect(v.getNodeConfig()).To(Succeed())
		g.Expect(v.analyzeHash()).To(Succeed())
		g.Expect(v.HashMetrics.objCount).To(Equal(hashKeysCount))
	})
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})
}

func TestAnalyzeWithKeyFilterMatchedPattern(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		g := NewWithT(t)
		address = createValkeyInstance(true)
		g.Eventually(address).To(BeAnExistingFile())
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	}) {
		return
	}

	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		v := makeValkeyNode(address)
		setTestFlag(t, "hash-key-pattern", "item*")
		setTestFlag(t, "print-output", "false")
		parseArguments()
		g.Expect(v.getNodeConfig()).To(Succeed())
		g.Expect(v.analyzeHash()).To(Succeed())
		g.Expect(v.HashMetrics.objCount).To(Equal(hashKeysCount))
	})
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})
}

func TestAnalyzeWithKeyFilterNotMatchingPattern(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		g := NewWithT(t)
		address = createValkeyInstance(true)
		g.Eventually(address).To(BeAnExistingFile())
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	}) {
		return
	}
	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		v := makeValkeyNode(address)
		setTestFlag(t, "print-output", "false")
		setTestFlag(t, "hash-key-pattern", "item-not-exists*")
		parseArguments()
		g.Expect((v.HashMetrics.objCount)).To(Equal(0))

		g.Expect(v.analyzeHash()).To(Succeed())
		g.Expect((v.HashMetrics.objCount)).To(Equal(0))
	})
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})
}

func TestAnalyzeWithFieldFilterMatchedPattern(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		g := NewWithT(t)
		address = createValkeyInstance(true)
		g.Eventually(address).To(BeAnExistingFile())
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	}) {
		return
	}
	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		v := makeValkeyNode(address)
		setTestFlag(t, "field-pattern", "nam.+")
		setTestFlag(t, "print-output", "false")
		parseArguments()
		g.Expect(v.getNodeConfig()).To(Succeed())
		g.Expect(v.analyzeHash()).To(Succeed())
		g.Expect(v.HashMetrics.fieldCount).To(Equal(hashKeysCount * 2))
		g.Expect(v.HashMetrics.maxField).To(ContainSubstring(".name"))
	})
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})
}

func TestAnalyzeWithFieldNotMatchingFilter(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		g := NewWithT(t)
		address = createValkeyInstance(true)
		g.Eventually(address).To(BeAnExistingFile())
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	}) {
		return
	}
	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		v := makeValkeyNode(address)
		setTestFlag(t, "field-pattern", "namo.+")
		setTestFlag(t, "print-output", "false")
		parseArguments()
		g.Expect(v.getNodeConfig()).To(Succeed())
		g.Expect(v.analyzeHash()).To(Succeed())
		g.Expect(v.HashMetrics.fieldCount).To(Equal(0))
		g.Expect(v.HashMetrics.maxField).To(BeEmpty())
	})
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})
}
