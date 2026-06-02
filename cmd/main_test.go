package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

type Item struct {
	Name        string
	Description string
	Price       int
}

func initTestFlags(t *testing.T) {
	t.Helper()

	oldCommandLine := flag.CommandLine
	oldBootstrapAddress := bootstrapAddress
	oldBootstrapUsername := bootstrapUsername
	oldBootstrapPassword := bootstrapPassword
	oldKeyPattern := hashKeyPattern
	oldListKeyPattern := listKeyPattern
	oldFieldPattern := fieldPattern
	oldFieldPatternRE := fieldPatternRE
	oldPrintOutput := printOutput
	oldFlagsInitialized := flagsInitialized

	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	flagsInitialized = nil
	fieldPatternRE = nil
	initFlags()

	t.Cleanup(func() {
		flag.CommandLine = oldCommandLine
		bootstrapAddress = oldBootstrapAddress
		bootstrapUsername = oldBootstrapUsername
		bootstrapPassword = oldBootstrapPassword
		hashKeyPattern = oldKeyPattern
		listKeyPattern = oldListKeyPattern
		fieldPattern = oldFieldPattern
		fieldPatternRE = oldFieldPatternRE
		printOutput = oldPrintOutput
		flagsInitialized = oldFlagsInitialized
	})
}

func setTestFlag(t *testing.T, name string, value string) {
	t.Helper()
	if err := flag.Set(name, value); err != nil {
		t.Fatalf("set flag %q: %v", name, err)
	}
}

func cleanupValkeyInstance(address string, client valkey.Client) {
	if client != nil {
		_ = client.Do(context.Background(), client.B().Shutdown().Build()).Error()
		client.Close()
	}
	if address != "" {
		dir := filepath.Dir(address)
		if strings.HasPrefix(filepath.Base(dir), "valkey-test-") {
			_ = os.RemoveAll(dir)
			return
		}
		_ = os.Remove(address)
	}
}

// create a Valkey test instance, only allow connections by socket
// return the path to the socket
func createValkeyInstance() string {
	tmpDir, err := os.MkdirTemp("", "valkey-test-*")
	if err != nil {
		panic(err)
	}
	addr := filepath.Join(tmpDir, "valkey.sock")
	logPath := filepath.Join(tmpDir, "valkey.log")
	cmd := exec.Command(
		"valkey-server",
		"--port", "0",
		"--unixsocket", addr,
		"--unixsocketperm", "700",
		"--daemonize", "yes",
		"--save", "",
		"--appendonly", "no",
		"--dir", tmpDir,
		"--logfile", logPath,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		panic(fmt.Sprintf("start valkey-server: %v\n%s", err, string(output)))
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(addr); err == nil {
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	logOutput, _ := os.ReadFile(logPath)
	_ = os.RemoveAll(tmpDir)
	panic(fmt.Sprintf("valkey-server did not create socket %s within 5s\n%s", addr, string(logOutput)))
}

func generateTestData(client valkey.Client, entriesCount int) {
	ctx := context.Background()

	client.Do(ctx, client.B().Flushdb().Build())
	for i := range entriesCount {
		var dsc string
		if v, _ := faker.RandomInt(1, 10); v[0] > 5 {
			dsc = faker.Paragraph()
		} else {
			dsc = faker.Sentence()
		}
		cmd := client.B().Hset().Key(fmt.Sprintf("item:%d", i)).
			FieldValue().FieldValue("name", faker.Word()).
			FieldValue("description", dsc).
			Build()
		client.Do(ctx, cmd)
	}

}

func TestAnalyzeNode(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		g := NewWithT(t)
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
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
func TestAnalyzeCluster(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance()
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	}) {
		return
	}
	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		setTestFlag(t, "print-output", "false")
		parseArguments()

		cs := analyzeCluster(makeValkeyNode(address))
		g.Expect(cs.HashMetrics.objCount).To(Equal(hashKeysCount))

	})
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})

}

func TestScanCluster(t *testing.T) {
	initTestFlags(t)

	totalKeys := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance()
		client = createClient(address)
		generateTestData(client, totalKeys)
	}) {
		return
	}
	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		nodes := getClusterNodes(ValkeyNode{
			Address: address,
		})
		dbSizeKeys := 0
		for _, n := range nodes {
			nClient := createClient(n.Address)
			ctx := context.Background()
			dbsize, err := nClient.Do(ctx, nClient.B().Dbsize().Build()).AsInt64()
			g.Expect(err).To(BeNil())
			dbSizeKeys += int(dbsize)
			nClient.Close()

		}
		g.Expect(totalKeys).To(Equal(dbSizeKeys))
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
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
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
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
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
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
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
		g.Expect(v.HashMetrics.fieldCount).To(Equal(hashKeysCount))
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
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
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
