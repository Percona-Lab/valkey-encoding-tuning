package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/caio/go-tdigest/v5"
	"github.com/go-faker/faker/v4"
	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

type Item struct {
	Name        string
	Description string
	Price       int
}

// create a Valkey test instance, only allow connections by socket
// return the path to the socket
func createValkeyInstance() string {
	tmpDir := os.TempDir()
	os.MkdirAll(tmpDir, 0755)
	addr := filepath.Join(tmpDir, "valkey.sock")
	cmd := exec.Command("valkey-server", "--unixsocket", addr, "--daemonize yes")
	_, err := cmd.CombinedOutput()
	if err != nil {
		panic(err)
	}
	return addr
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
	g := NewWithT(t)
	hashKeysCount := 1000
	var address string
	var client valkey.Client
	t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	})

	t.Run("test", func(t *testing.T) {
		td, _ := tdigest.New()
		v := ValkeyNode{
			Address: address,
			metrics: ValkeyNodeMetrics{tdigest: td},
		}
		initFlags()
		flag.Set("print-output", "false")
		flag.Parse()

		v.getNodeConfig()
		v.analyze()
		g.Expect(v.metrics.hashObjCount).To(Equal(hashKeysCount))
	})
	t.Cleanup(func() {
		client.Do(context.Background(), client.B().Shutdown().Build())
		os.Remove(address)
		client.Close()
	})

}
func TestAnalyzeCluster(t *testing.T) {
	g := NewWithT(t)
	hashKeysCount := 1000
	address := "127.0.0.1:30005"
	var client valkey.Client
	t.Run("setup env", func(t *testing.T) {
		// address = createValkeyInstance()
		// g.Eventually(address).To(BeAnExistingFile())
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	})
	t.Run("test", func(t *testing.T) {
		initFlags()
		flag.Set("print-output", "false")
		flag.Parse()

		cs := analyzeCluster(ValkeyNode{
			Address: address,
		})
		g.Expect(cs.metrics.hashObjCount).To(Equal(hashKeysCount))

	})
	t.Cleanup(func() {
		// client.Do(context.Background(), client.B().Shutdown().Build())
		// os.Remove(address)
		client.Close()
	})

}

func TestScanCluster(t *testing.T) {
	g := NewWithT(t)
	address := "127.0.0.1:30005"
	nodes := getClusterNodes(ValkeyNode{
		Address: address,
	})
	totalKeys := 1000
	dbSizeKeys := 0
	for _, n := range nodes {
		nClient, err := valkey.NewClient(valkey.ClientOption{
			InitAddress:       []string{n.Address},
			ForceSingleClient: true,
		})
		g.Expect(err).To(BeNil())
		ctx := context.Background()
		dbsize, err := nClient.Do(ctx, nClient.B().Dbsize().Build()).AsInt64()
		g.Expect(err).To(BeNil())
		dbSizeKeys += int(dbsize)
		nClient.Close()

	}
	g.Expect(totalKeys).To(Equal(dbSizeKeys))
}

func TestAnalyzeWithKeyFilterMatchedPattern(t *testing.T) {
	g := NewWithT(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	})

	t.Run("test", func(t *testing.T) {
		td, _ := tdigest.New()
		v := ValkeyNode{
			Address: address,
			metrics: ValkeyNodeMetrics{tdigest: td},
		}
		initFlags()
		flag.Set("key-pattern", "item*")
		flag.Set("print-output", "false")
		flag.Parse()
		v.getNodeConfig()
		v.analyze()
		g.Expect(v.metrics.hashObjCount).To(Equal(hashKeysCount))
		client.Do(context.Background(), client.B().Shutdown().Force().Build())
	})
	t.Cleanup(func() {
		client.Do(context.Background(), client.B().Shutdown().Build())
		os.Remove(address)
		client.Close()
	})
}

func TestAnalyzeWithKeyFilterNotMatchingPattern(t *testing.T) {
	g := NewWithT(t)
	hashKeysCount := 1000
	var address string
	var client valkey.Client
	var v ValkeyNode
	t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	})
	t.Run("test", func(t *testing.T) {
		td, _ := tdigest.New()
		v = ValkeyNode{
			Address: address,
			metrics: ValkeyNodeMetrics{tdigest: td},
		}
		initFlags()
		flag.Set("print-output", "false")
		flag.Set("key-pattern", "item-not-exists*")
		flag.Parse()
		g.Expect((v.metrics.hashObjCount)).To(Equal(0))

		v.analyze()
		g.Expect((v.metrics.hashObjCount)).To(Equal(0))
	})
	t.Cleanup(func() {
		client.Do(context.Background(), client.B().Shutdown().Build())
		os.Remove(address)
		client.Close()
	})

}

func TestAnalyzeWithFieldFilterMatchedPattern(t *testing.T) {
	g := NewWithT(t)
	hashKeysCount := 1000
	var address string
	var client valkey.Client
	t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	})
	t.Run("test", func(t *testing.T) {
		td, _ := tdigest.New()
		v := ValkeyNode{
			Address: address,
			metrics: ValkeyNodeMetrics{tdigest: td},
		}
		initFlags()
		flag.Set("field-pattern", "nam.+")
		flag.Set("print-output", "false")
		flag.Parse()
		parseArguments()
		v.getNodeConfig()
		v.analyze()
		g.Expect(v.metrics.hashFieldCount).To(Equal(hashKeysCount))
		g.Expect(v.metrics.maxField).To(ContainSubstring(".name"))
	})
	t.Cleanup(func() {
		client.Do(context.Background(), client.B().Shutdown().Build())
		os.Remove(address)
		client.Close()
	})

}

func TestAnalyzeWithFieldNotMatchingFilter(t *testing.T) {
	g := NewWithT(t)
	hashKeysCount := 1000
	var address string
	var client valkey.Client
	t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance()
		g.Eventually(address).To(BeAnExistingFile())
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	})
	t.Run("test", func(t *testing.T) {
		td, _ := tdigest.New()
		v := ValkeyNode{
			Address: address,
			metrics: ValkeyNodeMetrics{tdigest: td},
		}
		initFlags()
		flag.Set("field-pattern", "namo.+")
		flag.Set("print-output", "false")
		flag.Parse()
		parseArguments()
		v.getNodeConfig()
		v.analyze()
		g.Expect(v.metrics.hashFieldCount).To(Equal(0))
		g.Expect(v.metrics.maxField).To(BeEmpty())
	})
	t.Cleanup(func() {
		client.Do(context.Background(), client.B().Shutdown().Build())
		os.Remove(address)
		client.Close()
	})

}
