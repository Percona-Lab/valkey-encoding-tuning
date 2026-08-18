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

const (
	defaultPassword = "hello-world"
)

func initTestFlags(t *testing.T) {
	t.Helper()

	oldCommandLine := flag.CommandLine
	oldOptions := options
	oldFlagsInitialized := flagsInitialized

	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	flagsInitialized = nil
	initFlags()

	t.Cleanup(func() {
		flag.CommandLine = oldCommandLine
		options = oldOptions
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
func createValkeyInstance(setPassword bool) string {
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
	if setPassword {
		cmd.Args = append(cmd.Args, "--requirepass", defaultPassword)
	}
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

func generateTestData(t *testing.T, address string, db int64, entriesCount int) {
	t.Helper()

	ctx := context.Background()
	client := createClientWithDatabase(address, &options, db)
	t.Cleanup(client.Close)
	if err := client.Do(ctx, client.B().Flushdb().Build()).Error(); err != nil {
		t.Fatalf("flush database %d: %v", db, err)
	}

	for i := range entriesCount {
		var dsc string
		if v, _ := faker.RandomInt(1, 10); v[0] > 5 {
			dsc = faker.Paragraph()
		} else {
			dsc = faker.Sentence()
		}
		cmd := client.B().Hset().Key(fmt.Sprintf("{db%d}:item:%d", db, i)).
			FieldValue().FieldValue("name", faker.Word()).
			FieldValue("description", dsc).
			Build()
		if err := client.Do(ctx, cmd).Error(); err != nil {
			t.Fatalf("populate database %d: %v", db, err)
		}
	}
}

func TestAnalyzeCluster(t *testing.T) {
	initTestFlags(t)

	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance(true)
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		generateTestData(t, address, 0, hashKeysCount)
	}) {
		return
	}
	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		setTestFlag(t, "database", "0")
		setTestFlag(t, "print-output", "false")
		parseArguments()

		summaries := analyzeCluster(makeValkeyNode(address))
		g.Expect(summaries[0].HashMetrics.objCnt).To(Equal(hashKeysCount))

	})
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})

}

func TestAnalyzeClusterMultipleDatabases(t *testing.T) {
	initTestFlags(t)

	keyCounts := []int{7, 11, 13}
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance(true)
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		for db, keyCount := range keyCounts {
			generateTestData(t, address, int64(db), keyCount)
		}
	}) {
		return
	}
	t.Cleanup(func() {
		cleanupValkeyInstance(address, client)
	})

	t.Run("analyzes each database independently", func(t *testing.T) {
		g := NewWithT(t)
		setTestFlag(t, "database", "0,1,2")
		setTestFlag(t, "print-output", "false")
		parseArguments()

		results, err := analyzeClusterData(makeValkeyNode(address))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(results).To(HaveLen(len(keyCounts)))
		for db, keyCount := range keyCounts {
			g.Expect(results[db].Database).To(Equal(int64(db)))
			g.Expect(results[db].Summary.HashMetrics.objCnt).To(Equal(keyCount))
			g.Expect(results[db].Summary.HashMetrics.fieldStats.maxItem).
				To(HavePrefix(fmt.Sprintf("{db%d}", db)))
		}
	})
}

func TestScanCluster(t *testing.T) {
	initTestFlags(t)

	totalKeys := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		address = createValkeyInstance(true)
		setTestFlag(t, "username", "default")
		setTestFlag(t, "password", defaultPassword)
		client = createClient(address)
		generateTestData(t, address, 0, totalKeys)
	}) {
		return
	}
	t.Run("test", func(t *testing.T) {
		g := NewWithT(t)
		nodes, err := getClusterNodes(ValkeyNode{
			Address: address,
		})
		g.Expect(err).To(BeNil())
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
