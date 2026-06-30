package main

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

func TestGetServerInfo(t *testing.T) {
	hashKeysCount := 1000
	var address string
	var client valkey.Client
	if !t.Run("setup env", func(t *testing.T) {
		g := NewWithT(t)
		address = createValkeyInstance(false)
		g.Eventually(address).To(BeAnExistingFile())
		client = createClient(address)
		generateTestData(client, hashKeysCount)
	}) {
		return
	}
	v := makeValkeyNode(address)
	t.Run("test get commandstats", func(t *testing.T) {
		g := NewWithT(t)
		cmdstats, err := v.getCommandStats()
		g.Expect(err).To(BeNil())
		g.Expect(cmdstats["hset"]).To(Equal(hashKeysCount))
	})
	t.Run("test get server uptime", func(t *testing.T) {
		g := NewWithT(t)
		time.Sleep(1 * time.Second)
		uptime, err := v.getUptime()
		g.Expect(err).To(BeNil())
		g.Expect((uptime)).Should(BeNumerically(">", 0))
	})
}
