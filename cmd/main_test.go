package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-faker/faker/v4"
	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
)

type Item struct {
	Name        string
	Description string
	Price       int
}

func TestAnalyzeNode(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:6379"},
	})
	if err != nil {
		panic(err)
	}
	for i := range 1000 {
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

	v := ValkeyNode{
		Address: "127.0.0.1:6379",
	}
	v.getNodeConfig()
	v.analyze()
	g.Expect(v.metrics.hashObjCount).To(Equal(1000))
}
func TestAnalyzeCluster(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:30006"},
	})
	if err != nil {
		panic(err)
	}
	for i := range 1000 {
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
		resp := client.Do(ctx, cmd)
		g.Expect(resp.Error()).ToNot(HaveOccurred())
	}

	cs := analyzeCluster(ValkeyNode{
		Address: "127.0.0.1:30006",
	})
	g.Expect(cs.metrics.hashObjCount).To(Equal(1000))

}

func TestScanCluster(t *testing.T) {
	g := NewWithT(t)
	nodes := getClusterNodes(ValkeyNode{
		Address: "127.0.0.1:30006",
	})
	// g.Expect(len(nodes)).To(Equal(3))
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
		fmt.Println(n.Address)
		fmt.Println(dbsize)
		dbSizeKeys += int(dbsize)
		nClient.Close()

	}
	g.Expect(totalKeys).To(Equal(dbSizeKeys))
}
