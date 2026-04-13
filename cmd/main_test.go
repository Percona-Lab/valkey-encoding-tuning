package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/valkey-io/valkey-go"
)

type Item struct {
	Name        string
	Description string
	Price       int
}

func TestAnalyzeCluster(t *testing.T) {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:30006"},
	})
	if err != nil {
		panic(err)
	}
	for i := range 1000 {
		cmd := client.B().Hset().Key(fmt.Sprintf("item:%d", i)).
			FieldValue().FieldValue("name", faker.Word()).
			FieldValue("description", faker.Paragraph()).
			Build()
		client.Do(ctx, cmd)
	}
	analyzeCluster(ValkeyNode{
		Address: "127.0.0.1:30006",
	})

}
