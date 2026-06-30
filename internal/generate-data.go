package main

import (
	"context"
	"fmt"

	"github.com/go-faker/faker/v4"
	"github.com/valkey-io/valkey-go"
)

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

func main() {
	co := valkey.ClientOption{
		InitAddress: []string{"localhost:30001"},
		// Username: "default",
		// Password: "",
	}
	client, err := valkey.NewClient(co)
	if err != nil {
		panic(err)
	}
	generateTestData(client, 10000)
}
