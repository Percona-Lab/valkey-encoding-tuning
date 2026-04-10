package main

import (
	"context"
	"fmt"

	"github.com/valkey-io/valkey-go"
)

func getNodeConfig(client valkey.Client) map[string]string {
	ctx := context.Background()
	config, err := client.Do(ctx, client.B().ConfigGet().Parameter("*").Build()).AsStrMap()
	if err == nil {
		return config
	}
	return nil
}

func getMaxField(client valkey.Client, hash string) (string, int) {
	ctx := context.Background()
	var maxField string
	var maxFieldSize int
	var cursor uint64
	for {
		resp := client.Do(ctx, client.B().Hscan().Key(hash).Cursor(cursor).Build())
		entry, err := resp.AsScanEntry()
		if err != nil {
			return "", 0
		}
		for i := 0; i < len(entry.Elements); i += 2 {
			// fmt.Printf("%s=%s\n", entry.Elements[i], entry.Elements[i+1])
			if maxField == "" {
				maxField = entry.Elements[i]
				maxFieldSize = len(entry.Elements[i+1])
			} else {
				if maxFieldSize < len(entry.Elements[i+1]) {
					maxField = entry.Elements[i]
					maxFieldSize = len(entry.Elements[i+1])
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
	return fmt.Sprintf("%s.%s", hash, maxField), int(maxFieldSize)
}

func findHashKeys(client valkey.Client) ([]string, error) {
	ctx := context.Background()
	var output []string
	var cursor uint64
	var hashCount uint64
	var hashtableObjCount uint64
	var maxField string
	var maxFieldSize int
	for {
		resp := client.Do(
			ctx,
			client.B().Scan().Cursor(cursor).Type("hash").Build(),
		)
		entry, err := resp.AsScanEntry()
		if err != nil {
			return nil, err
		}
		for _, key := range entry.Elements {
			hashCount++
			if typeCheck, _ := client.Do(ctx, client.B().ObjectEncoding().Key(key).Build()).ToString(); typeCheck == "hashtable" {
				hashtableObjCount++
				output = append(output, key)
				// scan for maxfield
				if maxField == "" {
					maxField, maxFieldSize = getMaxField(client, key)
				} else {
					cMaxField, cMaxFieldSize := getMaxField(client, key)
					if cMaxFieldSize > maxFieldSize {
						maxField, maxFieldSize = cMaxField, cMaxFieldSize
					}
				}
			}
		}
		cursor = entry.Cursor
		if cursor == 0 {
			break
		}
	}
	fmt.Printf("hashtable keys found: %d/%d (%.2f%% of all hash keys)\n", hashtableObjCount, hashCount, (float64(hashtableObjCount) / float64(hashCount) * 100))
	fmt.Printf("largest hash field: %s, size:%d \n", maxField, maxFieldSize)
	return output, nil
}

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	// ctx := context.Background()
	getNodeConfig(client)
	findHashKeys(client)
}
