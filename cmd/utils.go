package main

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"

	"github.com/valkey-io/valkey-go"
)

func (v *ValkeyNode) getNodeConfig() error {
	ctx := context.Background()
	client := v.getClient()
	config, err := client.Do(ctx,
		client.B().ConfigGet().Parameter("*").Build(),
	).AsStrMap()
	if err != nil {
		return err
	}
	v.Config = make(map[string]string)
	maps.Copy(v.Config, config)
	return nil
}

func (v *ValkeyNode) getCommandStats() (map[string]int, error) {
	ctx := context.Background()
	client := v.getClient()
	info, err := client.Do(ctx,
		client.B().Arbitrary("INFO", "COMMANDSTATS").Build(),
	).ToString()
	if err != nil {
		return nil, err
	}

	cmdstats := make(map[string]int)
	re := regexp.MustCompile(`cmdstat_(?P<cmd>[\w\|]+):calls=(?P<count>\d+),.+`)
	for l := range strings.FieldsSeq(info) {
		match := re.FindStringSubmatch(l)
		if len(match) == 0 {
			continue
		}
		cmdstats[match[1]], _ = strconv.Atoi(match[2])

	}
	return cmdstats, nil
}

func (v *ValkeyNode) getUptime() (int, error) {
	ctx := context.Background()
	client := v.getClient()
	info, err := client.Do(ctx,
		client.B().Arbitrary("INFO", "server").Build(),
	).ToString()
	if err != nil {
		return -1, err
	}
	for l := range strings.FieldsSeq(info) {
		if strings.Contains(l, "uptime_in_seconds") {
			uptime, err := strconv.Atoi(strings.Split(l, ":")[1])
			if err != nil {
				return -1, err
			}
			return uptime, nil
		}
	}
	return -1, nil
}

func (v *ValkeyNode) printCommandStats() error {
	uptime, err := v.getUptime()
	if err != nil {
		return err
	}
	cmdstats, err := v.getCommandStats()
	if err != nil {
		return err
	}

	for key, value := range cmdstats {
		fmt.Printf("'%s' total execution: %d, op/s:%d\n", key, value, value/uptime)
	}
	return nil
}

func (v *ValkeyNode) getObjectEncoding(key string) string {
	ctx := context.Background()
	client := v.getClient()
	output, err := client.Do(ctx,
		client.B().ObjectEncoding().Key(key).Build(),
	).ToString()
	if err != nil {
		return ""
	}
	return output
}

func scan(client valkey.Client, dtype, keyPattern string, cursor uint64) (valkey.ScanEntry, error) {
	scanCmd := client.B().Scan().Cursor(cursor)
	if keyPattern != "" {
		scanCmd.Match(keyPattern)
	}
	scanCmd.Type(dtype)
	resp := client.Do(context.Background(), scanCmd.Build())
	return resp.AsScanEntry()
}
