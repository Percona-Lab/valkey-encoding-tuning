package main

import (
	"cmp"
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

func (v *ValkeyNode) scan(dtype string, cursor uint64) (valkey.ScanEntry, error) {
	var filter *string
	switch dtype {
	case hashDt:
		filter = &v.opts().HashKeyPattern
	case listDt:
		filter = &v.opts().ListKeyPattern
	case setDt:
		filter = &v.opts().SetKeyPattern
	case zsetDt:
		filter = &v.opts().ZSetKeyPattern
	default:
		return valkey.ScanEntry{}, fmt.Errorf("invalid datatype: %s", dtype)
	}

	scanCmd := v.getClient().B().Scan().Cursor(cursor)
	if *filter != "" {
		scanCmd.Match(*filter)
	}
	scanCmd.Type(dtype)
	resp := v.getClient().Do(context.Background(), scanCmd.Build())
	return resp.AsScanEntry()
}

func (v *ValkeyNode) analyze(db int64, dtype string, countKeys func(int), analyzeKey func(string) error) error {
	var cursor uint64
	// A dedicated client is needed because SELECT state is connection-specific and
	// cannot be safely changed with a standalone command on a multiplexed client.
	v.Close()
	v.Client = createClientWithDatabase(v.Address, v.opts(), db)
	for ok := true; ok; ok = (cursor != 0) {
		entry, err := v.scan(dtype, cursor)
		if err != nil {
			return err
		}
		if countKeys != nil {
			countKeys(len(entry.Elements))
		}
		for _, key := range entry.Elements {
			if err := analyzeKey(key); err != nil {
				return fmt.Errorf("analyze %s key %q: %w", dtype, key, err)
			}
		}
		cursor = entry.Cursor
	}
	return nil
}

func max[T cmp.Ordered](x, y T) T {
	if x > y {
		return x
	}
	return y
}
