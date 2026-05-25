package main

import (
	"context"
	"regexp"
	"strconv"
	"strings"
)

func (v *ValkeyNode) getNodeConfig() error {
	ctx := context.Background()
	client := v.getClient()
	config, err := client.Do(ctx,
		client.B().ConfigGet().Parameter(listpackMaxConfig).Build(),
	).AsStrMap()
	if err != nil {
		return err
	}
	v.maxListPackSize, err = strconv.Atoi(config[listpackMaxConfig])
	if err != nil {
		return err
	}
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
	client := createClient((v.Address))
	defer client.Close()
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
