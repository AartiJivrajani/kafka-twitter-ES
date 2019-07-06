package common

import (
	"context"
	"log"
	"os"
)

var (
	configList = map[string][]string{
		"kafka":   {"BROKER", "GROUP_ID", "TOPIC", "REPLICATION_FACTOR", "PARTITIONS"},
		"es":      {"ES_URL", "ES_ACCESS_KEY", "ES_ACCESS_SECRET"},
		"twitter": {"TWITTER_CONSUMER_KEY", "TWITTER_CONSUMER_SECRET", "TWITTER_ACCESS_TOKEN", "TWITTER_ACCESS_SECRET"},
	}
)

func CheckConfigEnv(ctx context.Context, infra string) {
	var found bool
	for _, config := range configList[infra] {
		_, found = os.LookupEnv(config)
	}
	if !found {
		log.Panicf("Compulsory env variables missing. Please export: %v", configList[infra])
		ctx.Done()
	}
}
