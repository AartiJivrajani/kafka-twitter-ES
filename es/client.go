package es

import (
	"context"
    "kafka-twitter-ES/common"

    "gopkg.in/olivere/elastic.v6"
)

var (
	esClient *elastic.Client
)

type esConfig struct {

}

func Setup(ctx context.Context) {
    common.CheckConfigEnv(ctx, "es")

}
