package es

import (
	"context"
	"kafka-twitter-ES/common"
	"log"
	"os"

	"gopkg.in/olivere/elastic.v6"
)

var (
	esClient *elastic.Client
	esConf   *esConfig
)

type esConfig struct {
	url    string
	key    string
	secret string
}

func createIndex(ctx context.Context) {
	var (
		err    error
		exists bool
	)
	// check if the index exists
	exists, err = esClient.IndexExists("twitter").Do(ctx)
	if err != nil {
		log.Panicln("failed to check if index exists", err.Error())
	}
	if exists {
		log.Println("create_index: index already exists, nothing to do")
		return
	}
	_, err = esClient.CreateIndex("twitter").Do(ctx)
	if err != nil {
		log.Panicln("error creating ES index", err.Error())
	}
}

func Setup(ctx context.Context) {
	var (
		err error
	)
	common.CheckConfigEnv(ctx, "es")
	esConf = &esConfig{
		url:    os.Getenv("ES_URL"),
		key:    os.Getenv("ES_ACCESS_KEY"),
		secret: os.Getenv("ES_ACCESS_SECRET"),
	}

	esClient, err = elastic.NewClient(elastic.SetURL(esConf.url),
		elastic.SetScheme("https"),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.Panic("error connecting to ES", err.Error())
	} else {
		log.Printf("connected to ES: %v", esClient.String())
	}
	createIndex(ctx)
}
