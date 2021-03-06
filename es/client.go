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
	ESChan   = make(chan []byte)
)

type esConfig struct {
	url    string
	key    string
	secret string
}

func esInsert(ctx context.Context, msg []byte) {
	var (
		err error
	)
	_, err = esClient.Index().Index(common.ESIndex).Type(common.ESType).BodyString(string(msg)).Do(ctx)
	if err != nil {
		if e, OK := err.(*elastic.Error); OK {
			log.Printf("error posting to ES.\nDetails: %v\nDoc:%v\n", e.Details, string(msg))
			return
		}
		log.Printf("error posting to ES.\nError: %v\nDoc:%v\n", err.Error(), string(msg))
		return
	}
	log.Println("posted data to ES")
	return
}

func startIngestWorker(ctx context.Context) {
	var (
		msg []byte
	)
	for {
		select {
		case msg = <-ESChan:
			esInsert(ctx, msg)
		}
	}
}

func createIndex(ctx context.Context) {
	var (
		err    error
		exists bool
	)
	// check if the index exists
	exists, err = esClient.IndexExists(common.ESIndex).Do(ctx)
	if err != nil {
		log.Panicln("failed to check if index exists", err.Error())
	}
	if exists {
		log.Println("create_index: index already exists, nothing to do")
		return
	}
	_, err = esClient.CreateIndex(common.ESIndex).BodyString(common.ESMapping).Do(ctx)
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

	go startIngestWorker(ctx)
}
