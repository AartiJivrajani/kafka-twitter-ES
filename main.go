package main

import (
	"context"
	"kafka-twitter-ES/es"
	"kafka-twitter-ES/kafka-streamer"
	"kafka-twitter-ES/twitter"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		osChan = make(chan os.Signal)
	)
	ctx, cancel := context.WithCancel(context.Background())
	twitter.Setup(ctx)
	twitter.StartStreamingTweets(ctx)

	// setup ES
	es.Setup(ctx)

	kafka_streamer.Setup(ctx)
	// start the kafka producer
	kafka_streamer.StartProducer(ctx)
	// start the kafka consumers
	kafka_streamer.StartConsumer(ctx)

	// cleanup on OS signals (Ctrl + C)
	signal.Notify(osChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-osChan
	cancel()
	twitter.TearDown()
}
