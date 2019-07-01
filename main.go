package main

import (
	"context"
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

	kafka_streamer.Setup(ctx)
	kafka_streamer.StartConsumer(ctx)

	// cleanup on OS signals (Ctrl + C)
	signal.Notify(osChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-osChan
	cancel()
	twitter.TearDown()
}
