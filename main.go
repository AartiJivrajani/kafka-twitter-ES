package main

import (
	"context"
	"kafka-twitter-ES/twitter"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var (
		osChan = make(chan os.Signal)
	)
	ctx := context.Background()
	twitter.StartStreamingTweets(ctx)

	// cleanup on OS signals (Ctrl + C_
	signal.Notify(osChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	<-osChan
	twitter.TearDown()
}
