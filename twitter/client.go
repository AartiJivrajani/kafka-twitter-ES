package twitter

import (
	"context"
	"fmt"
	"log"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

const (
	consumerKey    = "Te2MirNRTWm1oU8wk3DuKyUfq"
	consumerSecret = "ZuMM2P88CXX1GUg3CaWwmvbt2u0ExWz0ksGFwvzdUEfIV2sX7I"
	accessToken    = "526009322-LMa3YXrBGkoreplnakx1kEZMqSHwg3t7VJMfEen7"
	accessSecret   = "NvlhggmDhlXEAMq0sz0MG7C9FwvyTbRj63QhkJPkvXQd4"

	// stream the tweets for this keyword
	keyWord = "bitcoin"
)

var (
	streamChan = make(chan *twitter.Tweet)
	stream     *twitter.Stream
)

func createStreamingClient() *twitter.Client {
	config := oauth1.NewConfig(consumerKey, consumerSecret)
	token := oauth1.NewToken(accessToken, accessSecret)
	httpClient := config.Client(oauth1.NoContext, token)
	streamingClient := twitter.NewClient(httpClient)
	fmt.Println("created streaming client")

	// Verify Credentials
	verifyParams := &twitter.AccountVerifyParams{
		SkipStatus:   twitter.Bool(true),
		IncludeEmail: twitter.Bool(true),
	}

	// we can retrieve the user and verify if the credentials
	// we have used successfully allow us to log in!
	user, _, err := streamingClient.Accounts.VerifyCredentials(verifyParams)
	if err != nil {
		fmt.Println(err)
		return nil
	}

	log.Printf("User's ACCOUNT:\n%+v\n", user)
	return streamingClient
}

func processTweets(ctx context.Context) {
	var (
		tweet *twitter.Tweet
	)
	fmt.Println("Gonna start processing the tweets")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Stopping tweet processing")
			return
		case tweet = <-streamChan:
			log.Println("Tweet! ", tweet.Text)
		}
	}
}

// StartStreamingTweets creates a streaming client and starts listening to tweets based
// on a filter. Multiple types of filters can be set in this case
func StartStreamingTweets(ctx context.Context) {
	fmt.Println("starting the streaming...")
	var (
		err error

		client *twitter.Client
	)
	go processTweets(ctx)

	client = createStreamingClient()
	demux := twitter.NewSwitchDemux()

	filterParams := &twitter.StreamFilterParams{
		Track:         []string{keyWord},
		StallWarnings: twitter.Bool(true),
	}
	stream, err = client.Streams.Filter(filterParams)

	if err != nil {
		log.Fatal("Error connecting to the twitter streams", err.Error())
	}
	demux.Tweet = func(tweet *twitter.Tweet) {
		streamChan <- tweet
	}
	go demux.HandleChan(stream.Messages)

}

func TearDown() {
	fmt.Println("Stopping stream")
	stream.Stop()
}
