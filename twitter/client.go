package twitter

import (
	"context"
	"fmt"
	"kafka-twitter-ES/common"
	"log"
	"os"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

var (
	consumerKey, consumerSecret, accessToken, accessSecret string
	streamChan                                             = make(chan *twitter.Tweet)
	stream                                                 *twitter.Stream
)

func Setup(ctx context.Context) {
	consumerKey = os.Getenv("CONSUMER_KEY")
	consumerSecret = os.Getenv("CONSUMER_SECRET")
	accessToken = os.Getenv("ACCESS_TOKEN")
	accessSecret = os.Getenv("ACCESS_SECRET")

	if consumerKey == "" || consumerSecret == "" || accessSecret == "" || accessToken == "" {
		log.Println("Compulsory env variables missing. Please export CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET")
		ctx.Done()
	}
}

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
		Track:         []string{common.KeyWord},
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
