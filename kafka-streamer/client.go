package kafka_streamer

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	broker, groupId, topicsStr string
	topics                     []string
)

func Setup(ctx context.Context) {
	broker = os.Getenv("BROKER")
	groupId = os.Getenv("GROUP_ID")
	topicsStr = os.Getenv("TOPICS")

	if broker == "" || groupId == "" || topicsStr == "" {
		log.Print("Compulsory env variables missing. Please export BROKER, GROUP_ID, TOPICS(comma seperated)")
		ctx.Done()
	}
	topics = strings.Split(topicsStr, ",")
}

func createConsumer() *kafka.Consumer {
	var (
		err error
		c   *kafka.Consumer
	)

	c, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        groupId,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"auto.offset.reset":               "earliest",
	})
	if err != nil {
		log.Panic("Error starting Kafka consumer ", err.Error())
	}

	log.Println("Kafka Consumer created")

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		log.Panic("Error subscribing to the topics", err.Error())
	}
	return c
}

func StartConsumer(ctx context.Context) {
	var (
		consumer *kafka.Consumer
		err      error
	)
	consumer = createConsumer()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Received QUIT, stopping kafka consumers")
			err = consumer.Close()
			if err != nil {
				log.Println("Error closing the kafka conusmer")
			}
			return
		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}
}
