package kafka_streamer

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaConfig struct {
	broker            string
	groupId           string
	topic             string
	replicationFactor int
	partitions        int
}

var (
	requiredConfig = []string{"BROKER", "GROUP_ID", "TOPIC", "REPLICATION_FACTOR", "PARTITIONS"}
	producer       *kafka.Producer
	config         *kafkaConfig
)

func createKafkaTopic(ctx context.Context) {
	var (
		admin       *kafka.AdminClient
		err         error
		maxDuration time.Duration
		results     []kafka.TopicResult
	)
	admin, err = kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": config.broker,
	})
	if err != nil {
		log.Panicln("Unable to create kafka admin client", err.Error())
	}
	maxDuration, err = time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	results, err = admin.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             config.topic,
			NumPartitions:     config.partitions,
			ReplicationFactor: config.replicationFactor,
		}},
		kafka.SetAdminOperationTimeout(maxDuration))
	if err != nil {
		log.Panic("Failed to create topic", err.Error())
	}
	for _, result := range results {
		fmt.Println("Topic created: ", result)
	}
	admin.Close()
}

func checkConfigEnv(ctx context.Context) {
	var found bool
	for _, config := range requiredConfig {
		_, found = os.LookupEnv(config)
	}
	if !found {
		log.Panic("Compulsory env variables missing. Please export BROKER, GROUP_ID, TOPIC")
	}
}

func Setup(ctx context.Context) {
	checkConfigEnv(ctx)

	rFactor, _ := strconv.Atoi(os.Getenv("REPLICATION_FACTOR"))
	partitions, _ := strconv.Atoi(os.Getenv("PARTITIONS"))

	config = &kafkaConfig{
		broker:            os.Getenv("BROKER"),
		groupId:           os.Getenv("GROUP_ID"),
		topic:             os.Getenv("TOPIC"),
		replicationFactor: rFactor,
		partitions:        partitions,
	}
	createKafkaTopic(ctx)
}

// createConsumer creates a new kafka consumer and subscribes to the required topics
func createConsumer() *kafka.Consumer {
	var (
		err error
		c   *kafka.Consumer
	)

	c, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               config.broker,
		"group.id":                        config.groupId,
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

	err = c.SubscribeTopics([]string{config.topic}, nil)
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
				log.Fatal("kafka-consumer error", e.Error())
			}
		}
	}
}

func Publish(ctx context.Context, value []byte) {
	var (
		err     error
		event   kafka.Event
		message *kafka.Message
	)
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &config.topic, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          value,
	}, producer.Events())
	if err != nil {
		log.Panicln("error publishing to kafka", err.Error())
	}
	event = <-producer.Events()
	message = event.(*kafka.Message)

	if message.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", message.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *message.TopicPartition.Topic, message.TopicPartition.Partition, message.TopicPartition.Offset)
	}
	close(producer.Events())
}

func StartProducer(ctx context.Context) {
	var (
		err error
	)
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.broker,
	})
	if err != nil {
		log.Panicln("error creating a new kafka producer", err.Error())
	}
	log.Println("kafka producer created")
}
