package kafka_streamer

import (
	"context"
	"fmt"
	"kafka-twitter-ES/common"
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
	producer *kafka.Producer
	config   *kafkaConfig
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

func Setup(ctx context.Context) {
	common.CheckConfigEnv(ctx, "kafka")

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
		"bootstrap.servers": config.broker,
		"group.id":          config.groupId,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Panic("Error starting Kafka consumer ", err.Error())
	}
	fmt.Println()
	err = c.Subscribe(config.topic, nil)
	if err != nil {
		log.Panic("Error subscribing to the topics", err.Error())
	}
	log.Println("Kafka Consumer created")
	return c
}

func StartConsumer(ctx context.Context) {
	var (
		consumer *kafka.Consumer
		msg      *kafka.Message
		err      error
	)
	consumer = createConsumer()
	for {
		msg, err = consumer.ReadMessage(-1)
		if err != nil {
			fmt.Println("error reading kafka message", err.Error())
			continue
		} else {
			log.Printf("received message on partition [%d]: %s\n",
				msg.TopicPartition, string(msg.Value))
		}
	}
}

func Publish(ctx context.Context, value []byte) {
	var (
		event kafka.Event
	)

	doneChan := make(chan bool)

	go func() {
		defer close(doneChan)
		for event = range producer.Events() {
			switch ev := event.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *ev.TopicPartition.Topic,
						ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
				return
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()
	producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &config.topic, Partition: kafka.PartitionAny},
		Key:            nil,
		Value:          value,
	}
	_ = <-doneChan
}

func StartProducer(ctx context.Context) {
	var (
		err error
	)
	// create an idempotent/safe producer
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":  config.broker,
		"group.id":           config.groupId,
		"enable.idempotence": true,
		"acks":               "-1",

		// high throughput producer
		"compression.type":   "snappy",
		"linger.ms":          "20",
		"batch.num.messages": strconv.Itoa(32 * 1024), // 32kb
	})
	if err != nil {
		log.Panicln("error creating a new kafka producer", err.Error())
	}
	log.Println("kafka producer created")
}
