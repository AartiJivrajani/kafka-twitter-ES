## Producer Configs 

Details: https://kafka.apache.org/documentation/#producerconfigs


###### Acks (producer level)
- Ack = 0: No ACK needed.
- Ack = 1: ACK expected from the leader. Does not guarantee the replication on all the replicas.
- Ack = all: ACK expected from the leaders as well as the replicas - Adds more latency. No data loss.
        has to go hand-in-hand with min.insync.replicas: Can be set at broker or topic level.
        so for eg, `min.insync.replicas=2` implies that at least 2 brokers that are ISR(including leader) must
        respond that they have data.
        So, if replication_factor=3, min.insync=2, ack=all, we can only tolerate 1 broker going down otherwise
        the producer will receive an exception on send.

###### Retries (producer level)
- default: 0
- in case of other positive values, by default, there is a chance that the messages will be sent out of order
  (if a batch of messages has failed to be sent) - this is cause messages get requeued for send.
- this can be handled using the `max.in.flight.requests.per.connection` setting which controls how many produce
  requests can be made in parallel. By default, it is 5.
    - set it to 1.

###### Idempotent producer
- Due to network errors, messages duplication can be introduced by the kafka producers
- in Kafka >= 0.11, a produce request ID is created by the producer, and a de-duplication happens at the
  produce-request level.
 - retry here is set to MAX_VALUE
- max.in.flight.requests=1 (kafka >= 0.11 & < 1.1)
- max.in.flight.requests=5 (kafka >= 1.1, default)
- acks=all
- Safe producer:
    `enable.idempotence=true(producer level)  + min.insync.replicas=2(topic/broker level)`
    - keeps ordering, no duplicated, improves performance
    - might impact throughput and latency.

###### Compression
- Compression works at producer level
- Lets say we have M1, M2, M3.... M100
- If compression is enabled, all these messages are compressed into one batch and sent to kafka

    Advantage:
    - Much smaller producer request size.
    - Faster to transfer data over the network => less latency.
    - Better throughput
    - Better disk utilization in kafka (stored messages on disk are smaller)
    
    Disadvantage:
    - CPU cycles spent by the producer for compression
    - CPU cycles spent by the consumer for decompression

###### Linger.ms & batch size
- By default, kafka tries to send records ASAP
    - it will have upto 5 requests in flight, i.e., 5 messages individually
      sent at the same time. 
    - After this, if more messages have to be sent while others are in flight, Kafka will start
      batching them while they wait to send them all at once. 
- Smart batching allows Kafka to increase throughput while maintaining low latency
- Batches have higher compression ration so better efficiency.
- `linger.ms`: no. of ms a producer is willing to wait before sending a batch out(default 0)
- By introducing some lag(for eg, linger.ms=5), we increase the chances of messages being sent together in a batch
- if the batch is size, even before the linger.ms time is up, kafka sends out this message immediately :) 
- `batch.size` is the max no. of bytes that will be included in a batch(default: 16kb)
- any message bigger than batch.size will not be batched. (Note: here, `batch.num.messages` is used)
- A batch is allocated per partition

###### Producer Key Partitioning

- By default, keys are hashed using the murmur2 algorithm
```$xslt
targetPartition = ABS(murmur2(key)) % numPartition
```
- Implies that the same key will go to the same partition and adding partition to a topic will entirely alter the formula

###### Max.block.ms and buffer.memory
- If the producer produces faster that ingest capacity of the broker, records get buffered in the
memory. 
- Default buffer size: 32MB
- If the buffer is full, then the publish will start to block(wont return right away). The blocking time
is controlled by `max.block.ms=60000`. After this time, an exception is thrown. 


## Consumer Config
###### Idempotent consumer
- In case of delivery paradigms in kafka like "atleast once", chances are that the messages are duplicated
especially when the offsets are not committed to kafka. 
- In this case, we can use a kafka generic ID can be used which could be something like - 
`record_topic + record_partition + record_offset` and use this as the ID in the index request
of elastic search

###### Consumer offset commit strategies
- `enable.auto.commit=true` and synchronous processing of batches (default)
    - offsets will be committed automatically at regular intervals
    - `auto.commit.interval.ms=5000` by default
- `enable.auto.commit=false` and manual commit of offsets
    - you are in control of when the offsets are committed. 
    - example: accumulating records into a buffer and then flushing the buffer to a database and committing the
    offsets then.

###### Consumer offset reset behaviour
- In cases where the consumer might go down for a period longer than the retention period
of the broker, we might lose the offsets. 
- In such a case, use `auto.offset.reset config` 
    - `auto.offset.reset=latest` -> read from the end of the log
    - `auto.offset.reset=earliest` -> read from the start of the log
    - `auto.offset.reset=none` -> throw exception if no offset is found 
- Additionally, consumer offsets can also be lost. For example, in case of Kafka >= 2.0, it is 
lost if consumer hasn't read new data in 7 days. 
- This can be altered by setting the `offset.retention.minutes` config on the broker

######  Replaying data for consumers
To replay data for a consumer group
- Take all the consumers from a specific group down. 
- Use `kafka-consumer-groups` to set offset to what we want
- restart the consumers

##### Controlling consumer liveliness
- Each consumer polls kafka - using poll thread
- Each consumer also polls the consumer co-ordinator(acting broker) - using heartbeat thread
- Consumers should process data fast and poll often!! 

###### HeartBeat thread
- `session.timeout.ms` - default 10s
- heartbeats sent periodically to the broker
- If no heartbeat is sent, consumer is considered dead. 

- `heartbeat.interval.ms` - default 3s
- how often should we send the heartbeats
- 1/3rd of the session timeout. 

###### Consumer Poll Thread
- `max.poll.interval.ms` - default 5 minutes
- max amount of time b/w 2 .poll() calls before declaring the consumer dead
