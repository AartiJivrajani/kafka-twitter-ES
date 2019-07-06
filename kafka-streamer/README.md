Acks(producer level):
- Ack = 0: No ACK needed.
- Ack = 1: ACK expected from the leader. Does not guarantee the replication on all the replicas.
- Ack = all: ACK expected from the leaders as well as the replicas - Adds more latency. No data loss.
        has to go hand-in-hand with min.insync.replicas: Can be set at broker or topic level.
        so for eg, min.insync.replicas=2 implies that atleast 2 brokers that are ISR(including leader) must
        respond that they have data.
        So, if replication_factor=3, min.insync=2, ack=all, we can only tolerate 1 broker going down otherwise
        the producer will receive an exception on send.

Retries(producer level):
- default: 0
- in case of other positive values, by default, there is a chance that the messages will be sent out of order
  (if a batch of messages has failed to be sent) - this is cause messages get requeued for send.
- this can be handled using the max.in.flight.requests.per.connection setting which controls how many produce
  requests can be made in parallel. By default, it is 5.
    - set it to 1.

Idempotent producer:
- Due to network errors, messages duplication can be introduced by the kafka producers
- in Kafka >= 0.11, a produce request ID is created by the producer, and a de-duplication happens at the
  produce-request level.
 - retry here is set to MAX_VALUE
- max.in.flight.requests=1 (kafka >= 0.11 & < 1.1)
- max.in.flight.requests=5 (kafka >= 1.1, default)
- acks=all
- Safe producer:
    enable.idempotence=true(producer level)  + min.insync.replicas=2(topic/broker level)
    - keeps ordering, no duplicated, improves performance
    - might impact throughput and latency.

Compression
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
