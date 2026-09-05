# Kafka From Scratch

A simplified Kafka-like message broker built from scratch in Java.

## Features

- Concurrent TCP client connections using Java `Socket` and a fixed thread pool
- Custom binary request/response protocol with framing and correlation IDs
- Topics and partitions
- Append-only persistent partition logs with offset-based reads
- Log recovery after broker restart
- Producer and consumer fetch APIs
- Offset-based consumer reads
- Multi-broker metadata and replication
- Leader/follower replication
- Broker failure detection and simplified leader failover
- Basic broker metrics and concurrent producer benchmarking

## Architecture

```text
Client
  |
  | TCP + custom binary protocol
  v
KafkaBroker
  |
  +-- ClientConnection
  |
  +-- TopicManager
  |     |
  |     +-- PartitionLog
  |           |
  |           +-- LogSegment
  |
  +-- ClusterMetadata
  |
  +-- ReplicationManager
  |
  +-- FailureDetector
  |
  +-- BrokerMetrics
```

## Project Structure

```text
src/main/java/com/kafka/
├── broker/
│   ├── BrokerBenchmark.java
│   ├── BrokerInfo.java
│   ├── BrokerMetrics.java
│   ├── ClientConnection.java
│   ├── ClusterMetadata.java
│   ├── FailureDetector.java
│   ├── KafkaBroker.java
│   ├── PartitionMetadata.java
│   ├── ReplicationManager.java
│   └── TopicManager.java
├── protocol/
│   ├── FetchRequest.java
│   ├── ProduceRequest.java
│   ├── ProtocolDecoder.java
│   ├── ProtocolEncoder.java
│   ├── ProtocolFrame.java
│   ├── ReplicateRequest.java
│   └── RequestType.java
└── storage/
    ├── FetchResult.java
    ├── LogIndex.java
    ├── LogRecord.java
    ├── LogSegment.java
    └── PartitionLog.java
```

## Requirements

- Java 17+
- Maven 3.8+

## Build & Test

```bash
./mvnw clean test
```

## Run the Broker

The broker can be started from Java with:

```java
new KafkaBroker(9092, 8).start();
```

For persistent storage:

```java
new KafkaBroker(9092, 8, dataDirectory).start();
```

A broker can also be created with an explicit broker ID:

```java
new KafkaBroker(1, 9092, 8, dataDirectory).start();
```

## Protocol

Supported request types:

```text
PING
PRODUCE
FETCH
REPLICATE
```

Each frame contains:

```text
version
request type
correlation ID
payload
```

`PRODUCE` returns the assigned record offset.

`FETCH` returns the requested record or `-1` when no record is available at the requested offset.

## Storage

Each partition uses an append-only log.

Records contain:

```text
offset
timestamp
payload length
payload
```

Offsets are rebuilt from the log during startup recovery.

Incomplete records at the end of a segment are truncated during recovery. Corruption in the middle of a segment is treated as an error.

## Replication & Failover

Partition metadata tracks:

- leader broker
- replica brokers
- broker liveness

The leader replicates records to available followers.

The failure detector periodically probes brokers. If the current leader is unavailable, a live replica can be elected as the new leader.

This is a simplified failover mechanism, not a consensus protocol such as Raft.

## Benchmark

The included benchmark supports:

```bash
java ... BrokerBenchmark [messages] [messageBytes] [clients]
```

Example result from this implementation:

```text
100,000 messages
1,024 bytes/message
8 concurrent clients

Throughput: 583.06 msg/s
p50 latency: 12.031 ms
p95 latency: 20.068 ms
p99 latency: 27.535 ms
```

The benchmark measures a single broker with concurrent producers.

## Design Principles

- Keep networking, protocol, storage, and cluster coordination separate.
- Prefer simple mechanisms over unnecessary abstractions.
- Make persistence and recovery explicit.
- Define concurrency ownership clearly.
- Validate untrusted network input.
- Keep failure handling part of the normal broker lifecycle.
