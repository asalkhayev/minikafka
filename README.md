

# MiniKafka

MiniKafka is a lightweight message broker built from scratch in Java. It implements the core ideas behind systems like Apache Kafka: producers publish messages to topics, the broker stores them in append-only partition logs, consumers fetch messages by offset, and consumer groups commit progress so they can resume later.

This project does **not** use Kafka or any external message broker library. The broker, client protocol, networking, disk log, sparse index, producer, consumer, and consumer group offset tracking are implemented manually.

---

## Why this project matters

MiniKafka demonstrates several important backend and systems engineering concepts:

- Raw TCP networking with `ServerSocket` and `Socket`
- Custom binary wire protocol
- Append-only disk storage
- Offset-based message reads
- Topics and partitions
- Producer and consumer clients
- Concurrent client handling with threads
- Defensive validation for corrupted or invalid requests
- Persistent logs stored on disk
- Consumer group offset commits
- Sparse index files for faster offset lookup

The goal is to understand how a message broker works internally instead of only using one as a black box.

---

## Architecture

```text
Producer
   |
   |  TCP / binary protocol
   v
BrokerServer
   |
   v
Topic
   |
   v
Partition
   |
   v
DiskMessageLog  --->  SparseIndex
   |                   |
   v                   v
data/*.log        data/*.index

GroupConsumer
   ^
   |
   |  TCP / offset-based fetch + offset commits
   |
BrokerServer
   |
   v
ConsumerGroupManager
```

---

## Features implemented

### Broker

- Starts a TCP server on a configured port
- Accepts multiple client connections
- Handles each client in a separate thread
- Supports topic creation
- Routes messages to the correct topic partition
- Returns success/error responses to clients
- Stores committed offsets for consumer groups
- Supports clean shutdown for demo and Maven execution

### Producer

- Connects to the broker over TCP
- Sends messages using a custom binary protocol
- Receives the assigned offset from the broker

### Consumer and group consumer

- Connects to the broker over TCP
- Fetches messages from a topic partition
- Tracks its current offset locally
- Supports seeking to a specific offset
- Supports consumer groups with broker-side committed offsets
- Allows a consumer in the same group to resume from the last committed offset

### Storage

Messages are stored in an append-only log file on disk.

Each record contains:

```text
offset      8 bytes
timestamp   8 bytes
length      4 bytes
payload     N bytes
```

This gives every message a stable offset and allows consumers to fetch messages starting from any offset.

MiniKafka also creates a sparse index file for each partition:

```text
payments-0.log
payments-0.index
```

The index stores every 100th offset and its byte position in the log file:

```text
offset          8 bytes
byte position   8 bytes
```

When reading from an offset, the broker uses the sparse index to seek near the requested offset instead of always scanning the log from the beginning.

---

## Wire protocol

All requests start with a one-byte command.

### Commands

| Command | Value | Description |
|---|---:|---|
| `CMD_SEND` | `1` | Append a message to a topic partition |
| `CMD_FETCH` | `2` | Fetch messages from a topic partition |
| `CMD_COMMIT_OFFSET` | `3` | Commit a consumer group's current offset |
| `CMD_GET_OFFSET` | `4` | Fetch a consumer group's committed offset |

### Responses

| Response | Value | Description |
|---|---:|---|
| `RES_OK` | `0` | Request succeeded |
| `RES_ERROR` | `1` | Request failed |

### SEND request

```text
[command: 1 byte]
[topic length: 2 bytes]
[topic bytes: N bytes]
[partition id: 4 bytes]
[payload length: 4 bytes]
[payload bytes: N bytes]
```

### SEND response

```text
[response code: 1 byte]
[offset: 8 bytes]
```

### FETCH request

```text
[command: 1 byte]
[topic length: 2 bytes]
[topic bytes: N bytes]
[partition id: 4 bytes]
[from offset: 8 bytes]
[max entries: 4 bytes]
```

### FETCH response

```text
[response code: 1 byte]
[entry count: 4 bytes]

For each entry:
[offset: 8 bytes]
[timestamp: 8 bytes]
[payload length: 4 bytes]
[payload bytes: N bytes]
```

### COMMIT_OFFSET request

```text
[command: 1 byte]
[group length: 2 bytes]
[group bytes: N bytes]
[topic length: 2 bytes]
[topic bytes: N bytes]
[partition id: 4 bytes]
[offset: 8 bytes]
```

### COMMIT_OFFSET response

```text
[response code: 1 byte]
```

### GET_OFFSET request

```text
[command: 1 byte]
[group length: 2 bytes]
[group bytes: N bytes]
[topic length: 2 bytes]
[topic bytes: N bytes]
[partition id: 4 bytes]
```

### GET_OFFSET response

```text
[response code: 1 byte]
[committed offset: 8 bytes]
```

---

## Project structure

```text
src/main/java/dev/ayan
├── Main.java
├── Protocol.java
├── broker
│   ├── ConsumerGroupManager.java
│   ├── Partition.java
│   └── Topic.java
├── client
│   ├── Consumer.java
│   ├── GroupConsumer.java
│   └── Producer.java
├── log
│   ├── DiskMessageLog.java
│   ├── LogEntry.java
│   └── SparseIndex.java
└── server
    └── BrokerServer.java
```

---

## How to run

### Requirements

- Java 23+
- Maven

Check Java:

```bash
java -version
```

Check Maven:

```bash
mvn -v
```

### Compile

```bash
mvn clean compile
```

### Run demo

```bash
mvn exec:java
```

The demo starts a broker, creates a `payments` topic, sends messages through a producer, reads them through consumer groups, commits offsets, and shows that another consumer in the same group resumes from the committed offset.

---

## Example output

```text
[Broker] Topic created: payments with 1 partition(s)
[Broker] Listening on port 9092
[Broker] Client connected: /127.0.0.1
[Producer] Sent 19 byte(s) to payments-0 at offset 0
[Producer] Sent 19 byte(s) to payments-0 at offset 1
[Producer] Sent 20 byte(s) to payments-0 at offset 2
[Producer] Sent 19 byte(s) to payments-0 at offset 3
[Producer] Sent 19 byte(s) to payments-0 at offset 4
[Broker] Client disconnected.

--- Group consumer instance 1: poll first 2 and commit ---
[Broker] Client connected: /127.0.0.1
[GroupConsumer] Started group=payments-service, topic=payments, partition=0, offset=0
[GroupConsumer] group=payments-service read payments-0 offset 0: "payment:user1:50USD"
[GroupConsumer] group=payments-service read payments-0 offset 1: "payment:user2:30USD"
[ConsumerGroupManager] Committed offset 2 for group=payments-service, topic=payments, partition=0
[GroupConsumer] Committed offset 2 for group=payments-service, topic=payments, partition=0

--- Group consumer instance 2: resumes from committed offset ---
[Broker] Client connected: /127.0.0.1
[GroupConsumer] Started group=payments-service, topic=payments, partition=0, offset=2
[GroupConsumer] group=payments-service read payments-0 offset 2: "payment:user3:100USD"
[GroupConsumer] group=payments-service read payments-0 offset 3: "payment:user4:20USD"
[GroupConsumer] group=payments-service read payments-0 offset 4: "payment:user5:75USD"
[ConsumerGroupManager] Committed offset 5 for group=payments-service, topic=payments, partition=0
[GroupConsumer] Committed offset 5 for group=payments-service, topic=payments, partition=0

--- Different group: starts from offset 0 ---
[Broker] Client connected: /127.0.0.1
[GroupConsumer] Started group=audit-service, topic=payments, partition=0, offset=0
[GroupConsumer] group=audit-service read payments-0 offset 0: "payment:user1:50USD"
[GroupConsumer] group=audit-service read payments-0 offset 1: "payment:user2:30USD"
[GroupConsumer] group=audit-service read payments-0 offset 2: "payment:user3:100USD"
[GroupConsumer] group=audit-service read payments-0 offset 3: "payment:user4:20USD"
[GroupConsumer] group=audit-service read payments-0 offset 4: "payment:user5:75USD"
[ConsumerGroupManager] Committed offset 5 for group=audit-service, topic=payments, partition=0
[GroupConsumer] Committed offset 5 for group=audit-service, topic=payments, partition=0
[Broker] Stopped.
```

---

## Current limitations

MiniKafka is intentionally small and educational. It currently does not implement:

- Replication
- Leader election
- Log compaction
- Segment rolling
- Authentication
- Network retries

These are possible future improvements.

---

## Possible next improvements

- Add log segments instead of one file per partition
- Add multiple partitions with producer-side partition selection
- Add a command-line interface
- Add JUnit tests for log storage and protocol behavior
- Add replication between brokers
- Persist consumer group offsets to disk
- Add benchmarks for sparse-index reads

---

## What I learned

This project helped me understand how message brokers work under the hood: how clients communicate with a broker, how messages are serialized over the network, how append-only logs work, how sparse indexes speed up offset-based reads, and how consumer groups use committed offsets to resume processing reliably.