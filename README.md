

# MiniKafka

MiniKafka is a lightweight message broker built from scratch in Java. It implements the core ideas behind systems like Apache Kafka: producers publish messages to topics, the broker stores them in append-only partition logs, and consumers fetch messages by offset.

This project does **not** use Kafka or any external message broker library. The broker, client protocol, networking, disk log, producer, and consumer are implemented manually.

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
DiskMessageLog
   |
   v
data/*.log

Consumer
   ^
   |
   |  TCP / offset-based fetch
   |
BrokerServer
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

### Producer

- Connects to the broker over TCP
- Sends messages using a custom binary protocol
- Receives the assigned offset from the broker

### Consumer

- Connects to the broker over TCP
- Fetches messages from a topic partition
- Tracks its current offset locally
- Supports seeking to a specific offset

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

---

## Wire protocol

All requests start with a one-byte command.

### Commands

| Command | Value | Description |
|---|---:|---|
| `CMD_SEND` | `1` | Append a message to a topic partition |
| `CMD_FETCH` | `2` | Fetch messages from a topic partition |

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

---

## Project structure

```text
src/main/java/dev/ayan
├── Main.java
├── Protocol.java
├── broker
│   ├── Partition.java
│   └── Topic.java
├── client
│   ├── Consumer.java
│   └── Producer.java
├── log
│   ├── DiskMessageLog.java
│   └── LogEntry.java
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

The demo starts a broker, creates a `payments` topic, sends messages through a producer, and reads them through a consumer.

---

## Example output

```text
[Broker] Topic created: payments with 1 partition(s)
[Broker] Listening on port 9092
[Broker] Client connected: /127.0.0.1
[Broker] Client connected: /127.0.0.1
[Producer] Sent 19 byte(s) to payments-0 at offset 0
[Producer] Sent 19 byte(s) to payments-0 at offset 1
[Producer] Sent 20 byte(s) to payments-0 at offset 2

--- Poll 1 ---
[Consumer] Read payments-0 offset 0: "payment:user1:50USD"
[Consumer] Read payments-0 offset 1: "payment:user2:30USD"
[Producer] Sent 19 byte(s) to payments-0 at offset 3
[Producer] Sent 19 byte(s) to payments-0 at offset 4

--- Poll 2 ---
[Consumer] Read payments-0 offset 2: "payment:user3:100USD"
[Consumer] Read payments-0 offset 3: "payment:user4:20USD"
[Consumer] Read payments-0 offset 4: "payment:user5:75USD"
[Broker] Client disconnected.
[Broker] Client disconnected.
```

---

## Current limitations

MiniKafka is intentionally small and educational. It currently does not implement:

- Replication
- Consumer groups
- Leader election
- Log compaction
- Segment rolling
- Authentication
- Network retries
- Durable consumer offset commits

These are possible future improvements.

---

## Possible next improvements

- Add log segments instead of one file per partition
- Add sparse indexes for faster offset lookup
- Add consumer offset commits
- Add multiple partitions with producer-side partition selection
- Add a command-line interface
- Add JUnit tests for log storage and protocol behavior
- Add replication between brokers

---

## What I learned

This project helped me understand how message brokers work under the hood: how clients communicate with a broker, how messages are serialized over the network, how append-only logs work, and how consumers use offsets to read data reliably.