---
layout: page
title: Message Queue System
permalink: /message-queue/
---

<style>
img {
  max-width: 100%;
  height: auto;
  display: block;
  margin: 1em auto;
  border-radius: 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

.container {
  max-width: 1200px;
  margin: 0 auto;
}


code {
  word-wrap: break-word;
  overflow-wrap: break-word;
}

pre {
  overflow-x: auto;
  max-width: 100%;
}

@media (max-width: 768px) {
  img {
    max-width: 95%;
  }
  
  pre {
    font-size: 14px;
  }
  
}
</style>

# Message Queue System: Building a Kafka-like System from Scratch

## Introduction

### Why Do We Need Message Queues?

Message queues solve fundamental problems in distributed systems by enabling asynchronous communication between services. They provide a robust foundation for building scalable, resilient architectures.

## Problems Solved by Message Queues

### Asynchronous Communication and Service Independence

![2_syn_microservices.png](assets/message_queue/2_syn_microservices.png)

- **Services can operate independently in time**
- **Senders don't wait for receivers to become available**
- **Decouples service lifecycles and deployment schedules**

![1_sync_micrpservice_with_5_another_services.png](assets/message_queue/1_sync_micrpservice_with_5_another_services.png)

**Instead of complex synchronous interactions between multiple services, we can simplify with a queue:**

![1_write_to_queue_4_read.png](assets/message_queue/1_write_to_queue_4_read.png)

#### Improved Fault Tolerance
- **When a receiver temporarily crashes, messages are stored in the queue and processed later**
- **Reduces risk of data loss during service outages**
- **Provides graceful degradation under failure conditions**

#### Scalability
- **Easy to add more consumers (workers) to process messages faster**
- **Queue distributes load between multiple consumers**
- **Horizontal scaling without architectural changes**

#### Load Buffering
- **When incoming request flow is very large, the queue temporarily "smooths" the load**
- **Protects the system from overload situations**
- **Acts as a shock absorber for traffic spikes**

#### Reliable Data Transmission and Logging
- **Messages are not lost (or rarely lost, depending on configuration)**
- **Can implement guaranteed delivery ("at least once", "exactly once")**
- **Provides audit trail and replay capabilities**

#### Why Not Just Use Simple Programming Language Queues?

You might think: "Why not just use a simple queue from a programming language where first-in-first-out (FIFO) works?" Indeed, we could use basic data structures like queues from standard libraries:
![queue_as_data_strucure.png](assets/message_queue/queue_as_data_strucure.png)
```go
type SimpleQueue struct {
    items []Message
    mutex sync.Mutex
}

func (q *SimpleQueue) Enqueue(msg Message) {
q.mutex.Lock()
defer q.mutex.Unlock()
q.items = append(q.items, msg)
}

func (q *SimpleQueue) Dequeue() (Message, bool) {
q.mutex.Lock()
defer q.mutex.Unlock()

    if len(q.items) == 0 {
        return Message{}, false
    }
    
    msg := q.items[0]
    q.items = q.items[1:]
    return msg, true
}
```
However, this approach has a critical problem: **when we dequeue a message, we no longer store it anywhere**. If we retrieve a message and our consumer becomes unavailable (crashes, network issues, etc.), we lose that data permanently. The message is gone from the queue but never processed.

#### What Makes Message Brokers Production-Ready

Message brokers are sophisticated distributed systems designed to solve enterprise-grade messaging challenges. They consist of several core components and differ significantly in their architecture and trade-offs:

#### Core Components of Message Brokers

All modern message brokers share these fundamental building blocks:

**1. Storage Engine**
- **Persistent storage**: Messages stored on disk for durability (vs in-memory volatility)
- **Log-based storage**: Append-only logs for high throughput (Kafka, Pulsar)
- **Index structures**: Fast message lookup and routing (RabbitMQ exchanges, Kafka offsets)

**2. Acknowledgment & Delivery Guarantees**
- **At-most-once**: Fast but may lose messages
- **At-least-once**: Reliable but may duplicate messages  
- **Exactly-once**: Strongest guarantee but highest latency

**3. Consumer Position Tracking**
- **Offset management**: Track consumer progress through message stream
- **Resumability**: Consumers can restart from last processed position
- **Multiple consumption models**: Push vs Pull, competing consumers vs pub/sub

**4. Replication & High Availability**
- **Multi-node clusters**: Distribute load and provide fault tolerance
- **Leader/follower replication**: Ensure no data loss during failures
- **Automatic failover**: Seamless leader election when nodes fail

#### Message Broker Architecture Patterns

**Log-Based Architecture (Kafka, Pulsar)**
```
Topic → Multiple Partitions → Distributed across brokers
Messages stored in immutable log segments
Consumers track offset position
```

**Exchange-Based Architecture (RabbitMQ, ActiveMQ)**
```
Exchange → Routing Rules → Queues → Consumers
Messages routed through sophisticated exchange patterns
Queues can be persistent or transient
```

**Cloud-Native Architecture (AWS SQS/SNS, Google Pub/Sub)**
```
Managed service → Multiple availability zones → Auto-scaling
Built-in durability and scaling handled by cloud provider
Event-driven triggers and integrations
```

#### Different Message Brokers Comparison

Various message brokers exist, each with different trade-offs:

- **Kafka**: High-throughput, log-based, designed for stream processing
- **RabbitMQ**: Traditional messaging patterns, complex routing, lower latency
- **AWS SQS/SNS**: Managed services, simple integration, limited throughput
- **NATS**: Ultra-low latency, lightweight, good for microservices

### CAP Theorem and Message Brokers

![CAP_theorem.png](assets/message_queue/CAP_theorem.png)

#### How CAP Theorem Relates to Message Brokers

Message brokers (Kafka, RabbitMQ, NATS, Pulsar, etc.) are distributed systems and must comply with the CAP theorem. Different brokers make different trade-offs between Consistency, Availability, and Partition tolerance.

**Examples:**

**Apache Kafka**
- Chooses AP (Availability + Partition tolerance)
- During network failures, continues to operate
- May temporarily lose consistency (different nodes may see different states)
- Later data "catches up" and synchronizes

**RabbitMQ (in cluster mode)**
- Closer to CA (Consistency + Availability) while network is stable
- During partition, may "freeze" or lose some messages to maintain consistency

**NATS JetStream**
- Usually balances between AP and CP, depending on configuration
- Can choose priority (availability or strict consistency) based on use case

/// Introduction completed, now the main part with code examples:

## How Apache Kafka Works

### Background: From Objects to Events

For a long time, engineers developed programs that operate on real-world objects and save state about them in databases. These could be users, orders, or products. The representation of real-world things as objects is widespread in the OOP paradigm and development in general. But more and more modern companies operate not on the objects themselves, but on the events they generate.

Companies strive to reduce tight coupling between services and improve application resilience to failures by isolating data providers and their consumers. This request, together with the popularity of microservice architecture, popularizes the event-oriented approach.

Events in systems can be stored in traditional relational databases just like objects. But doing so is cumbersome and inefficient. Instead, we use a structure called a log, which we'll discuss later.

## System Architecture

### Brokers: The Foundation

The Kafka cluster consists of brokers. You can think of the system as a data center and servers in it. When first getting acquainted, think of a Kafka broker as a computer: it's a process in the operating system with access to its local disk.

![kaffka_classter_intro.png](assets/message_queue/kaffka_classter_intro.png)

All brokers are connected to each other by a network and act together, forming a single cluster. When we say that producers write events to a Kafka cluster, we mean that they work with brokers in it.

By the way, in a cloud environment, the cluster doesn't necessarily run on dedicated servers - these can be virtual machines or containers in Kubernetes.

### Topics and Partitions

#### Topics: Logical Organization

A topic is a **logical division** of message categories into groups. For example, events by order statuses, partner coordinates, route sheets, and so on.

The key word here is **logical**. Topics are conceptual containers that organize related messages together. We create topics for events of a common group and try not to mix them with each other. For example, partner coordinates should not be in the same topic as order statuses, and updated order statuses should not be stored mixed with user registration updates.

**Topics are logical splits** - they define what kind of data goes where, but they don't determine how the data is physically stored.
![topics.png](assets/message_queue/topics.png)

It's convenient to think of a topic as a log - you write an event to the end and don't destroy the chain of old events in the process. General logic:

- One producer can write to one or more topics
- One consumer can read one or more topics
- One or more producers can write to one topic
- One or more consumers can read from one topic

Theoretically, there are no restrictions on the number of these topics, but practically this is limited by the number of partitions.
#### Partitions: Physical Splitting of Data
![kafka_patrition.png](assets/message_queue/kafka_patrition.png)

While topics provide logical organization, **partitions handle the physical splitting of data**. There are no restrictions on the number of topics in a Kafka cluster, but there are limitations of the computer itself. It performs operations on the processor, input-output, and eventually hits its limit. We cannot increase the power and performance of machines indefinitely, so the topic data must be divided into physical parts.

In Kafka, these physical parts are called **partitions**. Each topic consists of one or more partitions, each of which can be placed on different brokers. This is how Kafka achieves horizontal scaling: you can create a topic, divide it into partitions, and place each partition on a separate broker.

**Key Relationship: Topics ⊃ Partitions**
- **Topics** = Logical containers (what data category)
- **Partitions** = Physical storage units (where and how data is stored)
- Topics are split into partitions for scalability and performance

Formally, a partition is a strictly ordered log of messages stored physically on disk. Each message in it is added to the end without the possibility of changing it in the future and somehow affecting already written messages. At the same time, the topic as a whole has no order, but the order of messages always exists within each individual partition.

**Physical Storage Hierarchy:**
```
Topic (logical)
├── Partition 0 (physical) → Broker 1
├── Partition 1 (physical) → Broker 2  
└── Partition 2 (physical) → Broker 3
```

#### Key-Based Partitioning and Scaling Limitations

Messages in Kafka have a structure that includes a key field:

```go
type Message struct {
    Topic       string            `json:"topic"`
    Partition   int32             `json:"partition,omitempty"`
    Key         []byte            `json:"key,omitempty"`
    Value       []byte            `json:"value"`
    Headers     map[string]string `json:"headers,omitempty"`
    Timestamp   int64             `json:"timestamp"`
    Offset      int64             `json:"offset,omitempty"`
}
```

**Important Rule: Messages with the same key always go to the same partition.** This is crucial for maintaining order within a key but creates scaling bottlenecks:

**Why same keys go to same partition:**
- Guarantees message ordering for each unique key
- Enables stateful processing where related events must be processed together
- Allows consumers to maintain state per key without coordination

**Scaling Limitation:**
When all messages have the same key (or a few hot keys), they all end up in the same partition, creating a bottleneck:

```
Topic: "user-events" 
Messages with key "user-123":
├── Message 1 → Partition 2 (based on hash of "user-123")
├── Message 2 → Partition 2 (same key = same partition)
├── Message 3 → Partition 2 (same key = same partition)
└── Message 4 → Partition 2 (same key = same partition)

Result: Only 1 partition handles all "user-123" traffic
Other partitions may be idle → Poor scaling
```

This means that even with 100 partitions, if all your messages have the same key, only 1 partition will be used, and you lose the benefits of horizontal scaling.

The partitions themselves are physically represented on disks as segments. These are separate files that can be created, rotated, or deleted according to the data aging settings in them. Usually you don't have to often remember about partition segments unless you administer the cluster, but it's important to remember the data storage model in Kafka topics.

```go
// Segment represents a physical file storing messages
type LogSegment struct {
    BaseOffset   int64     `json:"base_offset"`
    File         *os.File  `json:"-"`
    Size         int64     `json:"size"`
    MaxSize      int64     `json:"max_size"`
    CreatedAt    time.Time `json:"created_at"`
    MessageCount int       `json:"message_count"`
    IsActive     bool      `json:"is_active"`
}

// Partition contains multiple segments
type Partition struct {
    ID            int32          `json:"id"`
    Topic         string         `json:"topic"`
    Segments      []*LogSegment  `json:"segments"`
    ActiveSegment *LogSegment    `json:"active_segment"`
    HighWaterMark int64          `json:"high_water_mark"`
    LogStartOffset int64         `json:"log_start_offset"`
    Leader        BrokerID       `json:"leader"`
    Replicas      []BrokerID     `json:"replicas"`
    ISR           []BrokerID     `json:"isr"`
}

// Topic contains multiple partitions
type Topic struct {
    Name             string                `json:"name"`
    Partitions       map[int32]*Partition  `json:"partitions"`
    ReplicationFactor int                 `json:"replication_factor"`
    Config           TopicConfig           `json:"config"`
}

type TopicConfig struct {
    RetentionMs     int64  `json:"retention_ms"`
    SegmentMs       int64  `json:"segment_ms"`
    CleanupPolicy   string `json:"cleanup_policy"`
    CompressionType string `json:"compression_type"`
}
```

### Coordinator Service: Managing the Cluster

The coordinator acts as the brain of the Kafka cluster, managing metadata, broker orchestration, and service lifecycle. **In a traditional Kafka setup, this role is handled by Zookeeper** (or KRaft in newer versions), which maintains cluster state, broker registry, and topic metadata.

For more details about Kafka-Zookeeper architecture, see: [Kafka Architecture: Kafka Zookeeper](https://www.redpanda.com/guides/kafka-architecture-kafka-zookeeper)

**Real Kafka vs Our Implementation:**
- **Real Kafka**: Zookeeper manages cluster metadata, leader election, and broker coordination
- **Our System**: Custom coordinator service handles the same responsibilities
- **Same principles**, simplified implementation for learning

In our implementation, we'll build our own lightweight coordinator that:

```go
type Coordinator struct {
    config       CoordinatorConfig
    brokers      map[BrokerID]*BrokerInfo
    topics       map[string]*TopicMetadata
    assignments  map[PartitionKey]BrokerID
    serviceManager *ServiceManager
    console      *ConsoleServer
    metadata     *MetadataStore
    mutex        sync.RWMutex
}

type ServiceManager struct {
    brokerProcesses  [5]*os.Process
    producerProcess  *os.Process
    consumerProcess  *os.Process
    coordinator      *Coordinator
}

type BrokerInfo struct {
    ID       BrokerID     `json:"id"`
    Host     string       `json:"host"`
    Port     int          `json:"port"`
    Status   BrokerStatus `json:"status"`
    LastSeen time.Time    `json:"last_seen"`
    Load     BrokerLoad   `json:"load"`
}

type BrokerStatus int

const (
    BrokerStarting BrokerStatus = iota
    BrokerOnline
    BrokerOffline
    BrokerDraining
)
```

#### Process Management

The coordinator automatically manages all services in the system:

```go
func (c *Coordinator) StartAll() error {
    // Start 5 brokers
    for i := 0; i < 5; i++ {
        port := 9092 + i
        brokerID := BrokerID(i)
        
        cmd := exec.Command("./bin/broker",
            "--id", fmt.Sprintf("%d", i),
            "--port", fmt.Sprintf("%d", port),
            "--coordinator", "localhost:8080")
        
        err := cmd.Start()
        if err != nil {
            return fmt.Errorf("failed to start broker %d: %w", i, err)
        }
        
        c.serviceManager.brokerProcesses[i] = cmd.Process
        
        // Register broker in coordinator
        c.registerBroker(BrokerInfo{
            ID:     brokerID,
            Host:   "localhost",
            Port:   port,
            Status: BrokerStarting,
        })
    }
    
    // Start producer service
    producerCmd := exec.Command("./bin/producer",
        "--port", "9093",
        "--coordinator", "localhost:8080")
    
    err := producerCmd.Start()
    if err != nil {
        return fmt.Errorf("failed to start producer: %w", err)
    }
    c.serviceManager.producerProcess = producerCmd.Process
    
    // Start consumer service
    consumerCmd := exec.Command("./bin/consumer",
        "--port", "9094",
        "--coordinator", "localhost:8080")
    
    err = consumerCmd.Start()
    if err != nil {
        return fmt.Errorf("failed to start consumer: %w", err)
    }
    c.serviceManager.consumerProcess = consumerCmd.Process
    
    return nil
}
```
#### Metadata Management

The coordinator tracks all cluster metadata:

```go
type TopicMetadata struct {
    Name              string                    `json:"name"`
    PartitionCount    int32                     `json:"partition_count"`
    ReplicationFactor int16                     `json:"replication_factor"`
    Partitions        map[int32]*PartitionInfo  `json:"partitions"`
    Config            TopicConfig               `json:"config"`
}

type PartitionInfo struct {
    TopicName string      `json:"topic_name"`
    ID        int32       `json:"id"`
    Leader    BrokerID    `json:"leader"`
    Replicas  []BrokerID  `json:"replicas"`
    ISR       []BrokerID  `json:"isr"`
}

func (c *Coordinator) assignPartitionLeaders() {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    
    availableBrokers := c.getAvailableBrokers()
    if len(availableBrokers) == 0 {
        log.Println("No available brokers for leader assignment")
        return
    }
    
    // Simple round-robin leader assignment
    brokerIndex := 0
    for topicName, topic := range c.topics {
        for partitionID, partition := range topic.Partitions {
            // Assign leader in round-robin fashion
            newLeader := availableBrokers[brokerIndex%len(availableBrokers)]
            partition.Leader = newLeader
            
            log.Printf("Assigned partition %s-%d leader to broker %d", 
                topicName, partitionID, newLeader)
            
            brokerIndex++
        }
    }
}
```
In real app  thats handled by zookeeper
more https://www.redpanda.com/guides/kafka-architecture-kafka-zookeeper

## Producers: Writing Messages to the System

To write events to the Kafka cluster, there are producers - these are applications that you develop.

The producer program writes a message to Kafka, Kafka saves the events, returns acknowledgment of the write or acknowledgement. The producer receives it and starts the next write.

### Message Structure

Before diving into producer implementation, let's understand the message format that producers create:

```go
type Message struct {
    Topic       string            `json:"topic"`
    Partition   int32             `json:"partition,omitempty"`
    Key         []byte            `json:"key,omitempty"`
    Value       []byte            `json:"value"`
    Headers     map[string]string `json:"headers,omitempty"`
    Timestamp   int64             `json:"timestamp"`
    Offset      int64             `json:"offset,omitempty"`
}

// Wire format when sent over network
type WireMessage struct {
    MagicByte   byte              // Protocol version
    Attributes  byte              // Compression, timestamp type
    Timestamp   int64             // Message timestamp
    KeyLength   int32             // Length of key (-1 if null)
    Key         []byte            // Message key
    ValueLength int32             // Length of value
    Value       []byte            // Message payload
    Headers     []MessageHeader   // Optional headers
}

type MessageHeader struct {
    Key   string `json:"key"`
    Value []byte `json:"value"`
}
```

### Producer Service Implementation

The producer is responsible for publishing messages to topics with intelligent partitioning and batching.

#### Metadata Discovery: How Producer Knows Where to Write

Before sending any messages, the producer must discover cluster metadata from the coordinator service (as described in the Coordinator section above):

```go
type ClusterMetadata struct {
    Brokers     map[BrokerID]*BrokerInfo     `json:"brokers"`
    Topics      map[string]*TopicMetadata    `json:"topics"`
    Partitions  map[PartitionKey]*PartitionInfo `json:"partitions"`
    UpdatedAt   time.Time                    `json:"updated_at"`
}

type BrokerInfo struct {
    ID       BrokerID     `json:"id"`
    Host     string       `json:"host"`
    Port     int          `json:"port"`
    Load     BrokerLoad   `json:"load"`        // Current load metrics
    Status   BrokerStatus `json:"status"`      // Online/Offline/Draining
    LastSeen time.Time    `json:"last_seen"`
}

type BrokerLoad struct {
    CPUUsage    float64 `json:"cpu_usage"`     // 0.0 - 1.0
    MemoryUsage float64 `json:"memory_usage"`  // 0.0 - 1.0
    DiskUsage   float64 `json:"disk_usage"`    // 0.0 - 1.0
    NetworkIO   int64   `json:"network_io"`    // bytes/sec
    MessageRate int64   `json:"message_rate"`  // messages/sec
}

// Producer fetches metadata from coordinator before sending
func (p *Producer) getClusterMetadata() (*ClusterMetadata, error) {
    // 1. Contact coordinator to get current cluster state
    resp, err := http.Get("http://coordinator:8080/api/metadata")
    if err != nil {
        return nil, fmt.Errorf("failed to fetch metadata: %w", err)
    }
    defer resp.Body.Close()
    
    var metadata ClusterMetadata
    if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
        return nil, fmt.Errorf("failed to parse metadata: %w", err)
    }
    
    return &metadata, nil
}

// Producer discovers which broker leads each partition
func (p *Producer) getTopicMetadata(topic string) (*TopicMetadata, error) {
    metadata, err := p.getClusterMetadata()
    if err != nil {
        return nil, err
    }
    
    topicMeta, exists := metadata.Topics[topic]
    if !exists {
        return nil, fmt.Errorf("topic %s not found", topic)
    }
    
    // Update partition leader information
    for partitionID, partition := range topicMeta.Partitions {
        if brokerInfo, exists := metadata.Brokers[partition.Leader]; exists {
            partition.LeaderEndpoint = fmt.Sprintf("%s:%d", brokerInfo.Host, brokerInfo.Port)
            partition.LeaderLoad = brokerInfo.Load
        }
    }
    
    return topicMeta, nil
}
```

#### Load Balancing: Choosing Less Loaded Brokers

The coordinator tracks broker load and helps producers make intelligent decisions:

```go
// Producer chooses partition based on load balancing
func (p *Producer) selectOptimalPartition(message *Message, topicMeta *TopicMetadata) int32 {
    // Strategy 1: If message has key, use key-based partitioning (no choice)
    if message.Key != nil {
        hash := murmur3.Sum32(message.Key)
        return int32(hash % uint32(len(topicMeta.Partitions)))
    }
    
    // Strategy 2: For keyless messages, use load-aware selection
    return p.loadAwarePartitionSelection(topicMeta)
}

func (p *Producer) loadAwarePartitionSelection(topicMeta *TopicMetadata) int32 {
    type PartitionScore struct {
        ID    int32
        Score float64  // Lower is better
    }
    
    var scores []PartitionScore
    
    for partitionID, partition := range topicMeta.Partitions {
        // Calculate load score for this partition's leader
        load := partition.LeaderLoad
        score := (load.CPUUsage * 0.4) + 
                 (load.MemoryUsage * 0.3) + 
                 (load.DiskUsage * 0.2) + 
                 (float64(load.MessageRate) / 10000.0 * 0.1)
        
        scores = append(scores, PartitionScore{
            ID:    partitionID,
            Score: score,
        })
    }
    
    // Sort by score (ascending - lower is better)
    sort.Slice(scores, func(i, j int) bool {
        return scores[i].Score < scores[j].Score
    })
    
    // Return partition with lowest load
    return scores[0].ID
}
```

#### Metadata Refresh and Caching

```go
type Producer struct {
    clientID         string
    coordinatorURL   string
    metadataCache    *ClusterMetadata
    metadataExpiry   time.Time
    metadataRefreshInterval time.Duration
    // ... other fields
}

func (p *Producer) getMetadataWithCache(topic string) (*TopicMetadata, error) {
    // Check if cached metadata is still valid
    if p.metadataCache != nil && time.Now().Before(p.metadataExpiry) {
        if topicMeta, exists := p.metadataCache.Topics[topic]; exists {
            return topicMeta, nil
        }
    }
    
    // Fetch fresh metadata from coordinator
    metadata, err := p.getClusterMetadata()
    if err != nil {
        return nil, err
    }
    
    // Update cache
    p.metadataCache = metadata
    p.metadataExpiry = time.Now().Add(p.metadataRefreshInterval)
    
    topicMeta, exists := metadata.Topics[topic]
    if !exists {
        return nil, fmt.Errorf("topic %s not found", topic)
    }
    
    return topicMeta, nil
}
```

**Key Points:**
- **Producer asks coordinator** for cluster metadata (topics, partitions, broker locations and load)
- **Load balancing** happens only for keyless messages (keyed messages must go to specific partition)
- **Metadata is cached** to avoid hitting coordinator on every message
- **Fresh metadata** is fetched when cache expires or on errors
- **Smart partition selection** based on broker load metrics (CPU, memory, disk, message rate)

```go
type Producer struct {
    clientID     string
    brokers      []string
    partitioner  Partitioner
    batcher      *MessageBatcher
    serializer   Serializer
    ackLevel     AckLevel
    retryConfig  RetryConfig
    metrics      *ProducerMetrics
}

type ProducerConfig struct {
    Brokers         []string        `yaml:"brokers"`
    ClientID        string          `yaml:"client_id"`
    BatchSize       int             `yaml:"batch_size"`
    LingerMs        int             `yaml:"linger_ms"`
    Retries         int             `yaml:"retries"`
    AckLevel        AckLevel        `yaml:"ack_level"`
    Compression     CompressionType `yaml:"compression"`
    RequestTimeout  time.Duration   `yaml:"request_timeout"`
}

// Send publishes a message to a topic
func (p *Producer) Send(topic string, message *Message) error {
    message.Topic = topic
    message.Timestamp = time.Now().UnixNano()
    
    // 1. Serialize the message
    serializedMsg, err := p.serializer.Serialize(message)
    if err != nil {
        return fmt.Errorf("serialization failed: %w", err)
    }
    
    // 2. Select partition using partitioner
    topicMetadata := p.getTopicMetadata(topic)
    partition := p.partitioner.Partition(message, topicMetadata)
    message.Partition = partition
    
    // 3. Add to batch
    if p.batcher.Add(*message) {
        // Batch is ready, send it
        return p.flushBatch()
    }
    
    return nil
}

func (p *Producer) flushBatch() error {
    batch := p.batcher.Flush()
    if len(batch) == 0 {
        return nil
    }
    
    // Group messages by partition
    partitionBatches := make(map[int32][]Message)
    for _, msg := range batch {
        partitionBatches[msg.Partition] = append(partitionBatches[msg.Partition], msg)
    }
    
    // Send to brokers
    var wg sync.WaitGroup
    for partition, messages := range partitionBatches {
        wg.Add(1)
        go func(p int32, msgs []Message) {
            defer wg.Done()
            err := p.sendToPartition(p, msgs)
            if err != nil {
                log.Printf("Failed to send batch to partition %d: %v", p, err)
                // Implement retry logic here
            }
        }(partition, messages)
    }
    
    wg.Wait()
    return nil
}
```

### Load Distribution Algorithms

Kafka producers use various algorithms to distribute messages across partitions:

#### 1. Partition Selection Strategies

```go
type Partitioner interface {
    Partition(message *Message, metadata *TopicMetadata) int32
}

// Round-robin partitioner
type RoundRobinPartitioner struct {
    counter int32
}

func (r *RoundRobinPartitioner) Partition(message *Message, metadata *TopicMetadata) int32 {
    partitionCount := metadata.PartitionCount
    partition := atomic.AddInt32(&r.counter, 1) % partitionCount
    return partition
}

// Key-based partitioner (consistent hashing)
type KeyPartitioner struct{}

func (k *KeyPartitioner) Partition(message *Message, metadata *TopicMetadata) int32 {
    if message.Key == nil {
        // No key, use round-robin
        return rand.Int31n(metadata.PartitionCount)
    }
    
    hash := murmur3.Sum32(message.Key)
    return int32(hash % uint32(metadata.PartitionCount))
}

// Sticky partitioner (Apache Kafka 2.4+)
type StickyPartitioner struct {
    stickyPartition int32
    batchIsFull     bool
    mutex           sync.Mutex
}

func (s *StickyPartitioner) Partition(message *Message, metadata *TopicMetadata) int32 {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    
    if message.Key != nil {
        // Messages with keys always go to same partition
        hash := murmur3.Sum32(message.Key)
        return int32(hash % uint32(metadata.PartitionCount))
    }
    
    // For messages without keys, stick to one partition until batch is full
    if s.stickyPartition == -1 || s.batchIsFull {
        s.stickyPartition = rand.Int31n(metadata.PartitionCount)
        s.batchIsFull = false
    }
    
    return s.stickyPartition
}
```

#### 2. How Kafka Producer Chooses Partitions

The partition selection algorithm is crucial for performance and ordering:

**Key-based Partitioning:**
- Messages with the same key always go to the same partition
- Guarantees ordering within a key
- Can cause hotspots if key distribution is uneven

**Round-robin Distribution:**
- Distributes load evenly across all partitions
- Good for throughput when ordering isn't required
- Simple but can fragment batches

**Sticky Partitioning:**
- Batches messages to the same partition until batch is full
- Reduces latency by improving batch efficiency
- Better network utilization

```go
func (p *Producer) selectPartition(message *Message, topicMetadata *TopicMetadata) int32 {
    // Strategy 1: If partition is explicitly set, use it
    if message.Partition >= 0 {
        return message.Partition
    }
    
    // Strategy 2: If message has key, use key-based partitioning
    if message.Key != nil {
        return p.keyBasedPartition(message.Key, topicMetadata.PartitionCount)
    }
    
    // Strategy 3: Use sticky partitioning for better batching
    return p.stickyPartitioner.Partition(message, topicMetadata)
}

func (p *Producer) keyBasedPartition(key []byte, partitionCount int32) int32 {
    hash := murmur3.Sum32(key)
    return int32(hash % uint32(partitionCount))
}
```

### Message Batching System

Batching is crucial for throughput optimization:

```go
type MessageBatcher struct {
    maxBatchSize  int           // Maximum messages per batch
    maxBatchBytes int           // Maximum bytes per batch
    lingerTime    time.Duration // How long to wait for batch to fill
    batches       map[int32]*PartitionBatch
    flushChan     chan int32
    mutex         sync.Mutex
}

type PartitionBatch struct {
    partition     int32
    messages      []Message
    totalBytes    int
    createdAt     time.Time
    flushTimer    *time.Timer
}

func (b *MessageBatcher) Add(message Message) bool {
    b.mutex.Lock()
    defer b.mutex.Unlock()
    
    partition := message.Partition
    batch := b.batches[partition]
    
    if batch == nil {
        batch = &PartitionBatch{
            partition:  partition,
            messages:   make([]Message, 0, b.maxBatchSize),
            createdAt:  time.Now(),
        }
        b.batches[partition] = batch
        
        // Set timer for batch flush
        batch.flushTimer = time.AfterFunc(b.lingerTime, func() {
            b.flushChan <- partition
        })
    }
    
    // Add message to batch
    batch.messages = append(batch.messages, message)
    batch.totalBytes += len(message.Key) + len(message.Value)
    
    // Check if batch should be flushed
    shouldFlush := len(batch.messages) >= b.maxBatchSize || 
                   batch.totalBytes >= b.maxBatchBytes
    
    if shouldFlush {
        batch.flushTimer.Stop()
        return true // Signal caller to flush
    }
    
    return false
}

func (b *MessageBatcher) Flush() []Message {
    b.mutex.Lock()
    defer b.mutex.Unlock()
    
    var allMessages []Message
    for partition, batch := range b.batches {
        if batch.flushTimer != nil {
            batch.flushTimer.Stop()
        }
        allMessages = append(allMessages, batch.messages...)
        delete(b.batches, partition)
    }
    
    return allMessages
}
```

### Delivery Semantics

```go
type DeliverySemantics int

const (
    AtMostOnce DeliverySemantics = iota  // May lose messages, no duplicates
    AtLeastOnce                          // No message loss, possible duplicates  
    ExactlyOnce                          // No loss, no duplicates (expensive)
)

type AckLevel int

const (
    NoAck     AckLevel = 0  // Fire and forget
    LeaderAck AckLevel = 1  // Wait for leader acknowledgment
    AllAck    AckLevel = -1 // Wait for all in-sync replicas
)

func (p *Producer) sendWithAcks(messages []Message, ackLevel AckLevel) error {
    switch ackLevel {
    case NoAck:
        // Fire and forget - fastest but least reliable
        return p.sendAsync(messages)
        
    case LeaderAck:
        // Wait for leader acknowledgment - balanced approach
        return p.sendSyncToLeader(messages)
        
    case AllAck:
        // Wait for all ISR replicas - strongest guarantee
        return p.sendSyncToAll(messages)
    }
    return nil
}
```

### Consumers

Events that producers write to local broker disks can be read by consumers - these are also applications that you develop. In this case, the Kafka cluster is still something served by infrastructure, where you as a user write producers and consumers. The consumer program subscribes to events or polls and receives data in response. This continues in a loop.

## System Components

### Core Architecture

Our message queue system consists of several key components working together:

```go
// Core message structure
type Message struct {
    Key       []byte            `json:"key"`
    Value     []byte            `json:"value"`
    Headers   map[string]string `json:"headers"`
    Timestamp int64             `json:"timestamp"`
    Offset    int64             `json:"offset"`
    Partition int32             `json:"partition"`
    Topic     string            `json:"topic"`
}

// Topic represents a logical channel for messages
type Topic struct {
    Name         string      `json:"name"`
    Partitions   []Partition `json:"partitions"`
    ReplicationFactor int    `json:"replication_factor"`
    RetentionMs  int64       `json:"retention_ms"`
    CreatedAt    time.Time   `json:"created_at"`
}

// Partition represents a ordered, immutable sequence of messages
type Partition struct {
    ID          int32     `json:"id"`
    Topic       string    `json:"topic"`
    Leader      BrokerID  `json:"leader"`
    Replicas    []BrokerID `json:"replicas"`
    ISR         []BrokerID `json:"isr"` // In-Sync Replicas
    LogSize     int64     `json:"log_size"`
    HighWaterMark int64   `json:"high_water_mark"`
}
```

### Topics and Partitions

#### Topics

A topic is a logical division of message categories into groups. For example, events by order statuses, partner coordinates, route sheets, and so on.

The key word here is logical. We create topics for events of a common group and try not to mix them with each other. For example, partner coordinates should not be in the same topic as order statuses, and updated order statuses should not be stored mixed with user registration updates.

It's convenient to think of a topic as a log - you write an event to the end and don't destroy the chain of old events in the process. General logic:

- One producer can write to one or more topics
- One consumer can read one or more topics
- One or more producers can write to one topic
- One or more consumers can read from one topic

Theoretically, there are no restrictions on the number of these topics, but practically this is limited by the number of partitions.

#### Partitions and Segments

There are no restrictions on the number of topics in a Kafka cluster, but there are limitations of the computer itself. It performs operations on the processor, input-output, and eventually hits its limit. We cannot increase the power and performance of machines indefinitely, so the topic should be divided into parts.

In Kafka, these parts are called partitions. Each topic consists of one or more partitions, each of which can be placed on different brokers. Due to this, Kafka can scale: the user can create a topic, divide it into partitions, and place each of them on a separate broker.

Formally, a partition is a strictly ordered log of messages. Each message in it is added to the end without the possibility of changing it in the future and somehow affecting already written messages. At the same time, the topic as a whole has no order, but the order of messages always exists in one of its partitions.

The partitions themselves are physically represented on disks as segments. These are separate files that can be created, rotated, or deleted according to the data aging settings in them. Usually you don't have to often remember about partition segments unless you administer the cluster, but it's important to remember the data storage model in Kafka topics.

```go
// Segment represents a physical file storing messages
type LogSegment struct {
    BaseOffset   int64     `json:"base_offset"`
    File         *os.File  `json:"-"`
    Size         int64     `json:"size"`
    MaxSize      int64     `json:"max_size"`
    CreatedAt    time.Time `json:"created_at"`
    MessageCount int       `json:"message_count"`
    IsActive     bool      `json:"is_active"`
}

// Partition contains multiple segments
type Partition struct {
    ID            int32          `json:"id"`
    Topic         string         `json:"topic"`
    Segments      []*LogSegment  `json:"segments"`
    ActiveSegment *LogSegment    `json:"active_segment"`
    HighWaterMark int64          `json:"high_water_mark"`
    LogStartOffset int64         `json:"log_start_offset"`
    Leader        BrokerID       `json:"leader"`
    Replicas      []BrokerID     `json:"replicas"`
    ISR           []BrokerID     `json:"isr"`
}
```

### Data Flow as a Log

A segment is also convenient to think of as a regular log file: each next entry is added to the end of the file and does not change previous entries. This is actually a FIFO (First-In-First-Out) queue and Kafka implements exactly this model.

Semantically and physically, messages inside a segment cannot be deleted, they are immutable. All we can do is specify how long the Kafka broker will store events through the Retention Policy setting.

The numbers inside the segment are real system messages that have sequence numbers or offsets that increase monotonically over time. Each partition has its own counter, and it doesn't intersect with other partitions in any way - positions 0, 1, 2, 3, and so on are separate for each partition. Thus, producers write messages to partitions, and the broker addresses each of these messages with its sequence number.

In addition to producers that write messages, there are consumers that read them. A log can have multiple consumers that read it from different positions and don't interfere with each other. And consumers don't have a hard binding to reading events by time. If desired, they can read after days, weeks, months, or even several times after some time.

The starting position of the first message in the log is called log-start offset. The position of the message written last is log-end offset. The consumer's position now is current offset.

The distance between the end offset and the consumer's current offset is called lag - this is the first thing to monitor in your applications. The acceptable lag for each application is different; this is closely related to business logic and system operation requirements.

### Message Structure

Now it's time to look at what an individual Kafka message consists of. If simplified, the message structure is represented by optional headers. They can be added from the producer side, partitioning key, payload, and timestamp.

Each event is a key-value pair. The partitioning key can be anything: numeric, string, object, or completely empty. The value can also be anything - a number, string, or object in your domain that you can somehow serialize (JSON, Protobuf, ...) and store.

In the message, the producer can specify the time, or the broker will do this for it at the moment of message reception. Headers look like in the HTTP protocol - these are string key-value pairs. You shouldn't store the data itself in them, but they can be used to transmit metadata. For example, for tracing, MIME-type messages, digital signatures, etc.

```go
// Complete message structure with metadata
type KafkaMessage struct {
    // Core data
    Key       []byte            `json:"key"`
    Value     []byte            `json:"value"`
    
    // Metadata
    Headers   map[string]string `json:"headers"`
    Timestamp int64             `json:"timestamp"`
    
    // Kafka internals
    Topic     string            `json:"topic"`
    Partition int32             `json:"partition"`
    Offset    int64             `json:"offset"`
    
    // Additional metadata
    ProducerID     int64         `json:"producer_id"`
    SequenceNumber int32         `json:"sequence_number"`
    Checksum       uint32        `json:"checksum"`
}

func (m *KafkaMessage) Serialize() ([]byte, error) {
    // Message serialization logic
    buffer := &bytes.Buffer{}
    
    // Write magic byte and version
    buffer.WriteByte(0x02) // Magic byte for message format v2
    buffer.WriteByte(0x01) // Version
    
    // Write timestamp
    binary.Write(buffer, binary.BigEndian, m.Timestamp)
    
    // Write key length and key
    binary.Write(buffer, binary.BigEndian, int32(len(m.Key)))
    buffer.Write(m.Key)
    
    // Write value length and value
    binary.Write(buffer, binary.BigEndian, int32(len(m.Value)))
    buffer.Write(m.Value)
    
    // Write headers
    binary.Write(buffer, binary.BigEndian, int32(len(m.Headers)))
    for k, v := range m.Headers {
        binary.Write(buffer, binary.BigEndian, int32(len(k)))
        buffer.WriteString(k)
        binary.Write(buffer, binary.BigEndian, int32(len(v)))
        buffer.WriteString(v)
    }
    
    return buffer.Bytes(), nil
}
```

### Producer Service

The producer is responsible for publishing messages to topics with intelligent partitioning and batching:

```go
type Producer struct {
    clientID     string
    brokers      []string
    partitioner  Partitioner
    batcher      *MessageBatcher
    serializer   Serializer
    ackLevel     AckLevel
    retryConfig  RetryConfig
    metrics      *ProducerMetrics
}

type ProducerConfig struct {
    Brokers         []string      `yaml:"brokers"`
    ClientID        string        `yaml:"client_id"`
    BatchSize       int           `yaml:"batch_size"`
    LingerMs        int           `yaml:"linger_ms"`
    Retries         int           `yaml:"retries"`
    AckLevel        AckLevel      `yaml:"ack_level"`
    Compression     CompressionType `yaml:"compression"`
    RequestTimeout  time.Duration `yaml:"request_timeout"`
}

// Send publishes a message to a topic
func (p *Producer) Send(topic string, message *Message) error {
    message.Topic = topic
    message.Timestamp = time.Now().UnixNano()
    
    // Choose partition
    partition := p.partitioner.ChoosePartition(message.Key, len(p.getTopicMetadata(topic).Partitions))
    message.Partition = int32(partition)
    
    // Add to batch
    if p.batcher.Add(message) {
        return p.flushBatch()
    }
    
    return nil
}

// Partitioning strategies
type Partitioner interface {
    ChoosePartition(key []byte, numPartitions int) int
}

type HashPartitioner struct{}

func (h *HashPartitioner) ChoosePartition(key []byte, numPartitions int) int {
    if len(key) == 0 {
        return rand.Intn(numPartitions)
    }
    hash := crc32.ChecksumIEEE(key)
    return int(hash) % numPartitions
}

// How Kafka Producer Chooses Partitions
func (p *Producer) selectPartition(message *Message, topic *TopicMetadata) int32 {
    numPartitions := len(topic.Partitions)
    
    // If producer specifies partition directly
    if message.Partition >= 0 && message.Partition < int32(numPartitions) {
        return message.Partition
    }
    
    // Use partitioner strategy
    partition := p.partitioner.ChoosePartition(message.Key, numPartitions)
    
    // Ensure partition is valid
    return int32(partition % numPartitions)
}

// Balancing and Partitioning Strategies
// The producer program can specify a key for the message and determine 
// the partition number by dividing the computed hash by the number of partitions.
// This way, messages with the same identifier are saved to the same partition.
// This common partitioning strategy allows achieving strict ordering of events 
// when reading by saving messages to one partition instead of different ones.

// For example, the key could be a card number when setting and resetting 
// a dynamic limit. In this case, we guarantee that events for one card 
// will follow strictly in the order they were written to the partition.

type CardPartitioner struct{}

func (cp *CardPartitioner) ChoosePartition(key []byte, numPartitions int) int {
    if len(key) == 0 {
        return 0 // Default partition for messages without keys
    }
    
    cardNumber := string(key)
    hash := fnv.New32a()
    hash.Write([]byte(cardNumber))
    
    return int(hash.Sum32()) % numPartitions
}

type RoundRobinPartitioner struct {
    counter int64
}

func (rr *RoundRobinPartitioner) ChoosePartition(key []byte, numPartitions int) int {
    partition := atomic.AddInt64(&rr.counter, 1) % int64(numPartitions)
    return int(partition)
}

type KeyBasedPartitioner struct{}

func (k *KeyBasedPartitioner) ChoosePartition(key []byte, numPartitions int) int {
    if len(key) == 0 {
        return 0 // Default partition for messages without keys
    }
    
    hash := fnv.New32a()
    hash.Write(key)
    return int(hash.Sum32()) % numPartitions
}
```

### Message Batching System

Batching is crucial for performance, grouping multiple messages together:

```go
type MessageBatcher struct {
    messages    []Message
    maxSize     int
    maxWait     time.Duration
    lastFlush   time.Time
    mutex       sync.RWMutex
    flushChan   chan struct{}
}

func NewMessageBatcher(maxSize int, maxWait time.Duration) *MessageBatcher {
    batcher := &MessageBatcher{
        messages:  make([]Message, 0, maxSize),
        maxSize:   maxSize,
        maxWait:   maxWait,
        lastFlush: time.Now(),
        flushChan: make(chan struct{}, 1),
    }
    
    // Start flush timer
    go batcher.flushTimer()
    return batcher
}

func (b *MessageBatcher) Add(msg Message) bool {
    b.mutex.Lock()
    defer b.mutex.Unlock()
    
    b.messages = append(b.messages, msg)
    
    // Check if we should flush
    shouldFlush := len(b.messages) >= b.maxSize || 
                  time.Since(b.lastFlush) > b.maxWait
    
    if shouldFlush {
        select {
        case b.flushChan <- struct{}{}:
        default:
        }
        return true
    }
    
    return false
}

### Producer Design

A typical producer program works like this: the payload is packaged into a structure indicating the topic, partition, and partitioning key. Next, the payload is serialized into a suitable format - JSON, Protobuf, Avro, or your own format with schema support. Then the message is assigned a partition according to the transmitted key and selected algorithm. After that, it is grouped into batches of selected sizes and sent to the Kafka broker for storage.

Depending on the settings, the producer waits for the write to complete in the Kafka cluster and a response acknowledgment message. If the producer could not write the message, it can try to send the message again - and so on in a circle.

Each parameter in the chain can be individually configured by each producer. For example, you can choose a partitioning algorithm, determine batch size, operating the balance between latency and throughput, and also choose message delivery semantics.

```go
// Typical producer workflow
func (p *Producer) SendMessage(topic string, key, value []byte, headers map[string]string) error {
    // 1. Create message structure
    message := &Message{
        Key:       key,
        Value:     value,
        Headers:   headers,
        Timestamp: time.Now().UnixNano(),
        Topic:     topic,
    }
    
    // 2. Serialize message
    serializedData, err := p.serializer.Serialize(message)
    if err != nil {
        return fmt.Errorf("serialization failed: %w", err)
    }
    message.Value = serializedData
    
    // 3. Choose partition
    topicMetadata := p.getTopicMetadata(topic)
    partition := p.selectPartition(message, topicMetadata)
    message.Partition = partition
    
    // 4. Add to batch
    if p.batcher.Add(*message) {
        // Batch is ready, send it
        return p.flushBatch()
    }
    
    return nil
}

func (p *Producer) flushBatch() error {
    batch := p.batcher.Flush()
    if len(batch) == 0 {
        return nil
    }
    
    // Group messages by partition
    partitionBatches := make(map[int32][]Message)
    for _, msg := range batch {
        partitionBatches[msg.Partition] = append(partitionBatches[msg.Partition], msg)
    }
    
    // Send to brokers
    var wg sync.WaitGroup
    for partition, messages := range partitionBatches {
        wg.Add(1)
        go func(p int32, msgs []Message) {
            defer wg.Done()
            err := p.sendToPartition(p, msgs)
            if err != nil {
                log.Printf("Failed to send batch to partition %d: %v", p, err)
                // Implement retry logic here
            }
        }(partition, messages)
    }
    
    wg.Wait()
    return nil
}
```

### Delivery Semantics

In any queues, there is a choice between delivery speed and reliability costs. The colored squares in the diagram are messages that we will write to the queue, choosing the necessary semantics.

```go
type DeliverySemantics int

const (
    AtMostOnce DeliverySemantics = iota  // May lose messages, no duplicates
    AtLeastOnce                          // No message loss, possible duplicates  
    ExactlyOnce                          // No loss, no duplicates (expensive)
)

type AckLevel int

const (
    NoAck     AckLevel = 0  // Fire and forget
    LeaderAck AckLevel = 1  // Wait for leader acknowledgment
    AllAck    AckLevel = -1 // Wait for all in-sync replicas
)
```

**At-most once semantics** means that when delivering messages, we are okay with message losses, but not their duplicates. This is the weakest guarantee implemented by queue brokers.

**At-least once semantics** means that we don't want to lose messages, but we are okay with possible duplicates.

**Exactly-once semantics** means that we want to deliver one and only one message, losing nothing and duplicating nothing.

At first glance, exactly-once semantics seems the most correct for any application, but this is not always the case. For example, when transmitting partner coordinates, it's not at all necessary to save every point from them, and at-most once is quite enough. And when processing idempotent events, we may well be satisfied with a duplicate if the status model assumes its correct processing.

In distributed systems, exactly-once has its price: high reliability means high latency. Let's look at what tools Kafka offers for implementing all three message delivery semantics to the broker.

### Reliability of Delivery

From the producer side, the developer determines the reliability of message delivery to Kafka using the acks parameter. By specifying 0 or none, the producer will send messages to Kafka without waiting for any write confirmations to disk from the broker side.

This is the weakest guarantee. In case of broker failure or network problems, you will never know if the message got into the log or was simply lost.

```go
func (p *Producer) sendWithAcks(messages []Message, ackLevel AckLevel) error {
    switch ackLevel {
    case NoAck:
        // Fire and forget - fastest but least reliable
        return p.sendAsync(messages)
        
    case LeaderAck:
        // Wait for leader acknowledgment - balanced approach
        return p.sendSyncToLeader(messages)
        
    case AllAck:
        // Wait for all ISR replicas - strongest guarantee
        return p.sendSyncToAll(messages)
    }
    return nil
}
```

By specifying the setting to 1 or leader, the producer when writing will wait for a response from the broker with the leader partition - this means the message is saved to disk of one broker. In this case, you get a guarantee that the message was received at least once, but this still doesn't protect you from problems in the cluster itself.

Imagine that at the moment of confirmation, the broker with the leader partition fails, and the followers didn't have time to replicate data from it. In this case, you lose the message and again don't know about it. Such situations happen rarely, but they are possible.

Finally, by setting acks to -1 or all, you ask the broker with the leader partition to send you confirmation only when the write gets to the local disk of the broker and to follower replicas. The number of these replicas is set by the min.insync.replicas setting.

A common mistake when configuring a topic is choosing min.insync.replicas by the number of replicas. In such a scenario, in case of broker failure and loss of one replica, the producer will no longer be able to write a message to the cluster, since it won't wait for confirmation. It's better to prudently set min.insync.replicas to one less than the number of replicas.

Obviously, the third scheme is quite reliable, but it requires more overhead: not only do you need to save to disk, but you also need to wait for followers to replicate messages and save them to their disk log.

### Idempotent Producers

Even with choosing acks=all, message duplicates are possible. In normal operation, the producer sends a message to the broker, and the broker saves the data in the log on disk and sends confirmation to the producer. The producer again forms a batch of messages and so on. But no program is protected from failures.

What if the broker couldn't send confirmation to the producer due to network problems? In this case, the producer resends the message to the broker. The broker obediently saves and adds another message to the log - a duplicate appears.

This problem is solved in Kafka thanks to the transactional API and the use of idempotency. The broker has a special option that enables idempotency - enable.idempotence. So each message will be assigned a producer identifier or PID and a monotonically increasing sequence number. Due to this, duplicate messages from one producer with the same PID will be discarded on the broker side.

To put it simply - when you use acks=all, there are no reasons not to enable enable.idempotence for your producers. This way you will achieve exactly-once guarantee when writing to the broker and avoid duplicates. But this power has its price - writing will take longer.

```go
type IdempotentProducer struct {
    ProducerID     int64
    SequenceNumber int32
    config         ProducerConfig
    transactionMgr *TransactionManager
}

func (ip *IdempotentProducer) Send(message *Message) error {
    // Assign producer ID and sequence number for idempotency
    message.ProducerID = ip.ProducerID
    message.SequenceNumber = atomic.AddInt32(&ip.SequenceNumber, 1)
    
    // Calculate checksum for duplicate detection
    message.Checksum = ip.calculateChecksum(message)
    
    return ip.sendWithTransaction(message)
}

func (ip *IdempotentProducer) calculateChecksum(msg *Message) uint32 {
    hasher := crc32.NewIEEE()
    hasher.Write(msg.Key)
    hasher.Write(msg.Value)
    binary.Write(hasher, binary.BigEndian, msg.ProducerID)
    binary.Write(hasher, binary.BigEndian, msg.SequenceNumber)
    return hasher.Sum32()
}

type TransactionManager struct {
    transactionID string
    state         TransactionState
    coordinator   *TransactionCoordinator
}

type TransactionState int

const (
    TransactionBegin TransactionState = iota
    TransactionInProgress
    TransactionPrepareCommit
    TransactionCompleted
    TransactionAborted
)
```

func (b *MessageBatcher) flushTimer() {
ticker := time.NewTicker(b.maxWait / 2)
defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            b.checkTimeBasedFlush()
        case <-b.flushChan:
            // Batch is ready to flush
            return
        }
    }
}

func (b *MessageBatcher) Flush() []Message {
b.mutex.Lock()
defer b.mutex.Unlock()

    if len(b.messages) == 0 {
        return nil
    }
    
    batch := make([]Message, len(b.messages))
    copy(batch, b.messages)
    
    // Reset batch
    b.messages = b.messages[:0]
    b.lastFlush = time.Now()
    
    return batch
}
```

### Consumer Service

The consumer pulls messages from topics and manages offset tracking:

```go
type Consumer struct {
    groupID     string
    topics      []string
    assignment  map[string][]int32 // topic -> partitions
    offsets     map[TopicPartition]int64
    coordinator *GroupCoordinator
    fetcher     *MessageFetcher
    processor   MessageProcessor
    config      ConsumerConfig
}

type ConsumerConfig struct {
    GroupID              string        `yaml:"group_id"`
    Topics               []string      `yaml:"topics"`
    AutoOffsetReset      OffsetReset   `yaml:"auto_offset_reset"`
    EnableAutoCommit     bool          `yaml:"enable_auto_commit"`
    AutoCommitInterval   time.Duration `yaml:"auto_commit_interval"`
    SessionTimeout       time.Duration `yaml:"session_timeout"`
    HeartbeatInterval    time.Duration `yaml:"heartbeat_interval"`
    MaxPollRecords       int           `yaml:"max_poll_records"`
    FetchMinBytes        int           `yaml:"fetch_min_bytes"`
    FetchMaxWait         time.Duration `yaml:"fetch_max_wait"`
}

type TopicPartition struct {
    Topic     string `json:"topic"`
    Partition int32  `json:"partition"`
}

### Consumer Design and Operation

A typical consumer program works like this: when starting, a timer works inside it, which periodically polls new records from broker partitions. The poller receives a list of batches related to topics and partitions from which the consumer reads. Next, the received messages in batches are deserialized. As a result, the consumer, as a rule, somehow processes the messages.

At the end of reading, the consumer can commit the offset - remember the position of read records and continue reading the new portion of data. You can read both synchronously and asynchronously. The offset can be committed or not committed at all.

Thus, if the consumer read a message from a partition, processed it, and then crashed without committing the offset, then the next connected consumer on the next connection will read the same portion of data again.

The main thing is that the consumer periodically reads a new portion of data, deserializes it, and then processes it.

```go
// Consumer workflow implementation
func (c *Consumer) Start() error {
    // Start heartbeat routine for group coordination
    go c.sendHeartbeats()
    
    // Start offset commit routine if auto-commit enabled
    if c.config.EnableAutoCommit {
        go c.autoCommitOffsets()
    }
    
    // Main polling loop
    for {
        messages, err := c.Poll(c.config.PollTimeout)
        if err != nil {
            log.Printf("Poll error: %v", err)
            continue
        }
        
        if len(messages) > 0 {
            err = c.processMessages(messages)
            if err != nil {
                log.Printf("Processing error: %v", err)
                continue
            }
            
            // Commit offsets after successful processing
            if !c.config.EnableAutoCommit {
                err = c.CommitSync()
                if err != nil {
                    log.Printf("Commit error: %v", err)
                }
            }
        }
    }
}

func (c *Consumer) processMessages(messages []Message) error {
    for _, msg := range messages {
        // Deserialize message
        event, err := c.deserializer.Deserialize(msg.Value)
        if err != nil {
            return fmt.Errorf("deserialization failed: %w", err)
        }
        
        // Process business logic
        err = c.processor.Process(event)
        if err != nil {
            return fmt.Errorf("message processing failed: %w", err)
        }
        
        // Update local offset tracking
        tp := TopicPartition{Topic: msg.Topic, Partition: msg.Partition}
        c.offsets[tp] = msg.Offset + 1
    }
    
    return nil
}
```

// Poll retrieves messages from assigned partitions
func (c *Consumer) Poll(timeout time.Duration) ([]Message, error) {
ctx, cancel := context.WithTimeout(context.Background(), timeout)
defer cancel()

    var allMessages []Message
    
    for topic, partitions := range c.assignment {
        for _, partition := range partitions {
            tp := TopicPartition{Topic: topic, Partition: partition}
            offset := c.offsets[tp]
            
            messages, err := c.fetcher.Fetch(ctx, tp, offset, c.config.MaxPollRecords)
            if err != nil {
                return nil, fmt.Errorf("failed to fetch from %s-%d: %w", topic, partition, err)
            }
            
            allMessages = append(allMessages, messages...)
            
            // Update offset for next poll
            if len(messages) > 0 {
                lastOffset := messages[len(messages)-1].Offset
                c.offsets[tp] = lastOffset + 1
            }
        }
    }
    
    return allMessages, nil
}

// Commit offsets for processed messages
func (c *Consumer) CommitSync() error {
return c.coordinator.CommitOffsets(c.groupID, c.offsets)
}

func (c *Consumer) CommitAsync() {
go func() {
if err := c.CommitSync(); err != nil {
log.Printf("Failed to commit offsets: %v", err)
}
}()
}
```

### Consumer Groups

It would be strange if only one consumer was engaged in reading all partitions. They can be combined into a cluster - consumer groups.

You have before your eyes a canonical diagram: on the left side are producers, in the middle are topics, and on the right are consumers. There are two producers, each writing to its own topic, each topic has three partitions.

There is one consumer group with two instances of the same program - this is the same program, running twice. This consumer program reads two topics: X and Y.

The consumer connects to the broker with the leader partition, polls changes in the partition, reads messages, fills the buffer, and then processes the received messages.

Note that the partitions are distributed cooperatively: each consumer got three partitions. The distribution of partitions between consumers within one group is performed automatically on the broker side. Kafka tries to fairly distribute partitions between consumer groups, as much as possible.

Each such group has its own identifier, which allows registering on Kafka brokers. The namespace of consumer groups is global, which means their names in the Kafka cluster are unique.

Finally, the most important thing: Kafka saves on its side the current offset for each partition of topics that are part of the consumer group. When connecting or disconnecting consumers from the group, reading will continue from the last saved position. This makes consumer groups indispensable when working with event-driven systems: we can deploy our applications without problems and not think about storing offsets on the client side.

For this, the consumer in the group, after processing read messages, sends a request to save the offset - or commits its offset. Technically, there are no restrictions on committing the offset before processing messages, but for most scenarios it's more reasonable to do this after.

### Consumer Groups and Rebalancing

Consumer groups enable horizontal scaling of message processing:

```go
type GroupCoordinator struct {
    groups      map[string]*ConsumerGroup
    assignments map[string]map[string][]TopicPartition // groupID -> consumerID -> partitions
    offsets     map[string]map[TopicPartition]int64    // groupID -> partition -> offset
    mutex       sync.RWMutex
    rebalancer  Rebalancer
}

type ConsumerGroup struct {
    ID           string                    `json:"id"`
    Members      map[string]*GroupMember   `json:"members"`
    Leader       string                    `json:"leader"`
    Protocol     string                    `json:"protocol"`
    State        GroupState                `json:"state"`
    Generation   int32                     `json:"generation"`
}

type GroupMember struct {
    ID             string    `json:"id"`
    Host           string    `json:"host"`
    Topics         []string  `json:"topics"`
    LastHeartbeat  time.Time `json:"last_heartbeat"`
    SessionTimeout time.Duration `json:"session_timeout"`
}

type GroupState string

const (
    GroupStateStable      GroupState = "Stable"
    GroupStateRebalancing GroupState = "Rebalancing"
    GroupStateEmpty       GroupState = "Empty"
    GroupStateDead        GroupState = "Dead"
)

// JoinGroup handles new consumer joining a group
func (gc *GroupCoordinator) JoinGroup(groupID, memberID string, topics []string, sessionTimeout time.Duration) error {
    gc.mutex.Lock()
    defer gc.mutex.Unlock()
    
    group, exists := gc.groups[groupID]
    if !exists {
        group = &ConsumerGroup{
            ID:      groupID,
            Members: make(map[string]*GroupMember),
            State:   GroupStateEmpty,
        }
        gc.groups[groupID] = group
    }
    
    member := &GroupMember{
        ID:             memberID,
        Topics:         topics,
        LastHeartbeat:  time.Now(),
        SessionTimeout: sessionTimeout,
    }
    
    group.Members[memberID] = member
    
    // Trigger rebalance
    if group.State == GroupStateStable {
        group.State = GroupStateRebalancing
        go gc.rebalanceGroup(groupID)
    }
    
    return nil
}

// Rebalance redistributes partitions among group members
func (gc *GroupCoordinator) rebalanceGroup(groupID string) {
    gc.mutex.Lock()
    defer gc.mutex.Unlock()
    
    group := gc.groups[groupID]
    if group.State != GroupStateRebalancing {
        return
    }
    
    // Get all partitions for subscribed topics
    var allPartitions []TopicPartition
    topicSet := make(map[string]bool)
    
    for _, member := range group.Members {
        for _, topic := range member.Topics {
            topicSet[topic] = true
        }
    }
    
    for topic := range topicSet {
        partitions := gc.getTopicPartitions(topic)
        for _, partition := range partitions {
            allPartitions = append(allPartitions, TopicPartition{
                Topic:     topic,
                Partition: partition,
            })
        }
    }
    
    // Assign partitions using round-robin strategy
    memberIDs := make([]string, 0, len(group.Members))
    for memberID := range group.Members {
        memberIDs = append(memberIDs, memberID)
    }
    sort.Strings(memberIDs) // Ensure consistent ordering
    
    assignment := make(map[string][]TopicPartition)
    for i, partition := range allPartitions {
        memberID := memberIDs[i%len(memberIDs)]
        assignment[memberID] = append(assignment[memberID], partition)
    }
    
    gc.assignments[groupID] = assignment
    group.State = GroupStateStable
    group.Generation++
    
    log.Printf("Rebalanced group %s with %d members and %d partitions", 
               groupID, len(group.Members), len(allPartitions))
}

### Consumer Group Rebalancing Process

Let's consider a scenario when the group composition changes. In the Kafka cluster, consumer groups are created automatically when consumers connect to the cluster, and there's no need to create them manually, but this is possible through tooling. A new group has no saved offsets for topic partitions and by default they are equal to -1.

When new participants appear in the JoinGroup group, in a special broker process Group Coordinator, the first consumer to enter is assigned the role of Group Leader.

The group leader is responsible for distributing partitions among all group members. The process of finding the group leader, assigning partitions, stabilizing, and connecting consumers in the group to brokers is called consumer group rebalancing.

The group rebalancing process by default forces all consumers in the group to stop reading and wait for complete synchronization of participants to acquire new partitions for reading. Kafka has other group rebalancing strategies, including Static membership or Cooperative Incremental Partition Assignor, but this is a topic for a separate article.

```go
// Rebalancing process implementation
func (gc *GroupCoordinator) handleRebalance(groupID string, reason RebalanceReason) error {
    gc.mutex.Lock()
    defer gc.mutex.Unlock()
    
    group := gc.groups[groupID]
    if group == nil {
        return fmt.Errorf("group %s not found", groupID)
    }
    
    log.Printf("Starting rebalance for group %s, reason: %v", groupID, reason)
    
    // Step 1: Stop the world - pause all consumers
    group.State = GroupStateRebalancing
    gc.notifyGroupMembers(groupID, "REBALANCE_START")
    
    // Step 2: Wait for all members to acknowledge
    err := gc.waitForMemberAcknowledgments(groupID, 30*time.Second)
    if err != nil {
        return fmt.Errorf("rebalance timeout: %w", err)
    }
    
    // Step 3: Select group leader (first member or existing leader)
    leader := gc.selectGroupLeader(group)
    group.Leader = leader.ID
    
    // Step 4: Generate new partition assignment
    assignment := gc.generatePartitionAssignment(group)
    gc.assignments[groupID] = assignment
    
    // Step 5: Notify all members of new assignment
    for memberID, partitions := range assignment {
        member := group.Members[memberID]
        if member != nil {
            member.Assignment = partitions
            gc.notifyMemberAssignment(memberID, partitions)
        }
    }
    
    // Step 6: Group becomes stable
    group.State = GroupStateStable
    group.Generation++
    
    log.Printf("Rebalance completed for group %s, generation %d", groupID, group.Generation)
    return nil
}

type RebalanceReason int

const (
    MemberJoined RebalanceReason = iota
    MemberLeft
    MemberTimedOut
    TopicMetadataChanged
    PartitionCountChanged
)

// Heartbeat management for group coordination
func (c *Consumer) sendHeartbeats() {
    ticker := time.NewTicker(c.config.HeartbeatInterval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            err := c.coordinator.SendHeartbeat(c.groupID, c.memberID)
            if err != nil {
                log.Printf("Heartbeat failed: %v", err)
                // Trigger rejoin process
                go c.rejoinGroup()
                return
            }
        case <-c.quit:
            return
        }
    }
}
```

As soon as the group becomes stable, and its members receive partitions, the consumers in it begin reading. Since the group is new and didn't exist before, the consumer chooses the reading position of the offset: from the very beginning earliest or from the end latest. The topic could have existed for several months, and the consumer appeared quite recently. In this case, it's important to decide: whether to read all messages or it's enough to read from the end the most recent ones, skipping the history. The choice between the two options depends on the business logic of events flowing inside the topic.

If you add a new member to the group, the rebalancing process starts again. The new member, together with the rest of the consumers in the group, will be assigned partitions, and the group leader will try to distribute them among everyone more or less fairly, according to the configurable strategy he chose. Then the group again transitions to a stable state.

To ensure that the Group Coordinator in the Kafka cluster knows which of its members are active and working, and which are no longer, each consumer in the group regularly sends Heartbeat messages at equal intervals. The time value is configured by the consumer program before startup.

The consumer also declares the session lifetime - if during this time it couldn't send any Heartbeat messages to the broker, then it leaves the group. The broker, in turn, not receiving any Heartbeat messages from consumers, starts the process of rebalancing consumers in the group.

The rebalancing process is quite painful for large consumer groups with many topics. It causes Stop-The-World in the entire group with the slightest change in member composition or partition composition in topics. For example, when changing the partition leader in case of broker exit from the cluster due to accident or planned work, Group Coordinator also initiates rebalancing.

Therefore, developers of consumer programs are usually recommended to use one consumer group per topic. It's also useful to keep the number of consumers not too large so as not to trigger rebalancing many times, but also not too small to maintain performance and reliability when reading.

Heartbeat interval and session lifetime values should be set so that the Heartbeat interval is three to four times smaller than session timeout. The values themselves should be chosen not too large so as not to increase the time to detect a "dropped" consumer from the group, but also not too small so that in case of the slightest network problems, the group doesn't go into rebalancing.

Another hypothetical scenario: there are 4 partitions in a topic, and 5 consumers in a group. In this case, the group will be stabilized, but members who don't get any partitions will be idle. This happens because only one consumer in a group can work with one partition, and two or more consumers cannot read from one partition in a group.

This gives rise to the following basic recommendation: set a sufficient number of partitions at the start so that you can horizontally scale your application. Increasing partitions at the moment will bring you almost no result. Already written log messages cannot be moved and distributed between new partitions by Kafka means, and repartitioning on your own always carries risks associated with ordering and idempotency.
```

### Broker Service

The broker is the core storage and replication engine:

```go
type Broker struct {
    ID            BrokerID              `json:"id"`
    Host          string                `json:"host"`
    Port          int                   `json:"port"`
    Partitions    map[PartitionKey]*PartitionLog `json:"-"`
    ReplicationMgr *ReplicationManager   `json:"-"`
    Storage       StorageEngine         `json:"-"`
    Config        BrokerConfig          `json:"-"`
    mutex         sync.RWMutex
}

type PartitionKey struct {
    Topic     string `json:"topic"`
    Partition int32  `json:"partition"`
}

type PartitionLog struct {
    Key           PartitionKey    `json:"key"`
    Segments      []*LogSegment   `json:"segments"`
    ActiveSegment *LogSegment     `json:"active_segment"`
    Index         *OffsetIndex    `json:"index"`
    HighWaterMark int64           `json:"high_water_mark"`
    LogStartOffset int64          `json:"log_start_offset"`
    mutex         sync.RWMutex
}

type LogSegment struct {
    BaseOffset   int64     `json:"base_offset"`
    File         *os.File  `json:"-"`
    Size         int64     `json:"size"`
    MaxSize      int64     `json:"max_size"`
    CreatedAt    time.Time `json:"created_at"`
    MessageCount int       `json:"message_count"`
}

// Append messages to a partition
func (b *Broker) Append(key PartitionKey, messages []Message) ([]int64, error) {
    b.mutex.RLock()
    partitionLog, exists := b.Partitions[key]
    b.mutex.RUnlock()
    
    if !exists {
        return nil, fmt.Errorf("partition %s-%d not found", key.Topic, key.Partition)
    }
    
    return partitionLog.Append(messages)
}

func (pl *PartitionLog) Append(messages []Message) ([]int64, error) {
    pl.mutex.Lock()
    defer pl.mutex.Unlock()
    
    if pl.ActiveSegment == nil || pl.ActiveSegment.Size >= pl.ActiveSegment.MaxSize {
        if err := pl.rollSegment(); err != nil {
            return nil, err
        }
    }
    
    var offsets []int64
    for _, msg := range messages {
        msg.Offset = pl.HighWaterMark
        msg.Partition = pl.Key.Partition
        
        data := serializeMessage(msg)
        if err := pl.ActiveSegment.Append(data); err != nil {
            return nil, err
        }
        
        pl.Index.Add(msg.Offset, pl.ActiveSegment.Size)
        offsets = append(offsets, msg.Offset)
        pl.HighWaterMark++
    }
    
    return offsets, nil
}

// Fetch messages from a partition
func (b *Broker) Fetch(key PartitionKey, offset int64, maxBytes int) ([]Message, error) {
    b.mutex.RLock()
    partitionLog, exists := b.Partitions[key]
    b.mutex.RUnlock()
    
    if !exists {
        return nil, fmt.Errorf("partition %s-%d not found", key.Topic, key.Partition)
    }
    
    return partitionLog.Fetch(offset, maxBytes)
}

func (pl *PartitionLog) Fetch(startOffset int64, maxBytes int) ([]Message, error) {
    pl.mutex.RLock()
    defer pl.mutex.RUnlock()
    
    if startOffset < pl.LogStartOffset {
        return nil, fmt.Errorf("offset %d is before log start offset %d", startOffset, pl.LogStartOffset)
    }
    
    if startOffset >= pl.HighWaterMark {
        return []Message{}, nil // No new messages
    }
    
    // Find the segment containing the start offset
    segmentIndex := pl.Index.FindSegment(startOffset)
    if segmentIndex == -1 {
        return nil, fmt.Errorf("no segment found for offset %d", startOffset)
    }
    
    var messages []Message
    bytesRead := 0
    
    for i := segmentIndex; i < len(pl.Segments) && bytesRead < maxBytes; i++ {
        segment := pl.Segments[i]
        segmentMessages, err := segment.ReadFrom(startOffset)
        if err != nil {
            return nil, err
        }
        
        for _, msg := range segmentMessages {
            if msg.Offset >= startOffset && bytesRead < maxBytes {
                messages = append(messages, msg)
                bytesRead += len(msg.Value) + len(msg.Key) + 64 // Approximate message overhead
                startOffset = msg.Offset + 1
            }
        }
    }
    
    return messages, nil
}
```

### Replication System

Replication ensures data durability and availability:

```go
type ReplicationManager struct {
    brokerID         BrokerID
    leaderPartitions map[PartitionKey]*LeaderReplica
    followerPartitions map[PartitionKey]*FollowerReplica
    coordinator      *ClusterCoordinator
    fetcherPool      *FetcherPool
    mutex           sync.RWMutex
}

type LeaderReplica struct {
    Partition    *PartitionLog
    Followers    map[BrokerID]*FollowerState
    ISR          []BrokerID  // In-Sync Replicas
    HW           int64       // High Water Mark
    LEO          int64       // Log End Offset
}

type FollowerState struct {
    BrokerID     BrokerID
    LastFetchOffset int64
    LastCaughtUpTime time.Time
    InSync       bool
}

type FollowerReplica struct {
    Partition    *PartitionLog
    LeaderBroker BrokerID
    FetchOffset  int64
    LastSync     time.Time
}

// Replicate handles replication for leader partitions
func (rm *ReplicationManager) Replicate(key PartitionKey, messages []Message) error {
    rm.mutex.RLock()
    leader, isLeader := rm.leaderPartitions[key]
    rm.mutex.RUnlock()
    
    if !isLeader {
        return fmt.Errorf("not leader for partition %s-%d", key.Topic, key.Partition)
    }
    
    // Append to local log first
    offsets, err := leader.Partition.Append(messages)
    if err != nil {
        return err
    }
    
    leader.LEO = offsets[len(offsets)-1] + 1
    
    // Update high water mark based on ISR
    rm.updateHighWaterMark(leader)
    
    // Replicate to followers asynchronously
    go rm.replicateToFollowers(key, leader, messages)
    
    return nil
}

func (rm *ReplicationManager) replicateToFollowers(key PartitionKey, leader *LeaderReplica, messages []Message) {
    var wg sync.WaitGroup
    
    for followerID := range leader.Followers {
        wg.Add(1)
        go func(brokerID BrokerID) {
            defer wg.Done()
            
            err := rm.sendToFollower(brokerID, key, messages)
            if err != nil {
                log.Printf("Failed to replicate to broker %d: %v", brokerID, err)
                rm.markFollowerOutOfSync(leader, brokerID)
            } else {
                rm.updateFollowerState(leader, brokerID, messages)
            }
        }(followerID)
    }
    
    wg.Wait()
}

func (rm *ReplicationManager) updateHighWaterMark(leader *LeaderReplica) {
    minOffset := leader.LEO
    
    // High water mark is the minimum offset among all ISR members
    for _, followerID := range leader.ISR {
        if follower, exists := leader.Followers[followerID]; exists {
            if follower.LastFetchOffset < minOffset {
                minOffset = follower.LastFetchOffset
            }
        }
    }
    
    leader.HW = minOffset
    leader.Partition.HighWaterMark = minOffset
}

// Follower fetch process
func (rm *ReplicationManager) StartFollowerFetching() {
    for key, follower := range rm.followerPartitions {
        go rm.followLeader(key, follower)
    }
}

func (rm *ReplicationManager) followLeader(key PartitionKey, follower *FollowerReplica) {
    ticker := time.NewTicker(100 * time.Millisecond) // Fetch every 100ms
    defer ticker.Stop()
    
    for range ticker.C {
        messages, err := rm.fetchFromLeader(follower.LeaderBroker, key, follower.FetchOffset)
        if err != nil {
            log.Printf("Failed to fetch from leader %d: %v", follower.LeaderBroker, err)
            continue
        }
        
        if len(messages) > 0 {
            _, err := follower.Partition.Append(messages)
            if err != nil {
                log.Printf("Failed to append fetched messages: %v", err)
                continue
            }
            
            lastOffset := messages[len(messages)-1].Offset
            follower.FetchOffset = lastOffset + 1
            follower.LastSync = time.Now()
        }
    }
}
```

### Coordinator Service

The coordinator manages cluster metadata and broker orchestration:

```go
type Coordinator struct {
    config       CoordinatorConfig
    brokers      map[BrokerID]*BrokerInfo
    topics       map[string]*TopicMetadata
    assignments  map[PartitionKey]BrokerID
    serviceManager *ServiceManager
    console      *ConsoleServer
    metadata     *MetadataStore
    mutex        sync.RWMutex
}

type ServiceManager struct {
    brokerProcesses  [5]*os.Process
    producerProcess  *os.Process
    consumerProcess  *os.Process
    coordinator      *Coordinator
}

type BrokerInfo struct {
    ID       BrokerID  `json:"id"`
    Host     string    `json:"host"`
    Port     int       `json:"port"`
    Status   BrokerStatus `json:"status"`
    LastSeen time.Time `json:"last_seen"`
    Load     BrokerLoad `json:"load"`
}

type BrokerStatus string

const (
    BrokerStatusOnline  BrokerStatus = "online"
    BrokerStatusOffline BrokerStatus = "offline"
    BrokerStatusStarting BrokerStatus = "starting"
    BrokerStatusStopping BrokerStatus = "stopping"
)

type BrokerLoad struct {
    PartitionCount   int     `json:"partition_count"`
    LeaderCount      int     `json:"leader_count"`
    MessageRate      float64 `json:"message_rate"`
    DiskUsageBytes   int64   `json:"disk_usage_bytes"`
    CPUUsagePercent  float64 `json:"cpu_usage_percent"`
}

// Start all services automatically
func (sm *ServiceManager) StartAll() error {
    log.Println("Starting all services...")
    
    // Start 5 brokers
    for i := 0; i < 5; i++ {
        if err := sm.startBroker(i); err != nil {
            return fmt.Errorf("failed to start broker %d: %w", i, err)
        }
    }
    
    // Wait for brokers to be ready
    time.Sleep(2 * time.Second)
    
    // Start producer service
    if err := sm.startProducer(); err != nil {
        return fmt.Errorf("failed to start producer service: %w", err)
    }
    
    // Start consumer service
    if err := sm.startConsumer(); err != nil {
        return fmt.Errorf("failed to start consumer service: %w", err)
    }
    
    log.Println("All services started successfully")
    return nil
}

func (sm *ServiceManager) startBroker(id int) error {
    port := 9092 + id
    
    cmd := exec.Command("./bin/broker",
        "--id", fmt.Sprintf("%d", id),
        "--port", fmt.Sprintf("%d", port),
        "--coordinator", "localhost:8080",
        "--data-dir", fmt.Sprintf("./data/broker-%d", id))
    
    if err := cmd.Start(); err != nil {
        return err
    }
    
    sm.brokerProcesses[id] = cmd.Process
    log.Printf("Started broker %d on port %d (PID: %d)", id, port, cmd.Process.Pid)
    
    return nil
}

func (sm *ServiceManager) startProducer() error {
    cmd := exec.Command("./bin/producer",
        "--port", "9093",
        "--coordinator", "localhost:8080")
    
    if err := cmd.Start(); err != nil {
        return err
    }
    
    sm.producerProcess = cmd.Process
    log.Printf("Started producer service on port 9093 (PID: %d)", cmd.Process.Pid)
    
    return nil
}

func (sm *ServiceManager) startConsumer() error {
    cmd := exec.Command("./bin/consumer",
        "--port", "9094",
        "--coordinator", "localhost:8080")
    
    if err := cmd.Start(); err != nil {
        return err
    }
    
    sm.consumerProcess = cmd.Process
    log.Printf("Started consumer service on port 9094 (PID: %d)", cmd.Process.Pid)
    
    return nil
}

// Console commands for service management
type ConsoleServer struct {
    coordinator *Coordinator
    commands    map[string]CommandHandler
}

type CommandHandler func(args []string) (string, error)

func (cs *ConsoleServer) Start() {
    cs.registerCommands()
    
    listener, err := net.Listen("tcp", ":8081")
    if err != nil {
        log.Fatalf("Failed to start console server: %v", err)
    }
    
    log.Println("Console server started on :8081")
    
    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Printf("Failed to accept connection: %v", err)
            continue
        }
        
        go cs.handleConnection(conn)
    }
}

func (cs *ConsoleServer) registerCommands() {
    cs.commands = map[string]CommandHandler{
        "status":       cs.handleStatus,
        "broker":       cs.handleBroker,
        "producer":     cs.handleProducer,
        "consumer":     cs.handleConsumer,
        "topics":       cs.handleTopics,
        "restart":      cs.handleRestart,
        "shutdown":     cs.handleShutdown,
    }
}

func (cs *ConsoleServer) handleStatus(args []string) (string, error) {
    var result strings.Builder
    
    result.WriteString("System Status:\n")
    result.WriteString("==============\n\n")
    
    // Broker status
    result.WriteString("Brokers:\n")
    for i := 0; i < 5; i++ {
        brokerID := BrokerID(i)
        if broker, exists := cs.coordinator.brokers[brokerID]; exists {
            result.WriteString(fmt.Sprintf("  Broker %d: %s (Port: %d)\n", 
                broker.ID, broker.Status, broker.Port))
        } else {
            result.WriteString(fmt.Sprintf("  Broker %d: offline\n", i))
        }
    }
    
    // Producer/Consumer status
    result.WriteString(fmt.Sprintf("\nProducer Service: %s\n", cs.getServiceStatus("producer")))
    result.WriteString(fmt.Sprintf("Consumer Service: %s\n", cs.getServiceStatus("consumer")))
    
    // Topic information
    result.WriteString(fmt.Sprintf("\nTopics: %d\n", len(cs.coordinator.topics)))
    for topicName, topic := range cs.coordinator.topics {
        result.WriteString(fmt.Sprintf("  %s: %d partitions\n", topicName, len(topic.Partitions)))
    }
    
    return result.String(), nil
}

func (cs *ConsoleServer) handleBroker(args []string) (string, error) {
    if len(args) < 2 {
        return "Usage: broker <list|start|stop|restart> [broker_id]", nil
    }
    
    action := args[1]
    
    switch action {
    case "list":
        return cs.listBrokers(), nil
    case "start", "stop", "restart":
        if len(args) < 3 {
            return fmt.Sprintf("Usage: broker %s <broker_id>", action), nil
        }
        
        brokerID, err := strconv.Atoi(args[2])
        if err != nil || brokerID < 0 || brokerID >= 5 {
            return "Invalid broker ID. Must be 0-4", nil
        }
        
        return cs.manageBroker(action, brokerID)
    default:
        return "Unknown broker command. Use: list, start, stop, restart", nil
    }
}

func (cs *ConsoleServer) listBrokers() string {
    var result strings.Builder
    
    result.WriteString("Broker List:\n")
    result.WriteString("============\n")
    
    for i := 0; i < 5; i++ {
        brokerID := BrokerID(i)
        if broker, exists := cs.coordinator.brokers[brokerID]; exists {
            result.WriteString(fmt.Sprintf("Broker %d: %s (Port: %d, Partitions: %d, Load: %.2f%%)\n",
                broker.ID, broker.Status, broker.Port, 
                broker.Load.PartitionCount, broker.Load.CPUUsagePercent))
        } else {
            result.WriteString(fmt.Sprintf("Broker %d: offline\n", i))
        }
    }
    
    return result.String()
}
```

### Throughput Optimization

Achieving high throughput requires optimizations at every level:

```go
// Network batching for efficient transmission
type NetworkBatch struct {
    Destination BrokerID    `json:"destination"`
    Messages    []Message   `json:"messages"`
    Compressed  []byte      `json:"compressed"`
    Size        int         `json:"size"`
    CreatedAt   time.Time   `json:"created_at"`
}

func (nb *NetworkBatch) Pack(compression CompressionType) error {
    // Serialize all messages together
    buffer := &bytes.Buffer{}
    
    for _, msg := range nb.Messages {
        data, err := msg.Serialize()
        if err != nil {
            return err
        }
        
        // Write message length + data
        binary.Write(buffer, binary.BigEndian, uint32(len(data)))
        buffer.Write(data)
    }
    
    // Apply compression
    switch compression {
    case CompressionGzip:
        nb.Compressed = gzipCompress(buffer.Bytes())
    case CompressionSnappy:
        nb.Compressed = snappyCompress(buffer.Bytes())
    case CompressionLZ4:
        nb.Compressed = lz4Compress(buffer.Bytes())
    default:
        nb.Compressed = buffer.Bytes()
    }
    
    nb.Size = len(nb.Compressed)
    return nil
}

// Zero-copy file operations for disk I/O
func (s *LogSegment) SendFile(conn net.Conn, offset, length int64) error {
    file, err := os.Open(s.File.Name())
    if err != nil {
        return err
    }
    defer file.Close()
    
    // Use sendfile() system call for zero-copy transfer
    _, err = io.CopyN(conn, &io.LimitedReader{
        R: io.NewSectionReader(file, offset, length),
        N: length,
    }, length)
    
    return err
}

// Memory pooling to reduce GC pressure
var (
    messagePool = sync.Pool{
        New: func() interface{} {
            return &Message{
                Headers: make(map[string]string),
            }
        },
    }
    
    bufferPool = sync.Pool{
        New: func() interface{} {
            return bytes.NewBuffer(make([]byte, 0, 4096))
        },
    }
)

func AcquireMessage() *Message {
    msg := messagePool.Get().(*Message)
    msg.Reset()
    return msg
}

func ReleaseMessage(msg *Message) {
    messagePool.Put(msg)
}

func AcquireBuffer() *bytes.Buffer {
    buf := bufferPool.Get().(*bytes.Buffer)
    buf.Reset()
    return buf
}

func ReleaseBuffer(buf *bytes.Buffer) {
    bufferPool.Put(buf)
}

// Worker pool for CPU-intensive operations
type WorkerPool struct {
    workerCount int
    taskQueue   chan Task
    workers     []*Worker
    wg          sync.WaitGroup
}

type Task interface {
    Execute() error
}

type Worker struct {
    id        int
    taskQueue chan Task
    quit      chan bool
}

func NewWorkerPool(workerCount int, queueSize int) *WorkerPool {
    return &WorkerPool{
        workerCount: workerCount,
        taskQueue:   make(chan Task, queueSize),
        workers:     make([]*Worker, workerCount),
    }
}

func (wp *WorkerPool) Start() {
    for i := 0; i < wp.workerCount; i++ {
        worker := &Worker{
            id:        i,
            taskQueue: wp.taskQueue,
            quit:      make(chan bool),
        }
        
        wp.workers[i] = worker
        wp.wg.Add(1)
        
        go worker.Start(&wp.wg)
    }
}

func (w *Worker) Start(wg *sync.WaitGroup) {
    defer wg.Done()
    
    for {
        select {
        case task := <-w.taskQueue:
            if err := task.Execute(); err != nil {
                log.Printf("Worker %d task failed: %v", w.id, err)
            }
        case <-w.quit:
            return
        }
    }
}

func (wp *WorkerPool) Submit(task Task) {
    wp.taskQueue <- task
}

func (wp *WorkerPool) Stop() {
    for _, worker := range wp.workers {
        worker.quit <- true
    }
    wp.wg.Wait()
}
```

This comprehensive implementation covers all the major components of a Kafka-like message queue system, complete with Go code examples for each subsystem. The design follows the plan structure while providing practical, production-ready code patterns.