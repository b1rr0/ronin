---
layout: page
title: Message Broker Systems
permalink: /message-broker/
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


/* Code styling - Terminal look */
pre {
  background-color: #1e1e1e !important;
  color: #ffffff !important;
  border: 2px solid #333 !important;
  border-radius: 8px !important;
  padding: 20px !important;
  margin: 20px 0 !important;
  overflow-x: auto !important;
  max-width: 100% !important;
  font-family: 'Courier New', Consolas, Monaco, monospace !important;
  font-size: 14px !important;
  line-height: 1.4 !important;
  box-shadow: 0 4px 12px rgba(0,0,0,0.3) !important;
}

pre code {
  background-color: transparent !important;
  color: #ffffff !important;
  padding: 0 !important;
  border: none !important;
  word-wrap: break-word !important;
  overflow-wrap: break-word !important;
}

/* Inline code styling */
code {
  background-color: #2d2d2d !important;
  color: #ffffff !important;
  padding: 2px 6px !important;
  border-radius: 4px !important;
  font-family: 'Courier New', Consolas, Monaco, monospace !important;
  font-size: 13px !important;
  word-wrap: break-word;
  overflow-wrap: break-word;
}

/* Syntax highlighting for Go code */
pre code .keyword { color: #569cd6 !important; }
pre code .string { color: #ce9178 !important; }
pre code .comment { color: #6a9955 !important; }
pre code .function { color: #dcdcaa !important; }
pre code .type { color: #4ec9b0 !important; }

/* Header hierarchy styles */
h1 { font-size: 2.5rem !important; }
h2 { font-size: 2.0rem !important; }
h3 { font-size: 1.5rem !important; }
h4 { font-size: 1.25rem !important; }
h5 { font-size: 1.1rem !important; }
h6 { font-size: 1.0rem !important; }

@media (max-width: 768px) {
  img {
    max-width: 95%;
  }
  
  pre {
    font-size: 14px;
  }
  
}
</style>

## Introduction

In the world of multi-tier distributed systems, there is a critical need for message queues. They solve fundamental communication problems between services, providing reliable asynchronous data transmission and creating the foundation for scalable, fault-tolerant architectures.

## Problems Solved by Message Queues

### Asynchronous Communication and Service Independence

![2_syn_microservices.png](assets/message_broker/2_syn_microservices.png)

- **Services can operate independently in time**
- **Senders don't wait for receivers to become available**
- **Decouples service lifecycles and deployment schedules**

Instead of making sequential calls to multiple microservices, message queues enable rapid message delivery to several microservices simultaneously. A single message can be published once and consumed by multiple services, dramatically reducing latency and eliminating the complexity of coordinating synchronous calls across distributed systems.

![1_sync_micrpservice_with_5_another_services.png](assets/message_broker/1_sync_micrpservice_with_5_another_services.png)

**Instead of complex synchronous interactions between multiple services, we can simplify with a queue:**

![1_write_to_queue_4_read.png](assets/message_broker/1_write_to_queue_4_read.png)

**Improved Fault Tolerance:**
- When a receiver temporarily crashes, messages are stored in the queue and processed later
- Reduces risk of data loss during service outages
- Provides graceful degradation under failure conditions

**Scalability:**
- Easy to add more consumers (workers) to process messages faster
- Queue distributes load between multiple consumers
- Horizontal scaling without architectural changes

**Load Buffering:**
- When incoming request flow is very large, the queue temporarily "smooths" the load
- Protects the system from overload situations
- Acts as a shock absorber for traffic spikes

**Reliable Data Transmission and Logging:**
- **Messages are not lost (or rarely lost, depending on configuration)**
- **Can implement guaranteed delivery ("at least once", "exactly once")**
- **Provides audit trail and replay capabilities**

### Why Not Just Use Simple Programming Language Queues?

You might think: "Why not just use a simple queue from a programming language where first-in-first-out (FIFO) works?" Indeed, we could use basic data structures like queues from standard libraries:
![queue_as_data_strucure.png](assets/message_broker/queue_as_data_strucure.png)
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

### What Makes Message Brokers Production-Ready

Message brokers are sophisticated distributed systems designed to solve enterprise-grade messaging challenges. They consist of several core components and differ significantly in their architecture and trade-offs:

#### 1. Core Components of Message Brokers

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

#### 2. Message Broker Architecture Patterns

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

#### 3. Different Message Brokers Comparison

Various message brokers exist, each with different trade-offs:

- **Kafka**: High-throughput, log-based, designed for stream processing
- **RabbitMQ**: Traditional messaging patterns, complex routing, lower latency
- **AWS SQS/SNS**: Managed services, simple integration, limited throughput
- **NATS**: Ultra-low latency, lightweight, good for microservices

### CAP Theorem and Message Brokers

![CAP_theorem.png](assets/message_broker/CAP_theorem.png)

#### How CAP Theorem Relates to Message Brokers

Message brokers (Kafka, RabbitMQ, NATS, Pulsar, etc.) are distributed systems and must comply with the CAP theorem. Different brokers make different trade-offs between Consistency, Availability, and Partition tolerance.

#### Examples

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

## Let's Look Inside: How Apache Kafka Works

### Brokers: The Foundation

The Kafka cluster consists of brokers. You can think of the system as a data center and servers in it. When first getting acquainted, think of a Kafka broker as a computer: it's a process in the operating system with access to its local disk.

![kaffka_classter_intro.png](assets/message_broker/kaffka_classter_intro.png)

All brokers are connected to each other by a network and act together, forming a single cluster. When we say that producers write events to a Kafka cluster, we mean that they work with brokers in it.

By the way, in a cloud environment, the cluster doesn't necessarily run on dedicated servers - these can be virtual machines or containers in Kubernetes.

### Topics and Partitions

#### Topics: Logical Organization

A topic is a **logical division** of message categories into groups. For example, events by order statuses, partner coordinates, route sheets, and so on.

The key word here is **logical**. Topics are conceptual containers that organize related messages together. We create topics for events of a common group and try not to mix them with each other. For example, partner coordinates should not be in the same topic as order statuses, and updated order statuses should not be stored mixed with user registration updates.

**Topics are logical splits** - they define what kind of data goes where, but they don't determine how the data is physically stored.
![send_to_topic.png](assets/message_broker/send_to_topic.png)
It's convenient to think of a topic as a log - you write an event to the end and don't destroy the chain of old events in the process. General logic:

- One producer can write to one or more topics
- One consumer can read one or more topics
- One or more producers can write to one topic
- One or more consumers can read from one topic

Theoretically, there are no restrictions on the number of these topics, but practically this is limited by the number of partitions.
#### Partitions: Physical Splitting of Data
While topics provide logical organization, partitions handle the physical splitting of data. There are no restrictions on the number of topics in a Kafka cluster, but
computer hardware imposes natural limitations. As operations consume processor resources and I/O capacity, systems eventually hit performance ceilings.

Since we cannot increase machine power indefinitely, topic data must be divided into physical parts called partitions. Each topic consists of one or more partitions that
can be distributed across different brokers. This enables Kafka's horizontal scaling: create a topic, divide it into partitions, and place each partition on separate
brokers to distribute the workload.
![topics.png](assets/message_broker/topics.png)

**Key Relationship: Topics ⊃ Partitions**
- **Topics** = Logical containers (what data category)
- **Partitions** = Physical storage units (where and how data is stored)
- Topics are split into partitions for scalability and performance
Formally, a partition is a strictly ordered log of messages stored physically on disk. Each message in it is added to the end without the possibility of changing it in the future and somehow affecting already written messages. At the same time, the topic as a whole has no order, but the order of messages always exists within each individual partition.

**Partition Replication and High Availability**
![kafka_patrition.png](assets/message_broker/kafka_patrition.png)

Each partition has one **leader** broker that handles all reads and writes, while **follower replicas** (clones) are maintained on other brokers. If the leader broker fails, one of the followers automatically becomes the new leader, ensuring continuous operation. This replication mechanism provides fault tolerance and prevents data loss.
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


### Coordinator Service: Managing the Cluster
![zookeeper.png](assets/message_broker/zookeeper.png)
The coordinator acts as the brain of the Kafka cluster, managing metadata, broker orchestration, and service lifecycle. **In a traditional Kafka setup, this role is handled by Zookeeper** (or KRaft in newer versions), which maintains cluster state, broker registry, and topic metadata.

For more details about Kafka-Zookeeper architecture, see: [Kafka Architecture: Kafka Zookeeper](https://www.redpanda.com/guides/kafka-architecture-kafka-zookeeper)

**Our Custom Coordinator Implementation:**

Here's the code that emulates the work of our own coordinator, handling all cluster management responsibilities:

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

![producer_write_to_partitions.png](assets/message_broker/producer_write_to_partitions.png)
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

#### Load Balancing and Metadata Management

The coordinator tracks broker load and helps producers make intelligent decisions. This process involves both selecting optimal partitions and efficiently managing metadata:

**Partition Selection Strategies:**

Producers use different algorithms to distribute messages across partitions. Each strategy serves specific use cases:

##### 1. Round-Robin Partitioning

**Algorithm:** Distributes messages evenly across all partitions using a rotating counter.

**Description:**
- Simple sequential distribution across partitions
- Excellent for load balancing when message ordering isn't critical
- No hot partitions - traffic spreads uniformly

**Code:**
```go
type RoundRobinPartitioner struct {
    counter int32
}

func (r *RoundRobinPartitioner) Partition(message *Message, metadata *TopicMetadata) int32 {
    partitionCount := metadata.PartitionCount
    partition := atomic.AddInt32(&r.counter, 1) % partitionCount
    return partition
}
```

##### 2. Key-Based Partitioning (Consistent Hashing)

**Algorithm:** Uses message key hash to determine partition, ensuring same keys always go to same partition.

**Description:**
- Guarantees ordering within each key
- Enables related messages to stay together
- Risk of hot partitions if key distribution is uneven

**Code:**
```go
type KeyPartitioner struct{}

func (k *KeyPartitioner) Partition(message *Message, metadata *TopicMetadata) int32 {
    if message.Key == nil {
        return rand.Int31n(metadata.PartitionCount)
    }
    
    hash := murmur3.Sum32(message.Key)
    return int32(hash % uint32(metadata.PartitionCount))
}
```

##### 3. Sticky Partitioning

**Algorithm:** Batches keyless messages to same partition until batch is full, then switches.

**Description:**
- Improves batching efficiency for better throughput
- Reduces network round-trips
- Better resource utilization while maintaining load balance

**Code:**
```go
type StickyPartitioner struct {
    stickyPartition int32
    batchIsFull     bool
    mutex           sync.Mutex
}

func (s *StickyPartitioner) Partition(message *Message, metadata *TopicMetadata) int32 {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    
    if message.Key != nil {
        hash := murmur3.Sum32(message.Key)
        return int32(hash % uint32(metadata.PartitionCount))
    }
    
    if s.stickyPartition == -1 || s.batchIsFull {
        s.stickyPartition = rand.Int31n(metadata.PartitionCount)
        s.batchIsFull = false
    }
    
    return s.stickyPartition
}
```

##### 4. Load-Aware Selection

**Algorithm:** Chooses partition based on real-time broker load metrics.

**Description:**
- Monitors CPU, memory, disk usage, and message rates
- Calculates weighted scores for optimal distribution
- Prevents overloading busy brokers

**Code:**
```go
func (p *Producer) loadAwarePartitionSelection(topicMeta *TopicMetadata) int32 {
    type PartitionScore struct {
        ID    int32
        Score float64  // Lower is better
    }
    
    var scores []PartitionScore
    
    for partitionID, partition := range topicMeta.Partitions {
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
    
    sort.Slice(scores, func(i, j int) bool {
        return scores[i].Score < scores[j].Score
    })
    
    return scores[0].ID
}
```

**Metadata Caching for Performance:**

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

**For more information about load balancing challenges and solutions in Apache Kafka, see:** [How We Solve Load Balancing Challenges in Apache Kafka](https://medium.com/agoda-engineering/how-we-solve-load-balancing-challenges-in-apache-kafka-8cd88fdad02b)


### Message Batching System

Batching is one of the most critical optimizations in Kafka's architecture. Instead of sending each message individually (which would create enormous network overhead), producers collect multiple messages into batches before transmission.

**Why Batching is Essential:**

Modern applications can generate thousands of messages per second. Without batching, each message would require a separate network round-trip, overwhelming both the network and the broker with connection overhead. A single network request can have 1-2ms latency, so sending 1000 individual messages would take 1-2 seconds just in network overhead!

**The Batching Trade-off:**

Batching creates a fundamental trade-off between **latency** and **throughput**:
- **Higher batch sizes** = Better throughput, but higher latency (messages wait longer)
- **Smaller batch sizes** = Lower latency, but reduced throughput
- **Linger time** = How long to wait for a batch to fill up before sending

**Real-World Impact:**
- **Without batching**: 1,000 messages = 1,000 network requests = ~1-2 seconds
- **With batching**: 1,000 messages = 10 batches = ~10-20ms + processing time

**Our Implementation:**

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

![delivery_semantics.png](assets/message_broker/delivery_semantics.png)

Message delivery semantics define the guarantees about message delivery between producers and consumers. Our system supports three different levels of delivery guarantees, each with different performance and reliability trade-offs:

**How Each Delivery Semantic Works in Our System:**

#### 1. At-Most-Once Delivery
- **Producer**: Uses `NoAck` - fires messages and doesn't wait for confirmation
- **Consumer**: Commits offset **before** processing the message
- **Outcome**: Fast performance, but messages can be lost if consumer crashes after commit but before processing
- **Use case**: Non-critical data like metrics or logs where occasional loss is acceptable

#### 2. At-Least-Once Delivery
- **Producer**: Uses `LeaderAck` or `AllAck` - waits for broker confirmation before considering send successful
- **Consumer**: Commits offset **after** successfully processing the message
- **Outcome**: No message loss, but duplicates possible if consumer crashes after processing but before commit
- **Use case**: Most common pattern for business-critical data

#### 3. Exactly-Once Delivery
- **Producer**: Uses transactional writes with idempotent producers
- **Consumer**: Uses transactional reads with coordinated commits
- **Outcome**: Expensive but guarantees no loss and no duplicates
- **Use case**: Financial transactions, critical business events

**Our Implementation:**

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

**For More Detailed Information:**

For a deeper dive into exactly-once semantics and how Apache Kafka implements these delivery guarantees in production, read: [Exactly-once Semantics are Possible: Here's How Apache Kafka Does it](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)

## The Log: Foundation of Event Streaming

**TLDR**: A log is an ordered stream of events over time. An event occurs, gets to the end of the log, and remains there unchanged.

Apache Kafka manages logs and organizes a platform that connects data providers with consumers and provides the ability to receive an ordered stream of events in real time.

### How Kafka Logs Are Structured

At its core, Kafka is built around the concept of logs - not application logs for debugging, but commit logs similar to those used in databases. Each log is an ordered, immutable sequence of records that is continually appended to.

**Physical File Structure:**

Kafka organizes data on disk in a hierarchical structure:

```
/kafka-logs/
├── topic-name-partition-0/         # Partition 0 of "topic-name"
│   ├── 00000000000000000000.log    # Segment file (data)
│   ├── 00000000000000000000.index  # Offset index
│   ├── 00000000000000000000.timeindex # Time index
│   ├── 00000000000000012345.log    # Next segment file
│   ├── 00000000000000012345.index  # Next offset index
│   └── leader-epoch-checkpoint     # Leader epoch data
├── topic-name-partition-1/          # Partition 1 of "topic-name"
│   ├── 00000000000000000000.log
│   ├── 00000000000000000000.index
│   └── ...
├── another-topic-partition-0/       # Different topic, partition 0
│   ├── 00000000000000000000.log
│   └── ...
└── __consumer_offsets-0/  # Internal Kafka topic for offset storage
    ├── 00000000000000000000.log
    └── ...
```

**Segment Files Explained:**

The partitions themselves are physically represented on disks as segments. These are separate files that can be created, rotated, or deleted according to the data aging settings in them. Usually you don't have to often remember about partition segments unless you administer the cluster, but it's important to remember the data storage model in Kafka topics.

Each partition is divided into segments. A segment is simply a file on disk containing a portion of the log. When a segment reaches a certain size (default 1GB) or age (default 7 days), it's closed and a new segment is created.

- **Log files (.log)**: Contain the actual message data
  - Store messages in binary format with headers, keys, values, and metadata
  - Messages are appended sequentially (append-only log structure)
  - Each message has a fixed-size header followed by variable-length payload
  - Optimized for sequential I/O operations for maximum throughput
  - Cannot be modified once written (immutable log segments)

- **Index files (.index)**: Map offsets to physical positions in log files for fast lookups
  - Sparse index containing offset-to-position mappings every N messages
  - Enables O(log n) binary search to find message by offset
  - Much smaller than log files (only stores position pointers)
  - Memory-mapped for ultra-fast random access
  - Essential for consumer seek operations and random offset queries

- **Time index files (.timeindex)**: Map timestamps to offsets for time-based queries
  - Maps timestamp ranges to their corresponding offset ranges
  - Enables time-based message retrieval ("give me messages from 2 hours ago")
  - Supports retention policies based on message age
  - Used by consumers with timestamp-based seek operations
  - Critical for log compaction and cleanup operations

**Log Segment Naming Convention:**

The segment files are named with the base offset of the first message in that segment:
- `00000000000000000000.log` - Contains messages from offset 0
- `00000000000000012345.log` - Contains messages starting from offset 12,345

**Log Compaction and Retention:**

Kafka can manage log data in two ways:

1. **Time-based retention**: Delete segments older than a configured time (e.g., 7 days)
2. **Size-based retention**: Delete oldest segments when total size exceeds limit
3. **Log compaction**: Keep only the latest value for each key (for topics with keys)

**How Reads and Writes Work:**
![logs_image.png](assets/message_broker/logs_image.png)

*Note: The image above shows a simplified "human-readable" example for illustration purposes. In reality, Kafka stores messages in optimized binary format with fixed-size headers, checksums, and compressed payloads for maximum performance and storage efficiency.*

**Writing (Appending):**
- New messages are always appended to the active segment (latest .log file)
- Write operations are sequential, making them very fast
- Each message gets assigned the next available offset
- **Simultaneously updates multiple files:**
  - **Log file (.log)**: Appends the actual message data in binary format
  - **Index file (.index)**: Adds offset-to-position mapping every ~4KB of data
  - **Time index (.timeindex)**: Records timestamp-to-offset mapping for time-based queries

**Reading:**
- Consumers specify an offset to start reading from
- Kafka uses the index files to quickly locate the physical position
- Reads can happen from any point in the log, multiple times

**Log Segments in Action:**

```go
type LogSegment struct {
    BaseOffset    int64           // First offset in this segment
    NextOffset    int64           // Next offset to be assigned
    LogFile       *os.File        // The .log file
    IndexFile     *os.File        // The .index file
    TimeIndexFile *os.File        // The .timeindex file
    MaxSize       int64           // When to roll to new segment
    CreatedTime   time.Time       // When this segment was created
}

// When writing a new message
func (s *LogSegment) Append(message []byte) (offset int64, err error) {
    offset = s.NextOffset
    
    // Write to log file
    position, err := s.LogFile.Write(message)
    if err != nil {
        return 0, err
    }
    
    // Update index (offset -> file position mapping)
    s.IndexFile.Write(encodeIndex(offset, position))
    
    s.NextOffset++
    return offset, nil
}
```

#### Real-World Example: 10 Log Segments

Here's how a busy topic with 10 segments would look on disk:

```
/kafka-logs/topic-orders-partition-0/
# Log files (.log) - contain actual message data
├── 00000000000000000000.log      # Messages 0-9,999
├── 00000000000000010000.log      # Messages 10,000-19,999
├── 00000000000000020000.log      # Messages 20,000-29,999
├── 00000000000000030000.log      # Messages 30,000-39,999
├── 00000000000000040000.log      # Messages 40,000-49,999
├── 00000000000000050000.log      # Messages 50,000-59,999
├── 00000000000000060000.log      # Messages 60,000-69,999
├── 00000000000000070000.log      # Messages 70,000-79,999
├── 00000000000000080000.log      # Messages 80,000-89,999
├── 00000000000000090000.log      # Messages 90,000-99,999 (active segment)
#
# Index files (.index) - map offsets to positions in log files
├── 00000000000000000000.index    # Index for segment 0
├── 00000000000000010000.index    # Index for segment 1
├── 00000000000000020000.index    # Index for segment 2
├── 00000000000000030000.index    # Index for segment 3
├── 00000000000000040000.index    # Index for segment 4
├── 00000000000000050000.index    # Index for segment 5
├── 00000000000000060000.index    # Index for segment 6
├── 00000000000000070000.index    # Index for segment 7
├── 00000000000000080000.index    # Index for segment 8
└── 00000000000000090000.index    # Index for segment 9 (active)
```

#### Reading Process Example

**How Reading by Offset Works:**

**Step 1: Find Correct Segment (Binary Search)**
- Kafka maintains sorted list of segment base offsets: [0, 10000, 20000, 30000, ...]
- For target offset 65,432: Binary search finds segment starting at 60,000
- Load corresponding files: `00000000000000060000.log` and `00000000000000060000.index`

**Step 2: Find Position in Segment (Binary Search)**
- Use sparse index to locate approximate position
- Index entries: [(60000→0), (64000→4096), (68000→8192)]
- For offset 65,432: Binary search finds entry (64000→4096)
- Start reading from byte position 4096 in log file

**Step 3: Sequential Scan to Exact Offset**
- Read messages sequentially from position 4096
- Check each message offset until reaching 65,432
- Return batch of messages starting from that exact offset

**How Reading by Timestamp Works:**

**Step 1: Find Time Range (Binary Search)**
- Use time index to map timestamp to offset range
- Time entries: [(timestamp1→offset1), (timestamp2→offset2)]
- Binary search finds closest timestamp ≤ target timestamp

**Step 2: Convert to Offset-Based Search**
- Get offset from time index result
- Switch to standard offset-based lookup process
- Continue with binary search through segments and positions

**1. Log File Format Implementation:**
```go
type LogFileEntry struct {
    Offset      int64             // 8 bytes
    MessageSize uint32            // 4 bytes
    CRC32       uint32            // 4 bytes
    Magic       byte              // 1 byte
    Attributes  byte              // 1 byte
    Timestamp   int64             // 8 bytes
    KeyLength   int32             // 4 bytes
    Key         []byte            // Variable
    ValueLength int32             // 4 bytes
    Value       []byte            // Variable
    Headers     []MessageHeader   // Variable
}

func (l *LogFile) writeMessage(msg *Message) (int64, error) {
    messageSize := 26 + len(msg.Key) + len(msg.Value)
    buffer := make([]byte, 0, messageSize+12)
    
    binary.BigEndian.PutUint64(buffer[0:8], uint64(msg.Offset))
    binary.BigEndian.PutUint32(buffer[8:12], uint32(messageSize))
    
    pos := 12
    binary.BigEndian.PutUint32(buffer[pos:pos+4], msg.CRC32)
    buffer[pos+4] = msg.Magic
    buffer[pos+5] = msg.Attributes
    binary.BigEndian.PutUint64(buffer[pos+6:pos+14], uint64(msg.Timestamp))
    
    binary.BigEndian.PutUint32(buffer[pos+14:pos+18], int32(len(msg.Key)))
    copy(buffer[pos+18:], msg.Key)
    binary.BigEndian.PutUint32(buffer[pos+18+len(msg.Key):], int32(len(msg.Value)))
    copy(buffer[pos+22+len(msg.Key):], msg.Value)
    
    return l.file.Write(buffer)
}
```

**2. Index File Binary Search Implementation:**
```go
type IndexEntry struct {
    RelativeOffset uint32  // Offset relative to segment base offset
    Position       uint32  // Byte position in .log file
}

type IndexFile struct {
    file        *os.File
    mmap        []byte     // Memory-mapped file for fast access
    entries     []IndexEntry
    baseOffset  int64      // Base offset of the segment
    maxEntries  int        // Maximum number of entries
}

func (idx *IndexFile) lookup(targetOffset int64) (uint32, error) {
    relativeOffset := uint32(targetOffset - idx.baseOffset)
    
    // Binary search through memory-mapped entries
    left, right := 0, len(idx.entries)-1
    
    for left <= right {
        mid := (left + right) / 2
        entry := idx.entries[mid]
        
        if entry.RelativeOffset == relativeOffset {
            return entry.Position, nil
        } else if entry.RelativeOffset < relativeOffset {
            left = mid + 1
        } else {
            right = mid - 1
        }
    }
    
    // Return largest entry <= targetOffset
    if right >= 0 {
        return idx.entries[right].Position, nil
    }
    
    return 0, nil // Read from beginning
}
```

**3. Complete Reading Process:**
```go
// Consumer wants to read from offset 65,432
func (p *Partition) ReadFromOffset(targetOffset int64) []Message {
    // 1. Find correct segment: offset 65,432 is in segment 060000
    segment := p.findSegment(65,432) // Returns segment starting at 60,000
    
    // 2. Use index to find position in log file
    position := segment.IndexFile.findPosition(65,432)
    
    // 3. Seek to that position and read
    segment.LogFile.Seek(position, 0)
    return segment.readMessages(100) // Read next 100 messages
}
```

**Benefits of This Log Structure:**

1. **Sequential I/O**: All writes are appends, which are much faster than random writes
2. **Immutability**: Once written, messages never change, simplifying concurrent access
3. **Efficient Storage**: Index files allow fast random access without loading entire log
4. **Scalability**: Each partition can be on different disks/servers
5. **Fault Tolerance**: Segments can be replicated across multiple brokers


### Internal File Structure Details

Now let's dive deeper into how each file type stores data internally:

#### 1. Log Files (.log) - Message Data Storage

The .log files contain the actual message data in a binary format. Each message is stored with the following structure:

```
Message Record Format:
┌─────────────┬──────────────┬─────────────┬──────────────┬─────────────┬─────────────┐
│   Offset    │  Message     │   CRC32     │  Magic Byte  │  Attributes │  Timestamp  │
│   8 bytes   │  Size        │   4 bytes   │   1 byte     │   1 byte    │   8 bytes   │
│   (int64)   │  4 bytes     │             │              │             │             │
└─────────────┼──────────────┼─────────────┼──────────────┼─────────────┼─────────────┤
│  Key Length │  Key Data    │ Value Length│  Value Data  │   Headers   │             │
│   4 bytes   │  N bytes     │   4 bytes   │   M bytes    │  Variable   │             │
│   (int32)   │              │   (int32)   │              │             │             │
└─────────────┴──────────────┴─────────────┴──────────────┴─────────────┴─────────────┘
```

**Log File Components:**
- **Offset**: Unique monotonic identifier for the message within partition
- **Message Size**: Total size of the message record in bytes
- **CRC32**: Checksum for data integrity verification
- **Magic Byte**: Protocol version identifier (v0, v1, v2)
- **Attributes**: Compression type, timestamp type flags
- **Timestamp**: Message timestamp (producer or broker time)
- **Key/Value**: Actual message payload

#### 2. Index Files (.index) - Offset to Position Mapping

Index files provide fast lookup from logical offsets to physical byte positions in the log file. They use a compact binary format:

```
Index Entry Format (8 bytes each):
┌──────────────┬─────────────────┐
│ Relative     │ Physical        │
│ Offset       │ Position        │
│ 4 bytes      │ 4 bytes         │
│ (int32)      │ (int32)         │
└──────────────┴─────────────────┘
```

**Index File Details:**
- **Sparse indexing**: Not every message has an index entry (default: every 4KB of log data)
- **Relative offsets**: Stored relative to segment base offset to save space
- **Sorted order**: Entries are always sorted by offset for binary search
- **Fixed size**: Each entry is exactly 8 bytes

#### 3. Time Index Files (.timeindex) - Timestamp to Offset Mapping

Time index files enable time-based queries by mapping timestamps to offsets:

```
Time Index Entry Format (12 bytes each):
┌──────────────┬──────────────┬─────────────────┐
│ Timestamp    │ Relative     │ (Padding)       │
│ 8 bytes      │ Offset       │ 0 bytes         │
│ (int64)      │ 4 bytes      │                 │
│              │ (int32)      │                 │
└──────────────┴──────────────┴─────────────────┘
```

**Time Index Details:**
- **Timestamp precision**: Milliseconds since Unix epoch
- **Sparse entries**: Not every message, typically every segment boundary
- **Monotonic timestamps**: Entries must be in chronological order
- **Time-based queries**: Enable "read from timestamp X" operations

#### 4. Leader Epoch Checkpoint - Leader Election Tracking

```
│   └── leader-epoch-checkpoint     # Leader epoch data
```

The leader-epoch-checkpoint file tracks leadership changes for high availability and consistency:

- **Leader epochs**: Sequential numbers assigned each time a new leader is elected
- **Offset tracking**: Records the starting offset for each leader's term
- **Consistency guarantees**: Prevents data loss during leader failover scenarios
- **Recovery mechanism**: Helps determine valid data range after broker restarts

**Internal Format:**
```
Leader Epoch Checkpoint File Structure:
┌─────────────┬──────────────┬─────────────┬──────────────┐
│ Version     │ Entry Count  │ Epoch 0     │ Start Offset │
│ 4 bytes     │ 4 bytes      │ 4 bytes     │ 8 bytes      │
│ (int32)     │ (int32)      │ (int32)     │ (int64)      │
├─────────────┼──────────────┼─────────────┼──────────────┤
│             │              │ Epoch 1     │ Start Offset │
│             │              │ 4 bytes     │ 8 bytes      │
│             │              │ (int32)     │ (int64)      │
└─────────────┴──────────────┴─────────────┴──────────────┘

Example content:
Version: 0
Entry Count: 3
Epoch 0: Start Offset 0
Epoch 1: Start Offset 12345
Epoch 2: Start Offset 25000
```

#### 5. Consumer Offset Log - Internal Topic Storage

```
└── __consumer_offsets-0/  # Internal Kafka topic for offset storage
    ├── 00000000000000000000.log
    └── ...
```

The `__consumer_offsets` topic stores consumer group progress and metadata:

- **Offset commits**: Records where each consumer group has read up to
- **Group metadata**: Stores consumer group membership and partition assignments
- **Automatic cleanup**: Old offset records are periodically compacted
- **Distributed storage**: Replicated across brokers like any other topic

**Internal Format:**
```
Consumer Offset Message Structure:
┌──────────────────┬─────────────────┬──────────────────┐
│ Key              │ Value           │ Headers          │
│ (Group+Topic+    │ (Offset+        │ (Timestamp+      │
│  Partition)      │  Metadata)      │  Expiry)         │
└──────────────────┴─────────────────┴──────────────────┘

my-group|user-events|0 → offset:12345, epoch:2, ts:1642680000000
my-group|user-events|1 → offset:23456, epoch:2, ts:1642680000000
my-group|orders|0 → offset:5678, epoch:1, ts:1642680000000
```
### Message Write Flow: From Producer to Log

Here's the complete flow of how a message travels from producer to being stored in log files:

```go
// Complete message write flow
func (broker *Broker) handleProduceRequest(req *ProduceRequest) error {
    for _, batch := range req.MessageBatches {
        // Step 1: Validate and assign offsets
        partition := broker.getPartition(batch.Topic, batch.Partition)
        if partition == nil {
            return fmt.Errorf("partition not found")
        }
        
        // Step 2: Assign consecutive offsets
        startOffset := partition.nextOffset
        for i, msg := range batch.Messages {
            msg.Offset = startOffset + int64(i)
            msg.Timestamp = time.Now().UnixMilli()
        }
        
        // Step 3: Write to active log segment
        err := broker.writeToLog(partition, batch.Messages)
        if err != nil {
            return fmt.Errorf("failed to write to log: %w", err)
        }
        
        // Step 4: Update indexes
        err = broker.updateIndexes(partition, batch.Messages)
        if err != nil {
            return fmt.Errorf("failed to update indexes: %w", err)
        }
        
        // Step 5: Update high water mark
        partition.highWaterMark = batch.Messages[len(batch.Messages)-1].Offset + 1
        
        // Step 6: Trigger replication (if leader)
        if partition.isLeader {
            go broker.replicateToFollowers(partition, batch.Messages)
        }
        
        // Step 7: Send acknowledgment back to producer
        go broker.sendProduceResponse(req.ClientID, batch.Messages)
    }
    
    return nil
}

func (broker *Broker) writeToLog(partition *Partition, messages []*Message) error {
    activeSegment := partition.activeSegment
    
    // Check if we need to roll to new segment
    if activeSegment.shouldRoll() {
        newSegment, err := partition.rollNewSegment()
        if err != nil {
            return err
        }
        activeSegment = newSegment
    }
    
    // Write each message to log file
    for _, msg := range messages {
        // Step 1: Serialize message
        data, err := msg.serialize()
        if err != nil {
            return err
        }
        
        // Step 2: Calculate CRC32 for integrity
        msg.CRC32 = crc32.ChecksumIEEE(data)
        
        // Step 3: Write to log file
        position, err := activeSegment.logFile.writeMessage(msg)
        if err != nil {
            return err
        }
        
        // Step 4: Update segment metadata
        activeSegment.size += int64(len(data))
        activeSegment.messageCount++
        activeSegment.lastOffset = msg.Offset
        
        log.Printf("Written message offset=%d, size=%d bytes, position=%d", 
            msg.Offset, len(data), position)
    }
    
    return nil
}

func (broker *Broker) updateIndexes(partition *Partition, messages []*Message) error {
    activeSegment := partition.activeSegment
    
    for _, msg := range messages {
        // Update offset index (sparse - every 4KB or configurable interval)
        if broker.shouldCreateIndexEntry(activeSegment, msg) {
            err := activeSegment.offsetIndex.append(msg.Offset, msg.Position)
            if err != nil {
                return fmt.Errorf("failed to update offset index: %w", err)
            }
        }
        
        // Update time index (even more sparse - every segment or time interval)
        if broker.shouldCreateTimeIndexEntry(activeSegment, msg) {
            err := activeSegment.timeIndex.append(msg.Timestamp, msg.Offset)
            if err != nil {
                return fmt.Errorf("failed to update time index: %w", err)
            }
        }
    }
    
    return nil
}

func (broker *Broker) shouldCreateIndexEntry(segment *LogSegment, msg *Message) bool {
    // Create index entry every 4KB of log data (configurable)
    return (segment.size - segment.lastIndexedPosition) >= 4096
}

func (broker *Broker) shouldCreateTimeIndexEntry(segment *LogSegment, msg *Message) bool {
    // Create time index entry every 10 minutes or 1MB (configurable)
    timeDiff := msg.Timestamp - segment.lastIndexedTimestamp
    sizeDiff := segment.size - segment.lastTimeIndexedPosition
    
    return timeDiff >= 600000 || sizeDiff >= 1048576 // 10 minutes or 1MB
}

// Complete write flow example
func ExampleMessageWriteFlow() {
    // Producer sends batch of messages
    messages := []*Message{
        {Key: []byte("user-123"), Value: []byte(`{"action": "login"}`)},
        {Key: []byte("user-456"), Value: []byte(`{"action": "logout"}`)},
        {Key: []byte("user-789"), Value: []byte(`{"action": "purchase"}`)},
    }
    
    // Broker processes the batch
    for _, msg := range messages {
        log.Printf("Processing message: key=%s", string(msg.Key))
        
        // 1. Assign offset: 15000, 15001, 15002
        // 2. Write to log: append to 00000000000000012345.log
        // 3. Update index: add entry to 00000000000000012345.index
        // 4. Update time index: maybe add to 00000000000000012345.timeindex
        // 5. Sync to followers: replicate to other brokers
        // 6. Send ack: confirm write to producer
    }
    
    // Result: 3 messages persisted durably to disk with indexes for fast retrieval
}
```

**Write Flow Summary:**
1. **Message Reception**: Broker receives batch from producer
2. **Offset Assignment**: Each message gets unique, monotonic offset
3. **Log Writing**: Messages appended to active segment's .log file
4. **Index Updates**: Sparse entries added to .index and .timeindex files
5. **Metadata Update**: Partition high water mark advanced
6. **Replication**: Data copied to follower replicas (if leader)
7. **Acknowledgment**: Success response sent back to producer

This flow ensures durability, ordering, and efficient retrieval while maintaining high write throughput.

**Further Reading**: For more detailed information about Kafka log performance and internals, see [Kafka Performance: Kafka Logs](https://www.redpanda.com/guides/kafka-performance-kafka-logs).

## Consumer Service

A **Consumer** is a client that reads messages from Kafka topics. Consumers retrieve data from one or more partitions and process them at their own pace.

![send_to_topic.png](assets/message_broker/send_to_topic.png)

*Basic Kafka architecture showing producers sending messages to topics, which are then consumed by consumers. This illustrates the fundamental data flow where producers write to topics and consumers read from them.*

### Consumer Principles

- **Pull-based model**: Consumers actively request data from brokers (rather than receiving push notifications)
- **Offset tracking**: Consumers track their reading position in each partition
- **Stateful**: Consumers remember where they stopped, even after restarts

```go
type Consumer struct {
    groupID     string                    // Group identifier
    topics      []string                  // Subscribed topics  
    assignment  map[string][]int32        // topic -> partitions
    offsets     map[TopicPartition]int64  // Current reading positions
    coordinator *GroupCoordinator         // Group coordinator
    fetcher     *MessageFetcher          // Message fetching component
    processor   MessageProcessor         // Message processor
    config      ConsumerConfig          // Configuration
}

type TopicPartition struct {
    Topic     string `json:"topic"`
    Partition int32  `json:"partition"`
}
```

## Consumer Groups

### Consumer Group Concept

A **Consumer Group** is a group of Consumers with the same `group.id` that **collectively** read from a topic, **distributing partitions among themselves**.

### Key Principles

1. **One partition = one Consumer per group**
2. **One Consumer can read multiple partitions**
3. **Automatic partition distribution**
4. **Shared offset tracking**

### Visual Examples

#### Scenario 1: Single Consumer
![single_in_a_consumer_group.png](assets/message_broker/single_in_a_consumer_group.png)

*A single consumer in a consumer group reads from all partitions of a topic. This provides simplicity but limits scalability as all processing is done by one consumer.*


#### Scenario 2: Multiple Consumers
![Multiple_consumers_in_one_consumer.png](assets/message_broker/Multiple_consumers_in_one_consumer.png)
*Multiple consumers in the same group distribute partitions among themselves. Each partition is assigned to exactly one consumer in the group, enabling parallel processing while ensuring each message is processed only once within the group.*

#### Scenario 3: More Consumers than Partitions
![Additional_consumers_in_a_group_sit_idly.png](assets/message_broker/Additional_consumers_in_a_group_sit_idly.png)

*When there are more consumers in a group than partitions in the topic, some consumers will remain idle. This demonstrates the partition limit - you cannot have more active consumers in a group than partitions.*

#### Scenario 4: Multiple Groups Reading Same Topic
![Multiple_consumers_reading_the_same_records_from_the_topic.png](assets/message_broker/Multiple_consumers_reading_the_same_records_from_the_topic.png)
*Different consumer groups can read the same messages from a topic independently. Each group maintains its own offset tracking, allowing different applications to process the same data stream for different purposes (e.g., one group for analytics, another for notifications).*

#### Competing Consumers Pattern (Load Balancing)
When you want to **distribute workload** across multiple consumer instances, all consumers use the **same group ID**. Kafka ensures each message is delivered to only one consumer in the group. For example, if you have an order processing service with multiple instances, each order will be processed by exactly one instance, enabling horizontal scaling.

#### Publish-Subscribe Pattern (Broadcasting)
When you want **multiple applications** to process the same data, each application uses a **different group ID**. This allows multiple services (analytics, notifications, audit) to all receive every message from the same topic, enabling multiple independent data processing pipelines.

### Group States and Lifecycle
Consumer groups transition through these states:
- **Empty**: No active members
- **PreparingRebalance**: Rebalance initiated when members join/leave
- **CompletingRebalance**: Partition assignment in progress
- **Stable**: Normal operation with fixed assignments
- **Dead**: Group deleted

Groups continuously cycle between Stable and rebalancing states as membership changes.

### Key Benefits of Consumer Groups

1. **Horizontal Scalability**: Add more consumers to increase processing capacity
2. **Fault Tolerance**: If one consumer fails, others continue processing
3. **Load Distribution**: Workload automatically distributed across available consumers
4. **Flexible Processing**: Multiple applications can process the same data independently
5. **Automatic Recovery**: Failed consumers automatically rejoin and resume processing

## Complete Consumer Connection Flow

Here's how a consumer connects and starts reading messages:

### **📡 Phase 1: Connect & Discover**
1. **Connect to any broker** → get cluster info
2. **Cache metadata** (brokers, topics, leaders)

### **👥 Phase 2: Join Group**
3. **Find coordinator** → join consumer group
4. **Get partition assignment** → sync with others

### **🔗 Phase 3: Setup Reading**
8. **Find partition leaders** for assigned partitions
9. **Get current offsets** from `__consumer_offsets` topic
10. **Open connections** to partition leader brokers

### **🔄 Phase 4: Reading Loop**
11. **Send fetch requests** to leaders (offset, wait time, batch size)
12. **Broker reads logs** using segment files and indexes  
13. **Process message batches** received from brokers
14. **Commit offsets** to track progress
15. **Send heartbeats** to stay in the group

### **⚙️ Phase 5: Maintenance**
16. **Monitor rebalancing** when consumers join/leave
17. **Refresh metadata** when brokers fail/recover
18. **Handle errors** with retries and failover

This process ensures **reliable message delivery**, **horizontal scaling** through groups, and **fault tolerance** through automatic recovery.

## Protocols for Consumers and Producers

### Kafka Wire Protocol Foundation

Both **consumers** and **producers** communicate with Kafka brokers using the same **custom binary protocol over TCP**. This protocol is optimized for high throughput and low latency and is used by all Kafka clients regardless of whether they're reading (consuming) or writing (producing) messages.

**Protocol Stack:**
```
Application Layer:    Kafka API Requests (Fetch, Produce, Metadata, etc.)
Transport Layer:      TCP (reliable, ordered delivery)
Network Layer:        IP
```

**Key Features of Kafka Wire Protocol:**
- **Binary Format**: Compact binary encoding for efficiency
- **Request-Response Model**: Synchronous communication with correlation IDs
- **Batching Support**: Multiple messages per request for high throughput
- **Compression**: Built-in support for GZIP, Snappy, LZ4, ZSTD
- **API Versioning**: Backward/forward compatibility through API versions
- **Connection Pooling**: Persistent connections for reduced overhead

### Communication Protocol Options

Kafka supports multiple protocols that both **consumers** (readers) and **producers** (writers) can use to communicate with brokers. Each protocol is optimized for different use cases:

#### 1. Kafka Native Protocol (Wire Protocol)
```go
// Native Kafka TCP protocol - most efficient for both consumers and producers
type KafkaClient struct {
    BootstrapServers []string `yaml:"bootstrap_servers"`
    Protocol         string   `yaml:"protocol"` // "kafka" 
    SecurityProtocol string   `yaml:"security_protocol"` // "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"
}
```

**Key Features:**
- **Binary Protocol**: Custom TCP-based binary protocol for maximum efficiency
- **Batching**: Messages are fetched in batches for high throughput
- **Compression**: Supports GZIP, Snappy, LZ4, ZSTD compression
- **Security**: SSL/TLS encryption and SASL authentication support

#### 2. HTTP REST API Protocol
```go
// REST API client for web applications (both consumers and producers)
type RESTClient struct {
    BaseURL          string `yaml:"base_url"`           // "http://kafka-rest:8082"
    ConsumerInstance string `yaml:"consumer_instance"`  // "my-consumer-instance"
    Format          string `yaml:"format"`             // "json", "avro", "binary"
    LongPollTimeout  int    `yaml:"long_poll_timeout"`  // 30000ms (30 seconds)
    MaxBytes         int    `yaml:"max_bytes"`          // 1048576 (1MB)
}

// REST API consumer creation
POST /consumers/my-consumer-group
{
  "name": "my-consumer-instance",
  "format": "json",
  "auto.offset.reset": "earliest"
}

// Long polling request for consuming messages
GET /consumers/my-consumer-group/instances/my-consumer/records?timeout=30000&max_bytes=1048576
```

**Key Features:**
- **HTTP-based**: Standard HTTP requests for easy integration
- **Long Polling**: Server holds request open until messages arrive or timeout
- **JSON Format**: Human-readable message format
- **Stateless**: Each request is independent
- **Web Integration**: Perfect for web applications and microservices

**How REST Long Polling Works:**
1. **Client sends GET request** with timeout parameter (e.g., 30 seconds)
2. **Server waits** for new messages or until timeout expires
3. **Server responds** immediately when messages arrive OR after timeout
4. **Client processes** received messages and immediately sends next request
5. **Continuous cycle** creates near real-time message delivery

```javascript
// Long polling implementation example
async function longPollConsume() {
    while (true) {
        try {
            const response = await fetch(
                '/consumers/my-group/instances/my-consumer/records?timeout=30000&max_bytes=1048576',
                { 
                    method: 'GET',
                    headers: { 'Accept': 'application/vnd.kafka.json.v2+json' }
                }
            );
            
            const messages = await response.json();
            if (messages.length > 0) {
                processMessages(messages);
                await commitOffsets();
            }
            // Immediately start next long poll request
        } catch (error) {
            console.error('Long poll error:', error);
            await sleep(5000); // Wait before retry on error
        }
    }
}
```

#### 3. gRPC Protocol
```go
// gRPC client for microservices (both consumers and producers)
type GRPCClient struct {
    ServerAddress string `yaml:"server_address"` // "kafka-grpc:9093"
    TLSEnabled    bool   `yaml:"tls_enabled"`
    AuthToken     string `yaml:"auth_token"`
    StreamBuffer  int    `yaml:"stream_buffer"`   // 1000 messages
}

// gRPC service definition
service KafkaConsumerService {
    rpc Subscribe(SubscribeRequest) returns (stream Message);
    rpc Commit(CommitRequest) returns (CommitResponse);
    rpc GetOffsets(GetOffsetsRequest) returns (GetOffsetsResponse);
}
```

**Key Features:**
- **Streaming**: Real-time message streaming with backpressure
- **Type Safety**: Strong typing with Protocol Buffers
- **Efficient**: Binary serialization with HTTP/2
- **Cross-Language**: Works across different programming languages

**How gRPC Streaming Works:**
1. **Client opens stream** by calling `Subscribe()` RPC method
2. **Server pushes messages** continuously through the open stream
3. **Client receives** messages in real-time as they arrive
4. **Backpressure control** - client can slow down if overwhelmed
5. **Automatic reconnection** on connection failures

```go
// gRPC streaming consumer example
func (c *GRPCClient) StartConsuming() error {
    stream, err := c.client.Subscribe(context.Background(), &SubscribeRequest{
        Topics:  []string{"user-events"},
        GroupId: "my-group",
    })
    if err != nil {
        return err
    }
    
    // Listen for incoming messages on the stream
    for {
        message, err := stream.Recv()
        if err != nil {
            log.Printf("Stream error: %v", err)
            return c.reconnectAndRetry()
        }
        
        // Process message immediately as it arrives
        go c.processMessage(message)
    }
}
```

**gRPC Advantages:**
- **True streaming** - no polling overhead
- **Built-in flow control** - handles backpressure automatically
- **Connection multiplexing** - multiple streams over single connection
- **Efficient binary protocol** - lower bandwidth usage

#### 4. WebSocket Protocol
```go
// WebSocket client for real-time web applications (both consumers and producers)
type WebSocketClient struct {
    URL             string `yaml:"url"`              // "wss://kafka-ws:8080/consume"
    Topics          []string `yaml:"topics"`
    GroupID         string `yaml:"group_id"`
    ReconnectDelay  time.Duration `yaml:"reconnect_delay"`
    PingInterval    time.Duration `yaml:"ping_interval"`    // 30s keepalive
    MaxMessageSize  int64 `yaml:"max_message_size"`         // 1MB
}

// WebSocket subscription message
{
  "type": "subscribe",
  "topics": ["user-events"],
  "group_id": "web-notifications",
  "offset_reset": "earliest"
}

// WebSocket incoming message format
{
  "type": "message",
  "topic": "user-events",
  "partition": 0,
  "offset": 12345,
  "key": "user-123",
  "value": {"action": "login", "timestamp": 1642680000},
  "headers": {"source": "web-app"}
}
```

**Key Features:**
- **Real-time**: Low-latency bidirectional communication
- **Browser Support**: Native browser WebSocket support
- **Auto-Reconnect**: Automatic reconnection on connection loss
- **JSON Messages**: Easy to parse in JavaScript

**How WebSocket Listening Works:**
1. **Client establishes WebSocket connection** to Kafka WebSocket proxy
2. **Client sends subscription message** with topics and group ID
3. **Server pushes messages** immediately as they arrive in Kafka
4. **Event-driven processing** - client receives `onmessage` events
5. **Bidirectional communication** - client can send acknowledgments back

```javascript
// WebSocket consumer implementation
class WebSocketKafkaConsumer {
    constructor(url, topics, groupId) {
        this.url = url;
        this.topics = topics;
        this.groupId = groupId;
        this.reconnectDelay = 5000;
    }
    
    connect() {
        this.ws = new WebSocket(this.url);
        
        this.ws.onopen = () => {
            console.log('WebSocket connected');
            // Subscribe to topics
            this.ws.send(JSON.stringify({
                type: 'subscribe',
                topics: this.topics,
                group_id: this.groupId
            }));
        };
        
        // Listen for incoming messages
        this.ws.onmessage = (event) => {
            const message = JSON.parse(event.data);
            if (message.type === 'message') {
                this.processMessage(message);
                
                // Send acknowledgment
                this.ws.send(JSON.stringify({
                    type: 'ack',
                    topic: message.topic,
                    partition: message.partition,
                    offset: message.offset
                }));
            }
        };
        
        this.ws.onclose = () => {
            console.log('WebSocket disconnected, reconnecting...');
            setTimeout(() => this.connect(), this.reconnectDelay);
        };
        
        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }
    
    processMessage(message) {
        console.log('Received:', message);
        // Process message immediately
    }
}

// Usage
const consumer = new WebSocketKafkaConsumer(
    'wss://kafka-ws:8080/consume',
    ['user-events', 'orders'],
    'web-consumer-group'
);
consumer.connect();
```

**WebSocket Advantages:**
- **Real-time push** - messages arrive immediately, no polling
- **Low overhead** - persistent connection, minimal protocol overhead  
- **Browser native** - works directly in web browsers without extra libraries
- **Bidirectional** - can send acknowledgments and control messages back

### Protocol Comparison
![protocol_coparison.png](assets/message_broker/protocol_coparison.png)

