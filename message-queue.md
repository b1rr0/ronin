---
layout: page
title: Message Queue System
permalink: /message-queue/
---

# Building a Message Queue from Scratch

Ever wondered what's really happening inside Kafka when you publish a message? Or how systems like RabbitMQ manage to handle millions of messages per second without breaking a sweat? Let's build our own message queue system and find out.

## The Big Picture

At its core, a message queue is just a very sophisticated post office. You have senders (producers) dropping off mail, a bunch of sorting facilities (brokers), and recipients (consumers) picking up their messages. But unlike your local post office, our system needs to handle millions of messages per second, never lose anything important, and keep working even when half the building is on fire.

Here's what we're building:

```
Producer → [Topic: user-events] → Broker Cluster → Consumer Group
             ├─ Partition 0 ─────────┐
             ├─ Partition 1 ─────────┼─→ [Broker 1, 2, 3, 4, 5]
             └─ Partition 2 ─────────┘
```

## Starting Simple: The Message

Everything starts with a message. In our system, a message is just some data with a bit of metadata attached:

```go
type Message struct {
    Key       []byte            // Used for partitioning
    Value     []byte            // The actual data
    Headers   map[string]string // Extra metadata
    Timestamp int64             // When it was created
    Offset    int64             // Position in the log
    Partition int32             // Which partition it belongs to
}
```

Simple enough, right? The `Key` is interesting because it determines which partition your message ends up in. Messages with the same key always go to the same partition, which means they'll be processed in order. Perfect for things like user events where order matters.

## The Partition: An Append-Only Log

Here's where things get interesting. Each partition is essentially an append-only log - think of it as a file where you can only write to the end, never modify what's already there.

```go
type Partition struct {
    ID       int32
    Log      []Message
    Offset   int64    // Next offset to assign
    Leader   BrokerID // Which broker is the boss
    Replicas []BrokerID // Who has copies
}

func (p *Partition) Append(msg Message) int64 {
    msg.Offset = p.Offset
    msg.Partition = p.ID
    p.Log = append(p.Log, msg)
    p.Offset++
    return msg.Offset
}
```

This immutable log design is surprisingly powerful. Want to replay all events from the beginning? Just start reading from offset 0. Need to debug what happened at 2 PM last Tuesday? Find the right offset and read from there. It's like having a complete history of everything that ever happened.

## The Producer: Getting Messages In

The producer's job is to take your data and get it into the right partition. But it's not as simple as just picking a random partition and dumping data there.

```go
type Producer struct {
    brokers    []string
    partitioner Partitioner
    batcher     *MessageBatcher
}

type Partitioner interface {
    ChoosePartition(key []byte, numPartitions int) int
}

// Hash-based partitioner - same key always goes to same partition
type HashPartitioner struct{}

func (h HashPartitioner) ChoosePartition(key []byte, numPartitions int) int {
    if len(key) == 0 {
        return rand.Intn(numPartitions) // Random if no key
    }
    hash := crc32.ChecksumIEEE(key)
    return int(hash) % numPartitions
}
```

The partitioner is crucial. If you're sending user events, you probably want all events for user "alice" to go to the same partition. That way, Alice's "login" event will always be processed before her "purchase" event, maintaining the correct order.

But producers don't send messages one by one - that would be terribly inefficient. Instead, they batch them up:

```go
type MessageBatcher struct {
    messages   []Message
    maxSize    int
    maxWait    time.Duration
    lastFlush  time.Time
}

func (b *MessageBatcher) Add(msg Message) bool {
    b.messages = append(b.messages, msg)
    
    // Time to flush?
    if len(b.messages) >= b.maxSize || 
       time.Since(b.lastFlush) > b.maxWait {
        return true // Signal to flush
    }
    return false
}
```

This batching is a classic trade-off: you get much better throughput, but you add a bit of latency. Most of the time, this is exactly what you want.

## The Broker: Where the Magic Happens

Brokers are the workhorses of our system. They store the actual data, handle replication, and coordinate with other brokers. Each broker is responsible for a subset of partitions.

```go
type Broker struct {
    ID          BrokerID
    Address     string
    Partitions  map[PartitionID]*PartitionLog
    Coordinator *ClusterCoordinator
}

type PartitionLog struct {
    Segments     []*LogSegment
    ActiveSegment *LogSegment
    Index        *OffsetIndex
}
```

The interesting part is how we store data. Instead of keeping everything in one giant file, we split it into segments:

```go
type LogSegment struct {
    BaseOffset  int64
    File        *os.File
    MaxSize     int64
    MessageCount int
}

func (s *LogSegment) Append(data []byte) error {
    n, err := s.File.Write(data)
    if err != nil {
        return err
    }
    s.MessageCount++
    return nil
}
```

Why segments? Because we need to clean up old data eventually. When a segment gets old enough, we just delete the whole file. Much simpler than trying to clean up the middle of a giant file.

## Replication: Never Lose Anything Important

Here's where distributed systems get tricky. What happens when a broker crashes? We need copies of everything, but we also need to make sure those copies stay in sync.

```go
type ReplicationManager struct {
    LeaderPartitions   map[PartitionID]*Partition
    FollowerPartitions map[PartitionID]*PartitionReplica
}

type PartitionReplica struct {
    Partition    *Partition
    LeaderBroker BrokerID
    LastSynced   int64
    InSync       bool
}

func (rm *ReplicationManager) ReplicateMessage(partitionID PartitionID, msg Message) error {
    // First, append to leader
    offset, err := rm.LeaderPartitions[partitionID].Append(msg)
    if err != nil {
        return err
    }
    
    // Then replicate to followers
    for _, replica := range rm.getFollowers(partitionID) {
        go func(r *PartitionReplica) {
            r.SyncFromLeader(offset)
        }(replica)
    }
    
    return nil
}
```

We use a leader-follower model. One broker is the leader for each partition and handles all writes. The followers just copy everything the leader does. If the leader dies, we promote one of the followers.

## The Consumer: Getting Messages Out

Consumers pull messages from partitions at their own pace. This is different from traditional message queues where the server pushes messages to consumers.

```go
type Consumer struct {
    GroupID    string
    Topics     []string
    Assignment []PartitionID
    Offsets    map[PartitionID]int64
}

func (c *Consumer) Poll() []Message {
    var messages []Message
    
    for _, partitionID := range c.Assignment {
        offset := c.Offsets[partitionID]
        batch := c.fetchFromBroker(partitionID, offset)
        messages = append(messages, batch...)
        
        // Update our position
        if len(batch) > 0 {
            lastOffset := batch[len(batch)-1].Offset
            c.Offsets[partitionID] = lastOffset + 1
        }
    }
    
    return messages
}
```

## Consumer Groups: Sharing the Load

What if you have more messages than one consumer can handle? Consumer groups let multiple consumers work together:

```go
type ConsumerGroup struct {
    ID        string
    Members   []Consumer
    Coordinator *GroupCoordinator
}

type GroupCoordinator struct {
    Groups      map[string]*ConsumerGroup
    Assignments map[string]map[ConsumerID][]PartitionID
}

func (gc *GroupCoordinator) Rebalance(groupID string) {
    group := gc.Groups[groupID]
    partitions := gc.getAllPartitions(group.Topics)
    
    // Simple round-robin assignment
    assignment := make(map[ConsumerID][]PartitionID)
    for i, partition := range partitions {
        consumer := group.Members[i % len(group.Members)]
        assignment[consumer.ID] = append(assignment[consumer.ID], partition)
    }
    
    gc.Assignments[groupID] = assignment
}
```

When a new consumer joins the group, we pause everything, reassign partitions, and resume. It's disruptive but fair.

## Putting It All Together: A Simple Producer

Let's see how all these pieces work together. Here's a simple producer that sends user events:

```go
func main() {
    producer := NewProducer([]string{"localhost:9092", "localhost:9093"})
    
    events := []UserEvent{
        {UserID: "alice", Action: "login", Timestamp: time.Now()},
        {UserID: "bob", Action: "purchase", Amount: 29.99},
        {UserID: "alice", Action: "logout", Timestamp: time.Now()},
    }
    
    for _, event := range events {
        message := Message{
            Key:   []byte(event.UserID), // Same user = same partition
            Value: json.Marshal(event),
            Headers: map[string]string{
                "type": "user_event",
                "version": "1.0",
            },
        }
        
        producer.Send("user-events", message)
    }
}
```

The producer will hash Alice's ID and send both her login and logout events to the same partition, guaranteeing they're processed in order.

## Performance: Making It Fast

All of this is nice, but how do we make it fast? A few key optimizations:

**Batching Everything**: Don't send messages one by one. Batch them up and send dozens at once.

```go
type BatchingSender struct {
    pending map[PartitionID][]Message
    ticker  *time.Ticker
}

func (bs *BatchingSender) flush() {
    for partitionID, messages := range bs.pending {
        go bs.sendBatch(partitionID, messages)
    }
    bs.pending = make(map[PartitionID][]Message)
}
```

**Zero-Copy Operations**: When moving data between network and disk, avoid copying it through userspace:

```go
func (s *LogSegment) sendFile(conn net.Conn, offset, length int64) error {
    // Use sendfile() system call for zero-copy transfer
    return syscall.Sendfile(conn, s.File, offset, length)
}
```

**Memory Pooling**: Reuse objects instead of allocating new ones constantly:

```go
var messagePool = sync.Pool{
    New: func() interface{} {
        return &Message{
            Headers: make(map[string]string),
        }
    },
}

func borrowMessage() *Message {
    return messagePool.Get().(*Message)
}

func returnMessage(msg *Message) {
    msg.reset()
    messagePool.Put(msg)
}
```

## Consistency: The Hard Choices

Here's where it gets philosophical. Do you want your system to be:

- **Fast and available** but might give you slightly stale data?
- **Perfectly consistent** but might be slow or unavailable?

You can't have both (thanks, CAP theorem), so we choose fast and available. Our system uses "eventual consistency" - if you stop writing data, all replicas will eventually have the same data, but there might be brief moments where they disagree.

```go
type ConsistencyLevel int

const (
    NoAck     ConsistencyLevel = iota // Fire and forget
    LeaderAck                        // Wait for leader
    AllAck                           // Wait for all replicas
)

func (p *Producer) SendWithAck(msg Message, level ConsistencyLevel) error {
    switch level {
    case NoAck:
        return p.sendAsync(msg)
    case LeaderAck:
        return p.sendSyncLeader(msg)
    case AllAck:
        return p.sendSyncAll(msg)
    }
}
```

Most of the time, `LeaderAck` is the sweet spot - you get confirmation that the message won't be lost, but you don't wait for every replica.

## Throughput: How to Handle Millions of Messages

Getting high throughput isn't about one magic trick - it's about optimizing every part of the pipeline. Here's how we squeeze every bit of performance out of our system:

**Network Batching**: Instead of sending one message at a time, we pack dozens together:

```go
type NetworkBatch struct {
    messages    []Message
    compressed  []byte
    destination BrokerID
}

func (nb *NetworkBatch) pack() {
    // Serialize all messages together
    buffer := &bytes.Buffer{}
    for _, msg := range nb.messages {
        binary.Write(buffer, binary.BigEndian, msg.serialize())
    }
    
    // Compress the entire batch
    nb.compressed = gzipCompress(buffer.Bytes())
}
```

**Disk Optimizations**: Sequential writes are much faster than random writes. Our log segments are perfect for this:

```go
func (s *LogSegment) batchWrite(messages []Message) error {
    // Write all messages in one system call
    var buffer bytes.Buffer
    for _, msg := range messages {
        buffer.Write(msg.serialize())
    }
    
    // Single write to disk
    _, err := s.File.Write(buffer.Bytes())
    return err
}
```

**CPU Efficiency**: We use goroutines for parallel processing but avoid creating too many:

```go
type WorkerPool struct {
    workers   chan chan Message
    workQueue chan Message
    quit      chan bool
}

func (wp *WorkerPool) Start(numWorkers int) {
    for i := 0; i < numWorkers; i++ {
        worker := NewWorker(wp.workers)
        worker.Start()
    }
    
    go wp.dispatch()
}
```

The key insight? **Bottlenecks move around**. Fix the network, and suddenly disk becomes the problem. Fix disk, and CPU becomes the bottleneck. Real high-throughput systems need to optimize everything simultaneously.

## Scaling: Adding More Servers

What happens when one machine isn't enough? Our partitioned design makes horizontal scaling straightforward, but there are some tricks involved.

**Partition Redistribution**: When you add new brokers, you need to move some partitions around:

```go
type Rebalancer struct {
    currentAssignment map[PartitionID]BrokerID
    targetAssignment  map[PartitionID]BrokerID
}

func (r *Rebalancer) AddBroker(newBroker BrokerID) {
    allPartitions := r.getAllPartitions()
    numBrokers := len(r.getBrokers()) + 1 // Including new broker
    
    // Redistribute partitions evenly
    for i, partition := range allPartitions {
        targetBroker := BrokerID(i % numBrokers)
        r.targetAssignment[partition] = targetBroker
    }
    
    r.executeRebalance()
}

func (r *Rebalancer) executeRebalance() {
    for partition, newBroker := range r.targetAssignment {
        currentBroker := r.currentAssignment[partition]
        if currentBroker != newBroker {
            r.migratePartition(partition, currentBroker, newBroker)
        }
    }
}
```

**Hot Partition Problem**: Sometimes one partition gets way more traffic than others. Maybe everyone's talking about the same trending topic:

```go
type PartitionMonitor struct {
    messageRates map[PartitionID]int64
    threshold    int64
}

func (pm *PartitionMonitor) detectHotPartitions() []PartitionID {
    var hotPartitions []PartitionID
    avgRate := pm.calculateAverageRate()
    
    for partition, rate := range pm.messageRates {
        if rate > avgRate*3 { // 3x higher than average
            hotPartitions = append(hotPartitions, partition)
        }
    }
    
    return hotPartitions
}
```

When you detect a hot partition, you can either split it or add more replicas to spread the read load.

## Processing Styles: FIFO vs General

This is where the rubber meets the road. How you process messages determines what guarantees your system can provide.

### FIFO (First In, First Out) Processing

FIFO means strict ordering - message A always gets processed before message B if A arrived first. This requires some constraints:

```go
type FIFOProcessor struct {
    partition  PartitionID
    processor  MessageProcessor
    queue      chan Message
    processing bool
}

func (fp *FIFOProcessor) Start() {
    go func() {
        for msg := range fp.queue {
            fp.processing = true
            
            // Process one message at a time
            err := fp.processor.Process(msg)
            if err != nil {
                // Must retry - can't skip messages in FIFO
                fp.retry(msg)
                continue
            }
            
            // Only commit after successful processing
            fp.commitOffset(msg.Offset)
            fp.processing = false
        }
    }()
}
```

**FIFO Use Cases**:
- **Financial transactions**: Transfer A must complete before Transfer B
- **User state changes**: Login before logout, signup before purchase
- **Audit logs**: Events must be in exact chronological order

### General (Parallel) Processing

General processing prioritizes throughput over strict ordering. Multiple messages can be processed simultaneously:

```go
type ParallelProcessor struct {
    workers    []*Worker
    dispatcher chan Message
    numWorkers int
}

func (pp *ParallelProcessor) Start() {
    // Start multiple workers
    for i := 0; i < pp.numWorkers; i++ {
        worker := &Worker{
            id:       i,
            messages: make(chan Message, 100),
            processor: pp.processor,
        }
        pp.workers[i] = worker
        go worker.Start()
    }
    
    // Distribute messages to workers
    go pp.distribute()
}

func (pp *ParallelProcessor) distribute() {
    for msg := range pp.dispatcher {
        // Simple round-robin distribution
        workerID := int(msg.Offset) % pp.numWorkers
        pp.workers[workerID].messages <- msg
    }
}
```

**General Processing Use Cases**:
- **Analytics events**: Order doesn't matter, just process everything
- **Notification sending**: Can send emails in any order
- **Image processing**: Each image is independent

### The Trade-off

Here's the fundamental trade-off:

```go
type ProcessingGuarantee int

const (
    StrictOrdering ProcessingGuarantee = iota  // FIFO: Slow but ordered
    HighThroughput                             // General: Fast but unordered
    BestEffort                                 // Hybrid: Fast most of the time
)
```

Most real systems use a hybrid approach - FIFO for critical data flows, parallel processing for everything else.

## Comparing Message Queue Systems

How does our system stack up against the real world? Let's see:

### Apache Kafka
**What it's good at**: Exactly what we built - high throughput, durable logs, stream processing.
```go
// Similar to our partition design
type KafkaPartition struct {
    Topic     string
    Partition int32
    Leader    int32
    Replicas  []int32
}
```
**Use when**: You need massive throughput and don't mind some complexity.

### RabbitMQ
**What it's good at**: Traditional message queuing with complex routing.
```go
// RabbitMQ-style routing
type Exchange struct {
    Name       string
    Type       string // direct, topic, fanout, headers
    Bindings   []Binding
}

type Binding struct {
    Queue      string
    RoutingKey string
    Arguments  map[string]interface{}
}
```
**Use when**: You need flexible message routing and traditional queue semantics.

### AWS SQS
**What it's good at**: Simple, managed queuing with no infrastructure to manage.
```go
// SQS-style simple queue
type SQSMessage struct {
    Body              string
    ReceiptHandle     string
    VisibilityTimeout int
    DelaySeconds      int
}
```
**Use when**: You want simple queuing without any operational overhead.

### NATS
**What it's good at**: Ultra-low latency pub/sub for microservices.
```go
// NATS-style subject-based messaging
func (n *NATSClient) Publish(subject string, data []byte) error {
    return n.conn.Publish(subject, data)
}

func (n *NATSClient) Subscribe(subject string, handler func([]byte)) {
    n.conn.Subscribe(subject, handler)
}
```
**Use when**: You need the fastest possible message delivery between services.

### AWS SNS
**What it's good at**: Fan-out notifications to multiple subscribers.
```go
// SNS-style fan-out
type SNSTopic struct {
    ARN           string
    Subscriptions []Subscription
}

type Subscription struct {
    Protocol string // email, sms, http, sqs
    Endpoint string
}
```
**Use when**: You need to notify many different systems about events.

### The Decision Matrix

| System | Throughput | Latency | Durability | Complexity | Best For |
|--------|------------|---------|------------|------------|----------|
| Our System | Very High | Medium | High | Medium | Event streaming |
| Kafka | Very High | Medium | High | High | Data pipelines |
| RabbitMQ | Medium | Low | Medium | Medium | Traditional queues |
| SQS | Medium | Medium | High | Low | Simple async tasks |
| NATS | High | Very Low | Low | Low | Real-time messaging |
| SNS | Medium | Low | Medium | Low | Notifications |

The key insight? **There's no one-size-fits-all solution**. Pick the tool that matches your specific requirements.

## What We've Built

Starting from a simple "append data to a file" idea, we've built something that looks a lot like Kafka:

- **Partitioned topics** for scaling and ordering
- **Replication** for fault tolerance  
- **Consumer groups** for parallel processing
- **Batching and zero-copy** for performance
- **Configurable consistency** for different use cases

The beauty is in how these simple concepts compose into something powerful. An append-only log is just a file, but make it replicated and partitioned, and suddenly you have a system that can handle petabytes of data.

## What's Next?

This is just the beginning. Real message queue systems have many more features:

- **Exactly-once delivery** using idempotent producers and transactional consumers
- **Schema evolution** so you can change message formats without breaking everything
- **Stream processing** to transform data as it flows through
- **Multi-datacenter replication** for global deployments

But the core ideas remain the same: immutable logs, partitioning for scale, and replication for safety. Everything else is optimization and convenience features built on top of these foundations.

Want to see the full implementation? Check out the code samples above and try building your own version. There's nothing quite like implementing a distributed system to really understand how it works.