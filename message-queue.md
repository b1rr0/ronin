---
layout: page
title: Message Queue System
permalink: /message-queue/
---

# Message Queue System 

## Overview

A custom message queue system similar to Apache Kafka with core distributed messaging capabilities.

---

# Theoretical Foundations

## 1. Architectural Principles

### 1.1 CAP Theorem and Trade-offs

Our system is based on the CAP theorem (Consistency, Availability, Partition tolerance):

```
CAP Triangle:
    Consistency
       /\
      /  \
     /    \
    /______\
Availability  Partition Tolerance
```

**Our choice: AP (Availability + Partition Tolerance)**
- **Consistency**: Eventual consistency through replication
- **Availability**: System remains available when nodes fail
- **Partition Tolerance**: System works during network partitions

### 1.2 Distributed Log Model

The system is built on the **immutable log** model:

```
Partition as Immutable Log:
[Msg1][Msg2][Msg3][Msg4][Msg5]...
  ↑     ↑     ↑     ↑     ↑
Offset: 0     1     2     3     4

Features:
- Append-only writes
- Sequential reads
- Offset-based positioning
- Time-based retention
```

### 1.3 Sharding Theory (Partitioning)

**Horizontal partitioning** for scalability:

```go
// Partitioning strategies
type PartitionStrategy interface {
    SelectPartition(key []byte, partitionCount int) int
}

// 1. Hash-based partitioning
func (h *HashPartitioner) SelectPartition(key []byte, count int) int {
    return int(crc32.ChecksumIEEE(key)) % count
}

// 2. Round-robin partitioning  
func (r *RoundRobinPartitioner) SelectPartition(key []byte, count int) int {
    r.counter++
    return r.counter % count
}

// 3. Range-based partitioning
func (rng *RangePartitioner) SelectPartition(key []byte, count int) int {
    // Based on key ranges
    return r.findRange(key)
}
```

## 2. Consistency Algorithms

### 2.1 Leader Election

Uses simplified **Round-Robin Leader Election** algorithm:

```go
type LeaderElection struct {
    brokers []BrokerID
    current int
    mu      sync.RWMutex
}

func (le *LeaderElection) SelectLeader(partition int) BrokerID {
    le.mu.Lock()
    defer le.mu.Unlock()
    
    // Simple leader rotation
    leader := le.brokers[partition % len(le.brokers)]
    return leader
}

// Handle broker failure - re-election
func (le *LeaderElection) HandleBrokerFailure(failedBroker BrokerID) {
    // Reassign partitions from failed broker
    for partition, leader := range le.partitionLeaders {
        if leader == failedBroker {
            le.partitionLeaders[partition] = le.selectNextAvailable()
        }
    }
}
```

### 2.2 Replication Protocol

**ISR (In-Sync Replicas)** model:

```
Leader Broker:
┌─────────────┐
│   Partition │ ── writes ──► [Log: Msg1, Msg2, Msg3]
└─────────────┘                        │
                                       │ replication
                                       ▼
Follower Brokers:                 ┌─────────────┐
┌─────────────┐                   │ Replica 1   │ [Log: Msg1, Msg2, Msg3]
│ Follower 1  │ ◄─── sync ────────┤             │
└─────────────┘                   └─────────────┘
                                  ┌─────────────┐
┌─────────────┐                   │ Replica 2   │ [Log: Msg1, Msg2, --]
│ Follower 2  │ ◄─── sync ────────┤             │ (lagging)
└─────────────┘                   └─────────────┘

ISR = {Leader, Replica 1}  // Only synced replicas
```

### 2.3 Consistency Models

```go
// 1. At-Most-Once (maximum once)
type AtMostOnceConsumer struct {
    autoCommit bool
}

func (c *AtMostOnceConsumer) ProcessMessage(msg Message) {
    // Commit offset BEFORE processing
    c.commitOffset(msg.Offset)
    
    // Process message (may fail - message lost)
    err := c.processBusinessLogic(msg)
    if err != nil {
        // Message is lost, but no duplicates
        log.Printf("Message lost: %v", err)
    }
}

// 2. At-Least-Once (minimum once)  
type AtLeastOnceConsumer struct {
    manualCommit bool
}

func (c *AtLeastOnceConsumer) ProcessMessage(msg Message) {
    // Process message first
    err := c.processBusinessLogic(msg)
    if err != nil {
        // Will retry - potential duplicates
        return err
    }
    
    // Commit offset AFTER successful processing
    c.commitOffset(msg.Offset)
}

// 3. Exactly-Once (exactly once)
type ExactlyOnceConsumer struct {
    transactional bool
    idempotentKeys map[string]bool
}

func (c *ExactlyOnceConsumer) ProcessMessage(msg Message) {
    // Check idempotent key
    if c.idempotentKeys[msg.IdempotentKey] {
        // Already processed - skip
        return nil
    }
    
    // Transactional processing
    tx := c.beginTransaction()
    err := c.processBusinessLogic(msg)
    if err != nil {
        tx.rollback()
        return err
    }
    
    // Mark as processed and commit
    c.idempotentKeys[msg.IdempotentKey] = true
    tx.commit()
}
```

## 3. Performance and Optimization

### 3.1 Batch Processing

**Producer Batching**:
```go
type BatchProducer struct {
    batchSize    int
    lingerMs     int
    batch        []Message
    lastSent     time.Time
    mu           sync.Mutex
}

func (p *BatchProducer) Send(msg Message) {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    p.batch = append(p.batch, msg)
    
    // Trigger send conditions
    if len(p.batch) >= p.batchSize || 
       time.Since(p.lastSent) > time.Duration(p.lingerMs)*time.Millisecond {
        p.flushBatch()
    }
}

func (p *BatchProducer) flushBatch() {
    if len(p.batch) == 0 {
        return
    }
    
    // Send entire batch in one network call
    p.sendBatchToBroker(p.batch)
    p.batch = p.batch[:0] // Reset batch
    p.lastSent = time.Now()
}
```

### 3.2 Zero-Copy Optimization

**Data transfer optimization**:
```go
// Traditional copy method (slow)
func traditionalSend(file *os.File, socket net.Conn) error {
    buffer := make([]byte, 4096)
    for {
        n, err := file.Read(buffer)
        if err != nil {
            break
        }
        socket.Write(buffer[:n])  // Kernel -> User -> Kernel copy
    }
    return nil
}

// Zero-copy method (fast)
func zeroCopySend(file *os.File, socket net.Conn) error {
    // Direct kernel-to-kernel transfer
    if tcpConn, ok := socket.(*net.TCPConn); ok {
        tcpFile, _ := tcpConn.File()
        return syscall.Splice(
            int(file.Fd()),    // source
            int(tcpFile.Fd()), // destination  
            4096,              // bytes to copy
        )
    }
    return nil
}
```

### 3.3 Memory Management

**Object Pooling** to reduce GC pressure:
```go
type MessagePool struct {
    pool sync.Pool
}

func NewMessagePool() *MessagePool {
    return &MessagePool{
        pool: sync.Pool{
            New: func() interface{} {
                return &Message{
                    Headers: make(map[string]string, 4),
                    Value:   make([]byte, 0, 1024),
                }
            },
        },
    }
}

func (p *MessagePool) Get() *Message {
    return p.pool.Get().(*Message)
}

func (p *MessagePool) Put(msg *Message) {
    // Reset message state
    msg.Reset()
    p.pool.Put(msg)
}

// Usage in hot path
func (broker *Broker) handleMessage() {
    msg := broker.msgPool.Get()
    defer broker.msgPool.Put(msg)
    
    // Process message without allocation
    broker.processMessage(msg)
}
```

## 4. Consumer Group Coordination

### 4.1 Partition Assignment Algorithms

**Round-Robin Assignment**:
```go
type RoundRobinAssignor struct{}

func (rr *RoundRobinAssignor) Assign(consumers []ConsumerID, partitions []PartitionID) Assignment {
    assignment := make(Assignment)
    
    // Sort for deterministic results
    sort.Slice(consumers, func(i, j int) bool { return consumers[i] < consumers[j] })
    sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
    
    // Round-robin assignment
    for i, partition := range partitions {
        consumer := consumers[i % len(consumers)]
        assignment[consumer] = append(assignment[consumer], partition)
    }
    
    return assignment
}
```

**Range Assignment** (more balanced):
```go
type RangeAssignor struct{}

func (ra *RangeAssignor) Assign(consumers []ConsumerID, partitions []PartitionID) Assignment {
    assignment := make(Assignment)
    
    partitionsPerConsumer := len(partitions) / len(consumers)
    remainder := len(partitions) % len(consumers)
    
    start := 0
    for i, consumer := range consumers {
        // Distribute remainder among first few consumers
        count := partitionsPerConsumer
        if i < remainder {
            count++
        }
        
        assignment[consumer] = partitions[start:start+count]
        start += count
    }
    
    return assignment
}
```

### 4.2 Rebalancing Protocol

**Coordination model**:
```go
type ConsumerGroupCoordinator struct {
    groupID     string
    members     map[ConsumerID]*ConsumerMetadata
    generation  int
    state       GroupState
    mu          sync.RWMutex
}

type GroupState int
const (
    Stable GroupState = iota
    PreparingRebalance
    CompletingRebalance
)

func (cgc *ConsumerGroupCoordinator) HandleJoinGroup(consumerID ConsumerID) error {
    cgc.mu.Lock()
    defer cgc.mu.Unlock()
    
    // Add new member
    cgc.members[consumerID] = &ConsumerMetadata{
        ID: consumerID,
        JoinedAt: time.Now(),
    }
    
    // Trigger rebalancing
    if cgc.state == Stable {
        cgc.state = PreparingRebalance
        cgc.generation++
        go cgc.rebalance()
    }
    
    return nil
}

func (cgc *ConsumerGroupCoordinator) rebalance() {
    // Phase 1: Prepare rebalance
    cgc.notifyAllMembers("REBALANCE_REQUIRED")
    
    // Wait for all members to rejoin
    cgc.waitForRejoin()
    
    // Phase 2: Assign partitions
    assignment := cgc.assignor.Assign(
        cgc.getActiveMembers(),
        cgc.getPartitions(),
    )
    
    // Phase 3: Complete rebalance
    cgc.distributeAssignment(assignment)
    cgc.state = Stable
}
```

### 4.3 Heartbeat Mechanism

**Failure Detection via Heartbeats**:
```go
type HeartbeatManager struct {
    interval        time.Duration
    sessionTimeout  time.Duration
    lastHeartbeats  map[ConsumerID]time.Time
    mu             sync.RWMutex
}

func (hm *HeartbeatManager) StartMonitoring() {
    ticker := time.NewTicker(hm.interval)
    defer ticker.Stop()
    
    for range ticker.C {
        hm.checkTimeouts()
    }
}

func (hm *HeartbeatManager) checkTimeouts() {
    hm.mu.Lock()
    defer hm.mu.Unlock()
    
    now := time.Now()
    for consumerID, lastSeen := range hm.lastHeartbeats {
        if now.Sub(lastSeen) > hm.sessionTimeout {
            // Consumer timeout - trigger rebalance
            hm.handleConsumerFailure(consumerID)
            delete(hm.lastHeartbeats, consumerID)
        }
    }
}

func (hm *HeartbeatManager) UpdateHeartbeat(consumerID ConsumerID) {
    hm.mu.Lock()
    defer hm.mu.Unlock()
    hm.lastHeartbeats[consumerID] = time.Now()
}
```

## 5. Network Protocol Design

### 5.1 Binary Protocol Specification

**Message Wire Format**:
```
Kafka-style Binary Protocol:

Request Header (12 bytes):
┌─────────────┬─────────────┬─────────────┬─────────────┐
│ Message Len │ API Key     │ API Version │ Correlation │
│ (4 bytes)   │ (2 bytes)   │ (2 bytes)   │ ID (4 bytes)│
└─────────────┴─────────────┴─────────────┴─────────────┘

Message Body (variable):
┌─────────────┬─────────────┬─────────────┬─────────────┐
│ Client ID   │ Payload     │             │             │
│ (variable)  │ (variable)  │   ...       │             │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

**Protocol Implementation**:
```go
type RequestHeader struct {
    MessageLength   int32
    APIKey         int16
    APIVersion     int16
    CorrelationID  int32
    ClientID       string
}

type ProduceRequest struct {
    Header      RequestHeader
    TopicName   string
    Partition   int32
    Messages    []Message
    RequiredAcks int16
    Timeout     int32
}

func (pr *ProduceRequest) Encode() []byte {
    buf := &bytes.Buffer{}
    
    // Write header
    binary.Write(buf, binary.BigEndian, pr.Header.APIKey)
    binary.Write(buf, binary.BigEndian, pr.Header.APIVersion)
    binary.Write(buf, binary.BigEndian, pr.Header.CorrelationID)
    
    // Write strings with length prefix
    writeString(buf, pr.Header.ClientID)
    writeString(buf, pr.TopicName)
    
    // Write partition and message array
    binary.Write(buf, binary.BigEndian, pr.Partition)
    writeMessageArray(buf, pr.Messages)
    
    // Prepend total length
    data := buf.Bytes()
    length := int32(len(data))
    result := make([]byte, 4+len(data))
    binary.BigEndian.PutUint32(result[0:4], uint32(length))
    copy(result[4:], data)
    
    return result
}
```

### 5.2 Connection Management

**Connection Pooling**:
```go
type ConnectionPool struct {
    brokers     map[BrokerID]string // broker -> address mapping
    connections map[BrokerID]*ConnectionWrapper
    maxConns    int
    timeout     time.Duration
    mu          sync.RWMutex
}

type ConnectionWrapper struct {
    conn        net.Conn
    lastUsed    time.Time
    inUse       bool
    mu          sync.Mutex
}

func (cp *ConnectionPool) GetConnection(brokerID BrokerID) (*ConnectionWrapper, error) {
    cp.mu.Lock()
    defer cp.mu.Unlock()
    
    // Check if connection exists and is healthy
    if wrapper, exists := cp.connections[brokerID]; exists {
        if cp.isHealthy(wrapper) {
            wrapper.lastUsed = time.Now()
            wrapper.inUse = true
            return wrapper, nil
        }
        // Remove unhealthy connection
        delete(cp.connections, brokerID)
    }
    
    // Create new connection
    address := cp.brokers[brokerID]
    conn, err := net.DialTimeout("tcp", address, cp.timeout)
    if err != nil {
        return nil, err
    }
    
    wrapper := &ConnectionWrapper{
        conn:     conn,
        lastUsed: time.Now(),
        inUse:    true,
    }
    cp.connections[brokerID] = wrapper
    
    return wrapper, nil
}

func (cp *ConnectionPool) isHealthy(wrapper *ConnectionWrapper) bool {
    // Simple health check - could be enhanced
    return wrapper.conn != nil && !wrapper.inUse
}
```

### 5.3 Compression Support

**Message Compression Algorithms**:
```go
type CompressionType byte

const (
    NoCompression CompressionType = iota
    Gzip
    Snappy
    LZ4
    ZSTD
)

type Compressor interface {
    Compress(data []byte) ([]byte, error)
    Decompress(data []byte) ([]byte, error)
    Type() CompressionType
}

// GZIP Compressor
type GzipCompressor struct{}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
    var buf bytes.Buffer
    writer := gzip.NewWriter(&buf)
    
    _, err := writer.Write(data)
    if err != nil {
        return nil, err
    }
    
    err = writer.Close()
    if err != nil {
        return nil, err
    }
    
    return buf.Bytes(), nil
}

func (g *GzipCompressor) Decompress(data []byte) ([]byte, error) {
    reader, err := gzip.NewReader(bytes.NewReader(data))
    if err != nil {
        return nil, err
    }
    defer reader.Close()
    
    return ioutil.ReadAll(reader)
}

// Batch compression for better ratios
type BatchCompressor struct {
    compressor Compressor
    threshold  int
}

func (bc *BatchCompressor) CompressBatch(messages []Message) ([]byte, error) {
    // Only compress if batch is large enough
    totalSize := 0
    for _, msg := range messages {
        totalSize += len(msg.Value)
    }
    
    if totalSize < bc.threshold {
        return bc.serializeUncompressed(messages), nil
    }
    
    // Serialize all messages
    serialized := bc.serializeBatch(messages)
    
    // Compress the entire batch
    return bc.compressor.Compress(serialized)
}
```

## 6. Storage Engine Design

### 6.1 Log-Structured Storage

**Segment-based Storage Model**:
```go
type LogSegment struct {
    baseOffset    int64
    file          *os.File
    index         *IndexFile
    maxSize       int64
    currentSize   int64
    isReadOnly    bool
    mu            sync.RWMutex
}

type StorageEngine struct {
    segments      []*LogSegment
    activeSegment *LogSegment
    baseDir       string
    segmentSize   int64
    mu            sync.RWMutex
}

func (se *StorageEngine) Append(message Message) (int64, error) {
    se.mu.Lock()
    defer se.mu.Unlock()
    
    // Check if current segment is full
    if se.activeSegment.currentSize >= se.activeSegment.maxSize {
        err := se.rollSegment()
        if err != nil {
            return -1, err
        }
    }
    
    // Serialize message
    data := message.Serialize()
    
    // Write to active segment
    offset, err := se.activeSegment.Append(data)
    if err != nil {
        return -1, err
    }
    
    // Update index
    se.activeSegment.index.AddEntry(offset, se.activeSegment.currentSize)
    se.activeSegment.currentSize += int64(len(data))
    
    return offset, nil
}

func (se *StorageEngine) rollSegment() error {
    // Mark current segment as read-only
    se.activeSegment.isReadOnly = true
    
    // Create new active segment
    nextOffset := se.activeSegment.baseOffset + se.getSegmentMessageCount()
    newSegment, err := NewLogSegment(se.baseDir, nextOffset, se.segmentSize)
    if err != nil {
        return err
    }
    
    se.segments = append(se.segments, se.activeSegment)
    se.activeSegment = newSegment
    
    return nil
}
```

### 6.2 Indexing Strategy

**Sparse Index for fast lookup**:
```go
type IndexEntry struct {
    Offset       int64
    PhysicalPos  int64
    Timestamp    int64
}

type SparseIndex struct {
    entries     []IndexEntry
    indexFile   *os.File
    interval    int  // Index every N messages
    mu          sync.RWMutex
}

func (si *SparseIndex) AddEntry(offset, physicalPos int64) {
    si.mu.Lock()
    defer si.mu.Unlock()
    
    // Only add entries at intervals
    if offset % int64(si.interval) == 0 {
        entry := IndexEntry{
            Offset:      offset,
            PhysicalPos: physicalPos,
            Timestamp:   time.Now().UnixNano(),
        }
        
        si.entries = append(si.entries, entry)
        si.writeEntryToDisk(entry)
    }
}

func (si *SparseIndex) FindPosition(targetOffset int64) (int64, error) {
    si.mu.RLock()
    defer si.mu.RUnlock()
    
    // Binary search for closest entry
    idx := sort.Search(len(si.entries), func(i int) bool {
        return si.entries[i].Offset >= targetOffset
    })
    
    if idx == 0 {
        return 0, nil
    }
    
    // Return position of previous entry
    return si.entries[idx-1].PhysicalPos, nil
}

// Time-based index for retention
type TimeIndex struct {
    entries []TimeIndexEntry
    mu      sync.RWMutex
}

type TimeIndexEntry struct {
    Timestamp int64
    Offset    int64
}

func (ti *TimeIndex) FindOffsetByTime(timestamp int64) int64 {
    ti.mu.RLock()
    defer ti.mu.RUnlock()
    
    idx := sort.Search(len(ti.entries), func(i int) bool {
        return ti.entries[i].Timestamp >= timestamp
    })
    
    if idx >= len(ti.entries) {
        return -1
    }
    
    return ti.entries[idx].Offset
}
```

### 6.3 Retention Policies

**Time and Size-based Retention**:
```go
type RetentionPolicy struct {
    MaxAge     time.Duration
    MaxSize    int64
    CheckInterval time.Duration
}

type RetentionManager struct {
    policy   RetentionPolicy
    storage  *StorageEngine
    running  bool
    mu       sync.Mutex
}

func (rm *RetentionManager) Start() {
    rm.mu.Lock()
    defer rm.mu.Unlock()
    
    if rm.running {
        return
    }
    
    rm.running = true
    go rm.cleanupLoop()
}

func (rm *RetentionManager) cleanupLoop() {
    ticker := time.NewTicker(rm.policy.CheckInterval)
    defer ticker.Stop()
    
    for range ticker.C {
        rm.performCleanup()
    }
}

func (rm *RetentionManager) performCleanup() {
    // Time-based cleanup
    cutoffTime := time.Now().Add(-rm.policy.MaxAge)
    rm.removeSegmentsOlderThan(cutoffTime)
    
    // Size-based cleanup
    rm.removeSegmentsExceedingSize()
}

func (rm *RetentionManager) removeSegmentsOlderThan(cutoff time.Time) {
    rm.storage.mu.Lock()
    defer rm.storage.mu.Unlock()
    
    var toRemove []*LogSegment
    
    for _, segment := range rm.storage.segments {
        if segment.isReadOnly {
            // Check oldest message in segment
            if segment.getOldestTimestamp().Before(cutoff) {
                toRemove = append(toRemove, segment)
            }
        }
    }
    
    // Remove old segments
    for _, segment := range toRemove {
        rm.removeSegment(segment)
    }
}
```

## 7. Monitoring and Observability

### 7.1 Metrics Collection

**Comprehensive Metrics System**:
```go
type MetricsCollector struct {
    // Throughput metrics
    messagesProduced   prometheus.Counter
    messagesConsumed   prometheus.Counter
    bytesProduced      prometheus.Counter
    bytesConsumed      prometheus.Counter
    
    // Latency metrics
    produceLatency     prometheus.Histogram
    consumeLatency     prometheus.Histogram
    replicationLatency prometheus.Histogram
    
    // System metrics
    activeConnections  prometheus.Gauge
    partitionCount     prometheus.Gauge
    brokerHealth       prometheus.GaugeVec
    
    // Error metrics
    produceErrors      prometheus.CounterVec
    consumeErrors      prometheus.CounterVec
    replicationErrors  prometheus.CounterVec
}

func NewMetricsCollector() *MetricsCollector {
    return &MetricsCollector{
        messagesProduced: prometheus.NewCounter(prometheus.CounterOpts{
            Name: "kafka_analog_messages_produced_total",
            Help: "Total number of messages produced",
        }),
        
        produceLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
            Name: "kafka_analog_produce_latency_seconds",
            Help: "Latency of produce operations",
            Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
        }),
        
        brokerHealth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
            Name: "kafka_analog_broker_health",
            Help: "Health status of brokers (1=healthy, 0=unhealthy)",
        }, []string{"broker_id"}),
    }
}

// Usage in hot path
func (p *Producer) sendMessage(msg Message) error {
    start := time.Now()
    defer func() {
        p.metrics.produceLatency.Observe(time.Since(start).Seconds())
        p.metrics.messagesProduced.Inc()
        p.metrics.bytesProduced.Add(float64(len(msg.Value)))
    }()
    
    return p.doSendMessage(msg)
}
```

### 7.2 Health Checks

**Multi-level Health Monitoring**:
```go
type HealthChecker struct {
    brokers map[BrokerID]*BrokerHealth
    mu      sync.RWMutex
}

type BrokerHealth struct {
    ID           BrokerID
    Status       HealthStatus
    LastSeen     time.Time
    ResponseTime time.Duration
    ErrorCount   int
    Partitions   []PartitionHealth
}

type HealthStatus int
const (
    Healthy HealthStatus = iota
    Degraded
    Unhealthy
    Unknown
)

func (hc *HealthChecker) CheckBrokerHealth(brokerID BrokerID) HealthStatus {
    start := time.Now()
    
    // TCP connection check
    conn, err := net.DialTimeout("tcp", hc.getBrokerAddress(brokerID), 5*time.Second)
    if err != nil {
        hc.updateBrokerHealth(brokerID, Unhealthy, 0, err)
        return Unhealthy
    }
    defer conn.Close()
    
    responseTime := time.Since(start)
    
    // Application-level health check
    healthy := hc.pingBroker(conn)
    if !healthy {
        hc.updateBrokerHealth(brokerID, Degraded, responseTime, nil)
        return Degraded
    }
    
    hc.updateBrokerHealth(brokerID, Healthy, responseTime, nil)
    return Healthy
}

func (hc *HealthChecker) pingBroker(conn net.Conn) bool {
    // Send ping request
    ping := &PingRequest{RequestID: generateRequestID()}
    data, _ := ping.Serialize()
    
    conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
    _, err := conn.Write(data)
    if err != nil {
        return false
    }
    
    // Read pong response
    response := make([]byte, 1024)
    conn.SetReadDeadline(time.Now().Add(2 * time.Second))
    n, err := conn.Read(response)
    if err != nil {
        return false
    }
    
    return hc.validatePongResponse(response[:n], ping.RequestID)
}
```

### 7.3 Distributed Tracing

**OpenTelemetry Integration**:
```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/trace"
    "go.opentelemetry.io/otel/attribute"
)

type TracingProducer struct {
    producer Producer
    tracer   trace.Tracer
}

func NewTracingProducer(producer Producer) *TracingProducer {
    return &TracingProducer{
        producer: producer,
        tracer:   otel.Tracer("kafka-analog-producer"),
    }
}

func (tp *TracingProducer) SendMessage(ctx context.Context, msg Message) error {
    // Start span
    ctx, span := tp.tracer.Start(ctx, "producer.send_message",
        trace.WithAttributes(
            attribute.String("topic", msg.Topic),
            attribute.Int("partition", msg.Partition),
            attribute.Int("message.size", len(msg.Value)),
        ),
    )
    defer span.End()
    
    // Inject trace context into message headers
    tp.injectTraceContext(ctx, &msg)
    
    // Send message
    err := tp.producer.SendMessage(ctx, msg)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
        return err
    }
    
    span.SetStatus(codes.Ok, "message sent successfully")
    return nil
}

func (tp *TracingProducer) injectTraceContext(ctx context.Context, msg *Message) {
    if msg.Headers == nil {
        msg.Headers = make(map[string]string)
    }
    
    // Inject OpenTelemetry context
    otel.GetTextMapPropagator().Inject(ctx, &MessageHeaderCarrier{msg.Headers})
}

// Consumer side tracing
type TracingConsumer struct {
    consumer Consumer
    tracer   trace.Tracer
}

func (tc *TracingConsumer) ConsumeMessage(ctx context.Context, msg Message) error {
    // Extract trace context from headers
    parentCtx := otel.GetTextMapPropagator().Extract(ctx, &MessageHeaderCarrier{msg.Headers})
    
    // Start child span
    ctx, span := tc.tracer.Start(parentCtx, "consumer.process_message",
        trace.WithAttributes(
            attribute.String("topic", msg.Topic),
            attribute.Int("partition", msg.Partition),
            attribute.Int64("offset", msg.Offset),
        ),
    )
    defer span.End()
    
    // Process message
    err := tc.consumer.ProcessMessage(ctx, msg)
    if err != nil {
        span.RecordError(err)
        span.SetStatus(codes.Error, err.Error())
        return err
    }
    
    span.SetStatus(codes.Ok, "message processed successfully")
    return nil
}
```

---

## Main Components

### 1. Producer Part
The producer is responsible for publishing messages to specific topics:

- **Message Publishing**: Send messages to specific topics
- **Partitioning Strategy**: 
  - Round-robin distribution
  - Key-based partitioning
  - Custom partition selector
- **Batching**: Group messages for efficient transmission
- **Acknowledgment Modes**:
  - Fire-and-forget (no ack)
  - Leader acknowledgment
  - Full replication acknowledgment
- **Serialization**: Support multiple data formats (JSON, Binary, Avro-like)
- **Retry Logic**: Handle failed sends with configurable retries

### 2. Consumer Part
The consumer handles message consumption and processing:

- **Consumer Groups**: Multiple consumers can form groups for workload distribution
- **Consumption Strategies**:
  - **At-most-once**: Read and commit immediately (possible message loss)
  - **At-least-once**: Read, process, then commit (possible duplicates)
  - **Exactly-once**: Transactional processing (idempotent operations)
- **Offset Management**:
  - Auto-commit offsets
  - Manual offset control
  - Offset reset policies (earliest, latest, specific offset)
- **Pull-based Model**: Consumers pull messages at their own pace
- **Consumer Rebalancing**: Automatic partition reassignment when consumers join/leave

### 3. Broker System
The core messaging infrastructure:

- **Broker Management**:
  - Fixed pool of 5 brokers
  - Console commands for start/stop
  - Basic health monitoring (alive/dead)
- **Leader Election**: Simple round-robin for partition leaders
- **Replication**: Fixed 2 replicas per partition
- **Storage**: In-memory with optional disk persistence
- **Metadata Management**: Track topics, partitions, and consumer groups

### 4. Topics and Partitions (Sharding)
The data organization layer:

- **Topic Management**:
  - Create/delete topics
  - Configure retention policies (time/size based)
  - Topic-level configurations
- **Partition Strategy**:
  - Multiple partitions per topic
  - Partition distribution across brokers
  - Partition rebalancing
- **Sharding Between Instances**:
  - Consistent hashing for partition assignment
  - Load balancing across brokers
  - Partition migration for scaling

## How Topics and Partitioning Work

Topics are logical channels where messages are published. Each topic is divided into partitions:

```
Topic: user-events
├── Partition 0 (Leader: Broker 1, Replicas: Broker 2, 3)
├── Partition 1 (Leader: Broker 2, Replicas: Broker 3, 4)
├── Partition 2 (Leader: Broker 3, Replicas: Broker 4, 5)
└── Partition 3 (Leader: Broker 4, Replicas: Broker 5, 1)
```

### Partition Selection

For partition/server selection, the system uses consistent hashing strategies:

- **Key-based partitioning**: `hash(message_key) % partition_count`
- **Round-robin**: Distributes messages evenly across partitions
- **Custom partitioner**: User-defined logic for partition selection

## Scalability and Multiple Servers

The system scales horizontally by:

1. **Adding more brokers**: New brokers join the cluster and take over partitions
2. **Partition redistribution**: Coordinator reassigns partitions to balance load
3. **Consumer group scaling**: Add/remove consumers to handle varying load

```go
// Example: Coordinator managing 5 brokers
type ServiceManager struct {
    brokers  [5]*os.Process
    producer *os.Process
    consumer *os.Process
}

func (m *ServiceManager) StartAll() {
    // Start 5 brokers on ports 9092-9096
    for i := 0; i < 5; i++ {
        port := 9092 + i
        cmd := exec.Command("./bin/broker", 
            "--id", fmt.Sprintf("%d", i),
            "--port", fmt.Sprintf("%d", port))
        cmd.Start()
        m.brokers[i] = cmd.Process
    }
}
```

## Consumer Connection and Usage

Consumers connect to the system by:

1. **Joining a consumer group**: Multiple consumers share the workload
2. **Subscribing to topics**: Specify which topics to consume from
3. **Offset management**: Track processing progress

```javascript
// Node.js Consumer Example
const consumerConfig = {
    groupId: 'my-consumer-group',
    topics: ['user-events', 'system-logs'],
    fromBeginning: false,
    sessionTimeout: 10000,
    heartbeatInterval: 3000
};

consumer.on('message', async (message) => {
    try {
        await processMessage(message);
        await consumer.commitOffset(message.offset);
    } catch (error) {
        await handleError(error, message);
    }
});
```

## Message Processing Styles

### FIFO (First In, First Out)
- **Single partition**: Guarantees message order
- **Sequential processing**: Messages processed in order
- **Use case**: Financial transactions, audit logs

### General/Parallel Processing
- **Multiple partitions**: Higher throughput, no order guarantee
- **Parallel consumption**: Multiple consumers process simultaneously
- **Use case**: Analytics, metrics collection, user activity tracking

```
FIFO Style:
Producer → [Partition 0] → Consumer (ordered)

General Style:  
Producer → [Partition 0] → Consumer 1
        → [Partition 1] → Consumer 2  
        → [Partition 2] → Consumer 3
```

## Comparison with Other Message Queues

| Feature | Our System | NATS | RabbitMQ | AWS SQS | AWS SNS |
|---------|------------|------|----------|---------|---------|
| **Architecture** | Distributed logs | Pub/Sub | AMQP broker | Managed queue | Pub/Sub |
| **Ordering** | Per-partition | No | Optional | FIFO queues | No |
| **Persistence** | Optional disk | Memory/disk | Disk | Managed | N/A |
| **Scaling** | Horizontal | Horizontal | Vertical+ | Auto | Auto |
| **Delivery** | At-least-once+ | At-most-once | Various | At-least-once | At-least-once |
| **Use Cases** | Event streaming | Real-time msgs | Task queues | Decoupling | Notifications |

### When to Choose Each:

- **Our Kafka Analog**: Event streaming, audit logs, high-throughput data pipelines
- **NATS**: Real-time communication, IoT, microservices messaging
- **RabbitMQ**: Complex routing, task queues, legacy system integration
- **AWS SQS**: Simple queues, AWS ecosystem, serverless applications
- **AWS SNS**: Fan-out notifications, mobile push, email/SMS alerts

## Data Flow

```
Producer → Partition Selection → Broker Leader → Replication → Storage
                                                                  |
Consumer ← Offset Management ← Partition Reader ← Consumer Group ←
```

## Message Format

```
Header: [Magic Byte | Version | Timestamp | Key Length | Value Length | Checksum]
Payload: [Key | Value | Headers (optional)]
```

---

# Development Plan

Based on Plan.md, here's the implementation roadmap:

## 1) Short Description of Queue Components
- **Producer**: Handles message publishing and batching
- **Handlings**: Message processing, serialization, partitioning
- **Consumer**: Message consumption with FIFO and general processing modes
- **Throughput**: Optimized for high-volume message processing

## 2) Topics and Partitioning Implementation
- Implement topic creation and management
- Develop partitioning strategies (round-robin, key-based, custom)
- Create partition distribution across multiple brokers

## 3) Partition/Server Selection and Distribution
- Implement consistent hashing for partition assignment
- Add partition distribution strategies
- Create partition migration tools for scaling

## 4) Horizontal Scaling Architecture
- Design multi-server coordinator system
- Implement automatic broker discovery and registration
- Add partition rebalancing when brokers join/leave

## 5) Consumer Connection Patterns
- Implement consumer group coordination
- Add support for multiple consumption patterns (FIFO/general)
- Create consumer rebalancing algorithms

## 6) Processing Style Implementation
- **FIFO Mode**: Single partition, ordered processing
- **General Mode**: Multi-partition, parallel processing
- Configuration options for choosing processing style

## 7) Comparison Analysis
- Benchmark against NATS, RabbitMQ, SQS, SNS
- Document use case recommendations
- Create migration guides from other systems