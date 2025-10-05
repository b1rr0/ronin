# Kafka Analog System - Project Specification

## Overview
Build a custom message queue system similar to Apache Kafka with core distributed messaging capabilities.

## Main Components

### 1. Producer Part
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
- **Broker Management**:
  - Fixed pool of 5 brokers
  - Console commands for start/stop
  - Basic health monitoring (alive/dead)
- **Leader Election**: Simple round-robin for partition leaders
- **Replication**: Fixed 2 replicas per partition
- **Storage**: In-memory with optional disk persistence
- **Metadata Management**: Track topics, partitions, and consumer groups

### 3.1 Coordinator Service Management (Process-based)
The Coordinator manages ALL system services:

- **What Coordinator Manages**:
  - **5 Broker Processes**: Spawns and controls broker instances (ports 9092-9096)
  - **Producer Service**: Single producer process (port 9093)
  - **Consumer Service**: Single consumer process (port 9094)
  
- **Process-Based Service Management**:
  - **Automatic Startup**: Starts all 7 processes on launch (5 brokers + producer + consumer)
  - **Process Spawning**: Uses `exec.Command()` to start all services
  - **Health Monitoring**: TCP connection checks for all services
  - **Console Control**: Commands to manage individual services

- **Console Commands**:
  ```
  > status               # Show all services and their status
  > broker list          # Show all 5 brokers
  > broker stop 3        # Stop broker #3
  > broker start 3       # Start broker #3
  > producer stop        # Stop producer service
  > producer start       # Start producer service
  > consumer stop        # Stop consumer service
  > consumer start       # Start consumer service
  > restart all          # Restart all services
  ```

- **Simple Process Management**:
  ```go
  type ServiceManager struct {
      brokers  [5]*os.Process
      producer *os.Process
      consumer *os.Process
  }
  
  func (m *ServiceManager) StartAll() {
      // Start 5 brokers
      for i := 0; i < 5; i++ {
          port := 9092 + i
          cmd := exec.Command("./bin/broker", 
              "--id", fmt.Sprintf("%d", i),
              "--port", fmt.Sprintf("%d", port))
          cmd.Start()
          m.brokers[i] = cmd.Process
      }
      
      // Start producer
      cmd := exec.Command("./bin/producer", 
          "--port", "9093",
          "--coordinator", "localhost:8080")
      cmd.Start()
      m.producer = cmd.Process
      
      // Start consumer
      cmd = exec.Command("./bin/consumer",
          "--port", "9094", 
          "--coordinator", "localhost:8080")
      cmd.Start()
      m.consumer = cmd.Process
  }
  ```

### 4. Topics and Partitions (Sharding)
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

## Technical Architecture

### Core Features to Implement

#### Phase 1: Basic Messaging
- Coordinator with 5 brokers
- Simple producer/consumer APIs
- Topic creation with partitions distributed across 5 brokers
- Console commands for broker management

#### Phase 2: Replication and Groups
- Basic replication (2 replicas per partition)
- Consumer groups with simple rebalancing
- Offset management

#### Phase 3: Persistence (Optional)
- Add disk persistence option
- Basic recovery on restart

### Data Flow
```
Producer -> Partition Selection -> Broker Leader -> Replication -> Storage
                                                                      |
Consumer <- Offset Management <- Partition Reader <- Consumer Group <-
```

### Key Design Decisions

#### Message Format
```
Header: [Magic Byte | Version | Timestamp | Key Length | Value Length | Checksum]
Payload: [Key | Value | Headers (optional)]
```

#### Partition Assignment
- Use consistent hashing ring
- Virtual nodes for better distribution
- Partition ownership stored in metadata store

#### Consumer Group Coordination
- Heartbeat mechanism for liveness
- Generation ID for rebalance epochs
- Partition assignment strategies:
  - Range assignor
  - Round-robin assignor
  - Sticky assignor (minimize reassignments)

### Implementation Priorities

1. **Week 1: Basic System**
   - Coordinator starts 5 brokers
   - Console commands (list, stop, start)
   - Simple producer/consumer
   - In-memory storage

2. **Week 2: Distribution**
   - Partition distribution across 5 brokers
   - Basic replication (2 replicas)
   - Consumer groups

3. **Week 3: Polish**
   - Offset management
   - Basic persistence (optional)
   - Testing and bug fixes

### Testing Strategy
- Unit tests for each component
- Integration tests for end-to-end flows
- Chaos testing for failure scenarios
- Performance benchmarks

### Configuration Example (config.yaml)
```yaml
coordinator:
  port: 8080
  console_port: 8081
  broker_count: 5
  broker_executable: ./bin/broker
      
broker:
  base_port: 9092  # Brokers will use ports 9092-9096
  data_dir: ./data
  storage: memory  # memory | disk
  
topics:
  default_partitions: 10  # Distributed across 5 brokers
  replication_factor: 2
  
consumer:
  session_timeout_ms: 10000
  heartbeat_interval_ms: 3000
  
producer:
  batch_size: 16384
  linger_ms: 100
  
# Node.js client configurations
nodejs_writer:
  port: 3001
  producer_url: localhost:9093
  
nodejs_reader:
  port: 3002
  consumer_url: localhost:9094
```

## Go Project Structure (Following golang-standards/project-layout)

### Microservices Architecture

The system will be composed of 4 independent microservices:

#### 1. **Producer Service** (`services/producer`)
```
services/producer/
├── cmd/
│   └── producer/
│       └── main.go              # Application entrypoint
├── internal/
│   ├── batch/                   # Message batching logic
│   ├── partitioner/              # Partition selection strategies
│   ├── serializer/               # Message serialization
│   └── sender/                   # Network sending logic
├── pkg/
│   └── producer/                 # Public producer client library
├── api/
│   └── proto/                    # Protocol buffer definitions
├── configs/
│   └── producer.yaml             # Default configuration
├── bin/                         # Compiled binaries
├── scripts/
│   └── build.sh
├── go.mod
└── go.sum
```

#### 2. **Consumer Service** (`services/consumer`)
```
services/consumer/
├── cmd/
│   └── consumer/
│       └── main.go
├── internal/
│   ├── group/                    # Consumer group management
│   ├── offset/                   # Offset tracking
│   ├── rebalance/                # Partition rebalancing
│   └── fetcher/                  # Message fetching logic
├── pkg/
│   └── consumer/                 # Public consumer client library
├── api/
│   └── proto/
├── configs/
│   └── consumer.yaml
├── bin/
│   ├── docker/
│   └── k8s/
├── go.mod
└── go.sum
```

#### 3. **Broker Service** (`services/broker`)
```
services/broker/
├── cmd/
│   └── broker/
│       └── main.go
├── internal/
│   ├── storage/                  # Log storage engine
│   │   ├── segment/              # Segment file management
│   │   └── index/                # Offset indexing
│   ├── replication/              # Replication manager
│   ├── partition/                # Partition management
│   └── controller/               # Broker controller logic
├── pkg/
│   └── broker/                   # Broker client for admin operations
├── api/
│   ├── proto/
│   └── http/                     # REST API for admin
├── configs/
│   └── broker.yaml
├── bin/
├── go.mod
└── go.sum
```

#### 4. **Coordinator Service** (`services/coordinator`)
```
services/coordinator/
├── cmd/
│   └── coordinator/
│       └── main.go
├── internal/
│   ├── metadata/                 # Cluster metadata management
│   ├── brokers/                  # Simple broker management (5 brokers)
│   ├── console/                  # Console commands handler
│   └── assignment/               # Partition assignment
├── pkg/
│   └── coordinator/
├── api/
│   └── proto/
├── configs/
│   └── coordinator.yaml
├── bin/
├── go.mod
└── go.sum
```

### Node.js Client Services

#### 5. **Writer Service (Node.js)** (`clients/nodejs-writer`)
```
clients/nodejs-writer/
├── src/
│   ├── index.js                 # Application entry point
│   ├── config/
│   │   └── config.js             # Configuration management
│   ├── producer/
│   │   ├── client.js             # gRPC client for producer
│   │   └── connection.js         # Connection management
│   ├── api/
│   │   ├── routes/
│   │   │   ├── messages.js       # Message sending endpoints
│   │   │   └── topics.js         # Topic management
│   │   └── middleware/
│   │       ├── auth.js           # Authentication
│   │       └── validation.js     # Request validation
│   ├── services/
│   │   ├── messageService.js     # Business logic for messages
│   │   └── batchService.js       # Batching logic
│   └── utils/
│       ├── logger.js              # Logging utility
│       └── metrics.js             # Metrics collection
├── proto/
│   └── producer.proto             # gRPC definitions
├── test/
│   ├── unit/
│   └── integration/
├── .env.example
├── .eslintrc.json
├── .prettierrc
├── package.json
├── package-lock.json
└── README.md
```

**package.json dependencies:**
```json
{
  "name": "nodejs-writer",
  "version": "1.0.0",
  "main": "src/index.js",
  "scripts": {
    "start": "node src/index.js",
    "dev": "nodemon src/index.js",
    "test": "jest",
    "lint": "eslint src/"
  },
  "dependencies": {
    "@grpc/grpc-js": "^1.9.0",
    "@grpc/proto-loader": "^0.7.0",
    "express": "^4.18.0",
    "dotenv": "^16.0.0",
    "joi": "^17.9.0",
    "winston": "^3.10.0",
    "prom-client": "^14.2.0"
  },
  "devDependencies": {
    "nodemon": "^3.0.0",
    "jest": "^29.6.0",
    "eslint": "^8.45.0"
  }
}
```

**Writer Service API:**
```javascript
// REST API endpoints
POST /api/v1/messages
{
  "topic": "user-events",
  "key": "user-123",
  "value": {
    "action": "login",
    "timestamp": "2024-01-20T10:00:00Z"
  },
  "headers": {
    "source": "web-app"
  }
}

POST /api/v1/messages/batch
{
  "topic": "user-events",
  "messages": [...]
}

GET /api/v1/topics
POST /api/v1/topics
```

#### 6. **Reader Service (Node.js)** (`clients/nodejs-reader`)
```
clients/nodejs-reader/
├── src/
│   ├── index.js                  # Application entry point
│   ├── config/
│   │   └── config.js              # Configuration management
│   ├── consumer/
│   │   ├── client.js              # gRPC client for consumer
│   │   ├── group.js               # Consumer group management
│   │   └── offset.js              # Offset tracking
│   ├── handlers/
│   │   ├── messageHandler.js      # Message processing logic
│   │   └── errorHandler.js        # Error handling
│   ├── api/
│   │   ├── routes/
│   │   │   ├── subscriptions.js   # Subscription management
│   │   │   ├── offsets.js         # Offset management
│   │   │   └── health.js          # Health checks
│   │   └── websocket/
│   │       └── streaming.js       # WebSocket for real-time
│   ├── processors/
│   │   ├── processor.js           # Message processor interface
│   │   └── implementations/
│   │       ├── jsonProcessor.js
│   │       └── avroProcessor.js
│   └── utils/
│       ├── logger.js
│       └── metrics.js
├── proto/
│   └── consumer.proto             # gRPC definitions
├── test/
├── .env.example
├── package.json
└── README.md
```

**package.json dependencies:**
```json
{
  "name": "nodejs-reader",
  "version": "1.0.0",
  "main": "src/index.js",
  "scripts": {
    "start": "node src/index.js",
    "dev": "nodemon src/index.js",
    "test": "jest"
  },
  "dependencies": {
    "@grpc/grpc-js": "^1.9.0",
    "@grpc/proto-loader": "^0.7.0",
    "express": "^4.18.0",
    "ws": "^8.14.0",
    "dotenv": "^16.0.0",
    "winston": "^3.10.0",
    "prom-client": "^14.2.0",
    "avsc": "^5.7.0"
  }
}
```

**Reader Service Features:**
```javascript
// Consumer configuration
const consumerConfig = {
  groupId: 'nodejs-reader-group',
  topics: ['user-events', 'system-logs'],
  fromBeginning: false,
  sessionTimeout: 10000,
  heartbeatInterval: 3000,
  autoCommit: true,
  autoCommitInterval: 5000
};

// Message processing
consumer.on('message', async (message) => {
  try {
    await processMessage(message);
    await consumer.commitOffset(message.offset);
  } catch (error) {
    await handleError(error, message);
  }
});

// WebSocket streaming
ws.on('connection', (socket) => {
  const stream = consumer.subscribe('real-time-topic');
  stream.on('data', (message) => {
    socket.send(JSON.stringify(message));
  });
});
```

### Root Project Structure
```
kafka-analog/
├── services/                     # Go microservices
│   ├── producer/
│   ├── consumer/
│   ├── broker/
│   └── coordinator/
├── clients/                      # Client applications
│   ├── nodejs-writer/            # Node.js writer service
│   └── nodejs-reader/            # Node.js reader service
├── pkg/                          # Shared libraries
│   ├── protocol/                 # Wire protocol definitions
│   ├── common/                   # Common utilities
│   ├── errors/                   # Error definitions
│   └── metrics/                  # Metrics collection
├── internal/                     # Private shared code
│   └── testutil/                 # Test utilities
├── api/                          # API definitions
│   └── openapi/                  # OpenAPI specs
├── configs/                      # Configuration files
│   └── config.yaml               # Main configuration
├── bin/                          # Compiled executables
│   ├── coordinator
│   ├── broker
│   ├── producer
│   └── consumer
├── data/                         # Data directories for brokers
│   ├── broker-0/
│   ├── broker-1/
│   ├── broker-2/
│   ├── broker-3/
│   └── broker-4/
├── scripts/                      # Build and utility scripts
│   ├── build-all.sh
│   ├── test.sh
│   └── proto-gen.sh
├── test/                         # Integration tests
│   ├── e2e/                      # End-to-end tests
│   └── performance/              # Performance tests
├── docs/                         # Documentation
│   ├── architecture/
│   ├── api/
│   └── operations/
├── examples/                     # Example applications
│   ├── simple-producer/
│   └── simple-consumer/
├── tools/                        # Supporting tools
│   └── admin-cli/                # CLI for cluster management
├── vendor/                       # Vendored dependencies (optional)
├── .gitignore
├── .golangci.yaml               # Linter configuration
├── Makefile                      # Build automation
├── README.md
├── go.mod                        # Root module (if using workspace)
└── go.work                       # Go workspace file
```

### Service Communication

#### gRPC APIs
- **Producer -> Broker**: Send messages
- **Consumer -> Broker**: Fetch messages
- **Broker -> Coordinator**: Register, heartbeat, metadata sync
- **Producer/Consumer -> Coordinator**: Discover brokers, get metadata
- **Node.js Writer -> Producer**: Send messages via gRPC
- **Node.js Reader -> Consumer**: Fetch messages via gRPC

#### Internal Protocols
```proto
// Common message format
message Message {
    bytes key = 1;
    bytes value = 2;
    int64 timestamp = 3;
    map<string, string> headers = 4;
    int32 partition = 5;
    int64 offset = 6;
}

// Producer request
message ProduceRequest {
    string topic = 1;
    repeated Message messages = 2;
    AckLevel ack_level = 3;
}

// Consumer request  
message FetchRequest {
    string topic = 1;
    int32 partition = 2;
    int64 offset = 3;
    int32 max_bytes = 4;
}
```

### Development Workflow

#### Local Development (No Docker)
```bash
# Build all services
make build

# Start coordinator (automatically starts all services)
# This will spawn:
# - 5 broker processes (ports 9092-9096)
# - 1 producer service (port 9093)
# - 1 consumer service (port 9094)
./bin/coordinator

# Start Node.js writer client (optional)
cd clients/nodejs-writer && npm start

# Start Node.js reader client (optional)
cd clients/nodejs-reader && npm start

# Run tests
make test
```

#### Running the System
```bash
# Step 1: Build everything
make build

# Step 2: Start coordinator (starts ALL services automatically)
./bin/coordinator

# Step 3: Use the console to manage services
# In coordinator console:
> status               # See all services
> broker stop 2        # Stop broker #2
> producer restart     # Restart producer
> consumer stop        # Stop consumer

# Step 4: Start Node.js clients (optional)
cd clients/nodejs-writer && npm start &
cd clients/nodejs-reader && npm start &
```

#### Service Architecture
- **Coordinator**: Central manager that starts and controls all core services
  - Spawns 5 broker processes (ports 9092-9096)
  - Spawns producer service (port 9093)
  - Spawns consumer service (port 9094)
- **Node.js Writer**: Optional client that sends data via Producer service
- **Node.js Reader**: Optional client that reads data via Consumer service

#### What Happens When You Start Coordinator
1. Coordinator starts and opens console on port 8081
2. Automatically spawns 5 broker processes
3. Automatically starts producer service
4. Automatically starts consumer service
5. All services are ready - you can optionally connect Node.js clients

### Makefile Commands
```makefile
.PHONY: all build test clean run

all: build

build:
	@echo "Building all Go services..."
	cd services/coordinator && go build -o ../../bin/coordinator cmd/coordinator/main.go
	cd services/broker && go build -o ../../bin/broker cmd/broker/main.go
	cd services/producer && go build -o ../../bin/producer cmd/producer/main.go
	cd services/consumer && go build -o ../../bin/consumer cmd/consumer/main.go
	@echo "Installing Node.js dependencies..."
	cd clients/nodejs-writer && npm install
	cd clients/nodejs-reader && npm install

run:
	./bin/coordinator  # This starts everything!

test:
	@echo "Running Go tests..."
	go test ./services/...
	@echo "Running Node.js tests..."
	cd clients/nodejs-writer && npm test
	cd clients/nodejs-reader && npm test

clean:
	rm -rf bin/*
	rm -rf data/broker-*/*

proto:
	@echo "Generating protobuf files..."
	./scripts/proto-gen.sh
```

## Next Steps
1. Initialize Go modules for each service
2. Define protobuf schemas for service communication
3. Implement coordinator service for metadata management
4. Implement broker service with basic storage
5. Create producer client library and service
6. Create consumer client library and service
7. Add integration tests
8. Implement advanced features (replication, exactly-once)
