package console

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ronin/services/coordinator/internal/brokers"
	"github.com/ronin/services/coordinator/internal/metadata"
)

type Manager struct {
	brokerManager *brokers.ServiceManager
	metadataStore *metadata.Store
	running       bool
}

func NewManager(brokerMgr *brokers.ServiceManager, store *metadata.Store) *Manager {
	return &Manager{
		brokerManager: brokerMgr,
		metadataStore: store,
		running:       false,
	}
}

func (m *Manager) Start() {
	m.running = true
	m.printHelp()
	
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	
	for m.running && scanner.Scan() {
		command := strings.TrimSpace(scanner.Text())
		if command == "" {
			fmt.Print("> ")
			continue
		}
		
		m.handleCommand(command)
		
		if m.running {
			fmt.Print("> ")
		}
	}
}

func (m *Manager) Stop() {
	m.running = false
	fmt.Println("Console stopped")
}

func (m *Manager) handleCommand(command string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}
	
	switch parts[0] {
	case "help", "h":
		m.printHelp()
		
	case "status":
		m.showStatus()
		
	case "broker":
		m.handleBrokerCommand(parts[1:])
		
	case "producer":
		m.handleProducerCommand(parts[1:])
		
	case "consumer":
		m.handleConsumerCommand(parts[1:])
		
	case "restart":
		m.handleRestartCommand(parts[1:])
		
	case "metadata":
		m.handleMetadataCommand(parts[1:])
		
	case "topic":
		m.handleTopicCommand(parts[1:])
		
	case "exit", "quit":
		fmt.Println("Shutting down coordinator...")
		m.running = false
		
	default:
		fmt.Printf("Unknown command: %s. Type 'help' for available commands.\n", parts[0])
	}
}

func (m *Manager) printHelp() {
	fmt.Println(`
Kafka Analog Coordinator Console Commands:

Service Management:
  status                    - Show all services and their status
  broker list              - Show all 5 brokers
  broker start <id>        - Start broker by ID (0-4)
  broker stop <id>         - Stop broker by ID (0-4)
  broker restart <id>      - Restart broker by ID (0-4)
  producer start           - Start producer service
  producer stop            - Stop producer service
  producer restart         - Restart producer service
  consumer start           - Start consumer service
  consumer stop            - Stop consumer service
  consumer restart         - Restart consumer service
  restart all              - Restart all services

Metadata Management:
  metadata brokers         - Show broker metadata
  metadata topics          - Show topic metadata
  metadata groups          - Show consumer group metadata

Topic Management:
  topic list               - List all topics
  topic create <name>      - Create a new topic with default settings
  topic info <name>        - Show topic information

General:
  help, h                  - Show this help message
  exit, quit               - Shutdown coordinator
	`)
}

func (m *Manager) showStatus() {
	status := m.brokerManager.GetStatus()
	
	fmt.Println("\n=== Service Status ===")
	
	// Show brokers
	fmt.Println("Brokers:")
	brokers := status["brokers"].([]map[string]interface{})
	for _, broker := range brokers {
		id := broker["id"].(int)
		port := broker["port"].(int)
		running := broker["running"].(bool)
		
		statusStr := "STOPPED"
		pidStr := ""
		if running {
			statusStr = "RUNNING"
			if pid, exists := broker["pid"]; exists {
				pidStr = fmt.Sprintf(" (PID: %d)", pid)
			}
		}
		
		fmt.Printf("  Broker %d [port %d]: %s%s\n", id, port, statusStr, pidStr)
	}
	
	// Show producer
	producer := status["producer"].(map[string]interface{})
	producerStatus := "STOPPED"
	producerPid := ""
	if producer["running"].(bool) {
		producerStatus = "RUNNING"
		if pid, exists := producer["pid"]; exists {
			producerPid = fmt.Sprintf(" (PID: %d)", pid)
		}
	}
	fmt.Printf("Producer [port 9093]: %s%s\n", producerStatus, producerPid)
	
	// Show consumer
	consumer := status["consumer"].(map[string]interface{})
	consumerStatus := "STOPPED"
	consumerPid := ""
	if consumer["running"].(bool) {
		consumerStatus = "RUNNING"
		if pid, exists := consumer["pid"]; exists {
			consumerPid = fmt.Sprintf(" (PID: %d)", pid)
		}
	}
	fmt.Printf("Consumer [port 9094]: %s%s\n", consumerStatus, consumerPid)
	
	fmt.Println()
}

func (m *Manager) handleBrokerCommand(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: broker <list|start|stop|restart> [id]")
		return
	}
	
	switch args[0] {
	case "list":
		m.showStatus()
		
	case "start":
		if len(args) < 2 {
			fmt.Println("Usage: broker start <id>")
			return
		}
		id, err := strconv.Atoi(args[1])
		if err != nil || id < 0 || id >= 5 {
			fmt.Println("Invalid broker ID. Must be 0-4.")
			return
		}
		if err := m.brokerManager.StartBroker(id); err != nil {
			fmt.Printf("Error starting broker %d: %v\n", id, err)
		}
		
	case "stop":
		if len(args) < 2 {
			fmt.Println("Usage: broker stop <id>")
			return
		}
		id, err := strconv.Atoi(args[1])
		if err != nil || id < 0 || id >= 5 {
			fmt.Println("Invalid broker ID. Must be 0-4.")
			return
		}
		if err := m.brokerManager.StopBroker(id); err != nil {
			fmt.Printf("Error stopping broker %d: %v\n", id, err)
		}
		
	case "restart":
		if len(args) < 2 {
			fmt.Println("Usage: broker restart <id>")
			return
		}
		id, err := strconv.Atoi(args[1])
		if err != nil || id < 0 || id >= 5 {
			fmt.Println("Invalid broker ID. Must be 0-4.")
			return
		}
		if err := m.brokerManager.StopBroker(id); err != nil {
			fmt.Printf("Warning: Error stopping broker %d: %v\n", id, err)
		}
		if err := m.brokerManager.StartBroker(id); err != nil {
			fmt.Printf("Error starting broker %d: %v\n", id, err)
		}
		
	default:
		fmt.Printf("Unknown broker command: %s\n", args[0])
	}
}

func (m *Manager) handleProducerCommand(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: producer <start|stop|restart>")
		return
	}
	
	switch args[0] {
	case "start":
		if err := m.brokerManager.StartProducer(); err != nil {
			fmt.Printf("Error starting producer: %v\n", err)
		}
		
	case "stop":
		if err := m.brokerManager.StopProducer(); err != nil {
			fmt.Printf("Error stopping producer: %v\n", err)
		}
		
	case "restart":
		if err := m.brokerManager.StopProducer(); err != nil {
			fmt.Printf("Warning: Error stopping producer: %v\n", err)
		}
		if err := m.brokerManager.StartProducer(); err != nil {
			fmt.Printf("Error starting producer: %v\n", err)
		}
		
	default:
		fmt.Printf("Unknown producer command: %s\n", args[0])
	}
}

func (m *Manager) handleConsumerCommand(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: consumer <start|stop|restart>")
		return
	}
	
	switch args[0] {
	case "start":
		if err := m.brokerManager.StartConsumer(); err != nil {
			fmt.Printf("Error starting consumer: %v\n", err)
		}
		
	case "stop":
		if err := m.brokerManager.StopConsumer(); err != nil {
			fmt.Printf("Error stopping consumer: %v\n", err)
		}
		
	case "restart":
		if err := m.brokerManager.StopConsumer(); err != nil {
			fmt.Printf("Warning: Error stopping consumer: %v\n", err)
		}
		if err := m.brokerManager.StartConsumer(); err != nil {
			fmt.Printf("Error starting consumer: %v\n", err)
		}
		
	default:
		fmt.Printf("Unknown consumer command: %s\n", args[0])
	}
}

func (m *Manager) handleRestartCommand(args []string) {
	if len(args) == 0 || args[0] != "all" {
		fmt.Println("Usage: restart all")
		return
	}
	
	if err := m.brokerManager.RestartAll(); err != nil {
		fmt.Printf("Error restarting services: %v\n", err)
	}
}

func (m *Manager) handleMetadataCommand(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: metadata <brokers|topics|groups>")
		return
	}
	
	switch args[0] {
	case "brokers":
		brokers := m.metadataStore.GetAllBrokers()
		fmt.Println("\n=== Broker Metadata ===")
		for id, broker := range brokers {
			fmt.Printf("Broker %d: %s:%d [%s] (last seen: %s)\n",
				id, broker.Host, broker.Port, broker.Status, broker.LastSeen.Format("15:04:05"))
		}
		fmt.Println()
		
	case "topics":
		topics := m.metadataStore.GetAllTopics()
		fmt.Println("\n=== Topic Metadata ===")
		if len(topics) == 0 {
			fmt.Println("No topics found")
		}
		for name, topic := range topics {
			fmt.Printf("Topic: %s (partitions: %d, replication: %d)\n",
				name, topic.Config.PartitionCount, topic.Config.ReplicationFactor)
			for _, partition := range topic.Partitions {
				fmt.Printf("  Partition %d: leader=%d, replicas=%v, isr=%v\n",
					partition.ID, partition.Leader, partition.Replicas, partition.ISR)
			}
		}
		fmt.Println()
		
	case "groups":
		fmt.Println("\n=== Consumer Group Metadata ===")
		fmt.Println("Consumer groups not implemented yet")
		fmt.Println()
		
	default:
		fmt.Printf("Unknown metadata command: %s\n", args[0])
	}
}

func (m *Manager) handleTopicCommand(args []string) {
	if len(args) == 0 {
		fmt.Println("Usage: topic <list|create|info> [name]")
		return
	}
	
	switch args[0] {
	case "list":
		topics := m.metadataStore.GetAllTopics()
		fmt.Println("\n=== Topics ===")
		if len(topics) == 0 {
			fmt.Println("No topics found")
		}
		for name, topic := range topics {
			fmt.Printf("%s (partitions: %d, replication: %d)\n",
				name, topic.Config.PartitionCount, topic.Config.ReplicationFactor)
		}
		fmt.Println()
		
	case "create":
		if len(args) < 2 {
			fmt.Println("Usage: topic create <name>")
			return
		}
		config := metadata.TopicConfig{
			PartitionCount:    10,
			ReplicationFactor: 2,
			RetentionTime:     24 * 7 * 1000000000, // 7 days in nanoseconds
			RetentionSize:     1073741824,          // 1GB
		}
		if err := m.metadataStore.CreateTopic(args[1], config); err != nil {
			fmt.Printf("Error creating topic: %v\n", err)
		} else {
			fmt.Printf("Topic '%s' created successfully\n", args[1])
		}
		
	case "info":
		if len(args) < 2 {
			fmt.Println("Usage: topic info <name>")
			return
		}
		topic, exists := m.metadataStore.GetTopic(args[1])
		if !exists {
			fmt.Printf("Topic '%s' not found\n", args[1])
			return
		}
		fmt.Printf("\n=== Topic: %s ===\n", args[1])
		fmt.Printf("Partitions: %d\n", topic.Config.PartitionCount)
		fmt.Printf("Replication Factor: %d\n", topic.Config.ReplicationFactor)
		fmt.Printf("Retention Time: %v\n", topic.Config.RetentionTime)
		fmt.Printf("Retention Size: %d bytes\n", topic.Config.RetentionSize)
		fmt.Println("\nPartition Details:")
		for _, partition := range topic.Partitions {
			fmt.Printf("  Partition %d: leader=%d, replicas=%v, isr=%v\n",
				partition.ID, partition.Leader, partition.Replicas, partition.ISR)
		}
		fmt.Println()
		
	default:
		fmt.Printf("Unknown topic command: %s\n", args[0])
	}
}