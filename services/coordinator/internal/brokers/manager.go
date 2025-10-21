package brokers

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/ronin/services/coordinator/internal/metadata"
)

type ServiceManager struct {
	mu           sync.RWMutex
	brokers      [5]*os.Process
	producer     *os.Process
	consumer     *os.Process
	metadataStore *metadata.Store
}

func NewManager(store *metadata.Store) *ServiceManager {
	return &ServiceManager{
		metadataStore: store,
	}
}

func (m *ServiceManager) StartAll() error {
	fmt.Println("Starting all services...")
	
	if err := m.startBrokers(); err != nil {
		return fmt.Errorf("failed to start brokers: %w", err)
	}
	
	if err := m.startProducer(); err != nil {
		return fmt.Errorf("failed to start producer: %w", err)
	}
	
	if err := m.startConsumer(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	
	return nil
}

func (m *ServiceManager) startBrokers() error {
	for i := 0; i < 5; i++ {
		if err := m.StartBroker(i); err != nil {
			return fmt.Errorf("failed to start broker %d: %w", i, err)
		}
	}
	return nil
}

func (m *ServiceManager) StartBroker(id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if id < 0 || id >= 5 {
		return fmt.Errorf("invalid broker ID: %d", id)
	}
	
	if m.brokers[id] != nil {
		return fmt.Errorf("broker %d is already running", id)
	}
	
	port := 9092 + id
	cmd := exec.Command("./bin/broker",
		"--id", fmt.Sprintf("%d", id),
		"--port", fmt.Sprintf("%d", port),
		"--coordinator", "localhost:8080")
	
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start broker %d: %w", id, err)
	}
	
	m.brokers[id] = cmd.Process
	
	brokerInfo := &metadata.BrokerInfo{
		ID:       id,
		Host:     "localhost",
		Port:     port,
		Status:   "running",
		LastSeen: time.Now(),
	}
	m.metadataStore.RegisterBroker(brokerInfo)
	
	fmt.Printf("Broker %d started on port %d (PID: %d)\n", id, port, cmd.Process.Pid)
	return nil
}

func (m *ServiceManager) StopBroker(id int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if id < 0 || id >= 5 {
		return fmt.Errorf("invalid broker ID: %d", id)
	}
	
	if m.brokers[id] == nil {
		return fmt.Errorf("broker %d is not running", id)
	}
	
	if err := m.brokers[id].Kill(); err != nil {
		return fmt.Errorf("failed to stop broker %d: %w", id, err)
	}
	
	m.brokers[id] = nil
	m.metadataStore.UpdateBrokerStatus(id, "stopped")
	
	fmt.Printf("Broker %d stopped\n", id)
	return nil
}

func (m *ServiceManager) startProducer() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.producer != nil {
		return fmt.Errorf("producer is already running")
	}
	
	cmd := exec.Command("./bin/producer",
		"--port", "9093",
		"--coordinator", "localhost:8080")
	
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start producer: %w", err)
	}
	
	m.producer = cmd.Process
	fmt.Printf("Producer started on port 9093 (PID: %d)\n", cmd.Process.Pid)
	return nil
}

func (m *ServiceManager) StartProducer() error {
	return m.startProducer()
}

func (m *ServiceManager) StopProducer() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.producer == nil {
		return fmt.Errorf("producer is not running")
	}
	
	if err := m.producer.Kill(); err != nil {
		return fmt.Errorf("failed to stop producer: %w", err)
	}
	
	m.producer = nil
	fmt.Println("Producer stopped")
	return nil
}

func (m *ServiceManager) startConsumer() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.consumer != nil {
		return fmt.Errorf("consumer is already running")
	}
	
	cmd := exec.Command("./bin/consumer",
		"--port", "9094",
		"--coordinator", "localhost:8080")
	
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}
	
	m.consumer = cmd.Process
	fmt.Printf("Consumer started on port 9094 (PID: %d)\n", cmd.Process.Pid)
	return nil
}

func (m *ServiceManager) StartConsumer() error {
	return m.startConsumer()
}

func (m *ServiceManager) StopConsumer() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.consumer == nil {
		return fmt.Errorf("consumer is not running")
	}
	
	if err := m.consumer.Kill(); err != nil {
		return fmt.Errorf("failed to stop consumer: %w", err)
	}
	
	m.consumer = nil
	fmt.Println("Consumer stopped")
	return nil
}

func (m *ServiceManager) RestartAll() error {
	fmt.Println("Restarting all services...")
	
	if err := m.StopAll(); err != nil {
		fmt.Printf("Warning: some services failed to stop: %v\n", err)
	}
	
	time.Sleep(2 * time.Second) // Give processes time to clean up
	
	return m.StartAll()
}

func (m *ServiceManager) StopAll() error {
	var errors []error
	
	// Stop consumer
	if err := m.StopConsumer(); err != nil && m.consumer != nil {
		errors = append(errors, err)
	}
	
	// Stop producer
	if err := m.StopProducer(); err != nil && m.producer != nil {
		errors = append(errors, err)
	}
	
	// Stop all brokers
	for i := 0; i < 5; i++ {
		if err := m.StopBroker(i); err != nil && m.brokers[i] != nil {
			errors = append(errors, err)
		}
	}
	
	if len(errors) > 0 {
		return fmt.Errorf("errors stopping services: %v", errors)
	}
	
	fmt.Println("All services stopped")
	return nil
}

func (m *ServiceManager) GetStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	status := map[string]interface{}{
		"brokers":  make([]map[string]interface{}, 5),
		"producer": map[string]interface{}{"running": m.producer != nil},
		"consumer": map[string]interface{}{"running": m.consumer != nil},
	}
	
	// Get broker status
	brokers := status["brokers"].([]map[string]interface{})
	for i := 0; i < 5; i++ {
		brokers[i] = map[string]interface{}{
			"id":      i,
			"port":    9092 + i,
			"running": m.brokers[i] != nil,
		}
		if m.brokers[i] != nil {
			brokers[i]["pid"] = m.brokers[i].Pid
		}
	}
	
	// Add PIDs for producer and consumer
	if m.producer != nil {
		status["producer"].(map[string]interface{})["pid"] = m.producer.Pid
	}
	if m.consumer != nil {
		status["consumer"].(map[string]interface{})["pid"] = m.consumer.Pid
	}
	
	return status
}