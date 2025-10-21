package metadata

import (
	"fmt"
	"sync"
	"time"
)

type BrokerInfo struct {
	ID       int    `json:"id"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Status   string `json:"status"` // "running", "stopped", "failed"
	LastSeen time.Time `json:"last_seen"`
}

type TopicInfo struct {
	Name       string      `json:"name"`
	Partitions []Partition `json:"partitions"`
	Config     TopicConfig `json:"config"`
}

type Partition struct {
	ID       int   `json:"id"`
	Leader   int   `json:"leader"`
	Replicas []int `json:"replicas"`
	ISR      []int `json:"isr"` // In-Sync Replicas
}

type TopicConfig struct {
	PartitionCount    int           `json:"partition_count"`
	ReplicationFactor int           `json:"replication_factor"`
	RetentionTime     time.Duration `json:"retention_time"`
	RetentionSize     int64         `json:"retention_size"`
}

type ConsumerGroup struct {
	ID        string            `json:"id"`
	Members   []ConsumerMember  `json:"members"`
	State     string            `json:"state"` // "stable", "rebalancing", "dead"
	Offsets   map[string]int64  `json:"offsets"` // topic-partition -> offset
}

type ConsumerMember struct {
	ID         string   `json:"id"`
	Host       string   `json:"host"`
	Partitions []string `json:"partitions"` // assigned partitions
}

type Store struct {
	mu             sync.RWMutex
	brokers        map[int]*BrokerInfo
	topics         map[string]*TopicInfo
	consumerGroups map[string]*ConsumerGroup
}

func NewStore() *Store {
	return &Store{
		brokers:        make(map[int]*BrokerInfo),
		topics:         make(map[string]*TopicInfo),
		consumerGroups: make(map[string]*ConsumerGroup),
	}
}

func (s *Store) RegisterBroker(broker *BrokerInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.brokers[broker.ID] = broker
}

func (s *Store) GetBroker(id int) (*BrokerInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	broker, exists := s.brokers[id]
	return broker, exists
}

func (s *Store) GetAllBrokers() map[int]*BrokerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[int]*BrokerInfo)
	for id, broker := range s.brokers {
		result[id] = broker
	}
	return result
}

func (s *Store) UpdateBrokerStatus(id int, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	broker, exists := s.brokers[id]
	if !exists {
		return fmt.Errorf("broker %d not found", id)
	}
	
	broker.Status = status
	broker.LastSeen = time.Now()
	return nil
}

func (s *Store) CreateTopic(name string, config TopicConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}
	
	partitions := make([]Partition, config.PartitionCount)
	brokerIDs := s.getAvailableBrokerIDs()
	
	for i := 0; i < config.PartitionCount; i++ {
		leader := brokerIDs[i%len(brokerIDs)]
		replicas := s.selectReplicas(leader, config.ReplicationFactor, brokerIDs)
		
		partitions[i] = Partition{
			ID:       i,
			Leader:   leader,
			Replicas: replicas,
			ISR:      replicas, // Initially all replicas are in-sync
		}
	}
	
	s.topics[name] = &TopicInfo{
		Name:       name,
		Partitions: partitions,
		Config:     config,
	}
	
	return nil
}

func (s *Store) GetTopic(name string) (*TopicInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topic, exists := s.topics[name]
	return topic, exists
}

func (s *Store) GetAllTopics() map[string]*TopicInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]*TopicInfo)
	for name, topic := range s.topics {
		result[name] = topic
	}
	return result
}

func (s *Store) RegisterConsumerGroup(group *ConsumerGroup) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumerGroups[group.ID] = group
}

func (s *Store) GetConsumerGroup(id string) (*ConsumerGroup, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	group, exists := s.consumerGroups[id]
	return group, exists
}

func (s *Store) getAvailableBrokerIDs() []int {
	var ids []int
	for id, broker := range s.brokers {
		if broker.Status == "running" {
			ids = append(ids, id)
		}
	}
	return ids
}

func (s *Store) selectReplicas(leader int, replicationFactor int, availableBrokers []int) []int {
	replicas := []int{leader}
	
	for i := 0; i < len(availableBrokers) && len(replicas) < replicationFactor; i++ {
		if availableBrokers[i] != leader {
			replicas = append(replicas, availableBrokers[i])
		}
	}
	
	return replicas
}