package datapipe

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
	"data_pipe/internal/database"
	"data_pipe/internal/datapipe/active_period"
	"data_pipe/internal/datapipe/filters"
	"data_pipe/internal/datapipe/processing_policy"
	"data_pipe/internal/datapipe/transformations"
)

// NodeState stores the state for a single node
type NodeState struct {
	LastMessageTime time.Time
	LastValue       string
	mu              sync.RWMutex
}

// Processor handles message processing and configuration management
type Processor struct {
	clickhouseDB *database.ClickHouseDB
	postgresDB   *database.PostgresDB
	configs      *DataPipeConfigs
	nodeStates   map[string]*NodeState
	statesMu     sync.RWMutex
}

// NewProcessor creates a new Processor instance
func NewProcessor(clickhouseDB *database.ClickHouseDB, postgresDB *database.PostgresDB, cfg *config.Config) *Processor {
	return &Processor{
		clickhouseDB: clickhouseDB,
		postgresDB:   postgresDB,
		configs:      NewDataPipeConfigs(cfg),
		nodeStates:   make(map[string]*NodeState),
	}
}

// LoadNodeConfigs loads active node configurations from PostgreSQL
func (p *Processor) LoadNodeConfigs(ctx context.Context) error {
	return p.configs.LoadNodeConfigs(ctx, p.postgresDB)
}

// StartConfigSync starts background processes for configuration synchronization
func (p *Processor) StartConfigSync(ctx context.Context, redisDB *database.RedisDB, mqttClient *mqtt_client.MQTTClient) {
	p.configs.StartConfigSync(ctx, p.postgresDB, redisDB, mqttClient)
}

// getNodeState returns or creates a NodeState for the given node
func (p *Processor) getNodeState(nodeUUID string) *NodeState {
	p.statesMu.Lock()
	defer p.statesMu.Unlock()

	state, exists := p.nodeStates[nodeUUID]
	if !exists {
		state = &NodeState{}
		p.nodeStates[nodeUUID] = state
	}
	return state
}

// ProcessMessage processes a message from a topic
func (p *Processor) ProcessMessage(ctx context.Context, topic string, payload []byte) error {
	// Extract node UUID from topic
	nodeUUID := extractNodeUUID(topic)
	if nodeUUID == "" {
		return fmt.Errorf("invalid topic format: %s", topic)
	}

	config, exists := p.configs.Get(nodeUUID)
	if !exists {
		return nil
	}

	currentTime := time.Now()
	if !active_period.IsActive(&config.ActivePeriod, currentTime) {
		return nil
	}

	nodeState := p.getNodeState(nodeUUID)
	shouldProcess := filters.ApplyFilters(string(payload), config.Filters, nodeState.LastMessageTime, nodeState.LastValue)
	if !shouldProcess {
		return nil
	}

	transformedValue := string(payload)
	if config.Transformations != nil {
		// Apply transformations
		var err error
		transformedValue, err = transformations.ApplyTransformations(string(payload), config.Transformations, &config.Filters)
		if err != nil {
			return nil
		}
	}

	// Apply processing policy
	processing_policy.ApplyProcessingPolicy(ctx, p.postgresDB, nodeUUID, transformedValue, currentTime, config.ProcessingPolicy)

	// Update node state
	nodeState.mu.Lock()
	nodeState.LastMessageTime = currentTime
	nodeState.LastValue = transformedValue
	nodeState.mu.Unlock()

	log.Printf("Success '%s' %s", transformedValue, nodeUUID)

	// TODO: Implement actual message processing logic
	return nil
}

// extractNodeUUID extracts the node UUID from a topic
func extractNodeUUID(topic string) string {
	// Topic format: backend_domain/node_uuid
	parts := strings.Split(topic, "/")
	if len(parts) != 2 {
		return ""
	}
	return parts[1]
}

// SetConfigs sets the DataPipeConfigs instance
func (p *Processor) SetConfigs(configs *DataPipeConfigs) {
	p.configs = configs
}
