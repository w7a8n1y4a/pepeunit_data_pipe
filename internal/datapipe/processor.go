package datapipe

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
	"data_pipe/internal/database"
	"data_pipe/internal/datapipe/active_period"
	"data_pipe/internal/datapipe/filters"
	"data_pipe/internal/types"
)

// Processor handles message processing and configuration management
type Processor struct {
	clickhouseDB *database.ClickHouseDB
	postgresDB   *database.PostgresDB
	configs      *DataPipeConfigs
}

// NewProcessor creates a new Processor instance
func NewProcessor(clickhouseDB *database.ClickHouseDB, postgresDB *database.PostgresDB, cfg *config.Config) *Processor {
	return &Processor{
		clickhouseDB: clickhouseDB,
		postgresDB:   postgresDB,
		configs:      NewDataPipeConfigs(cfg),
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

// ProcessMessage processes a message from a topic
func (p *Processor) ProcessMessage(ctx context.Context, topic string, payload []byte) error {
	// Extract node UUID from topic
	nodeUUID := extractNodeUUID(topic)
	if nodeUUID == "" {
		return fmt.Errorf("invalid topic format: %s", topic)
	}

	// Get node configuration
	config, exists := p.configs.Get(nodeUUID)
	if !exists {
		return fmt.Errorf("no configuration found for node %s", nodeUUID)
	}

	// Check if the message should be processed based on active period
	currentTime := time.Now()
	if !active_period.IsActive(&config.ActivePeriod, currentTime) {
		return nil
	}

	// Convert payload based on input type
	var value interface{}
	if config.Filters.TypeInputValue == types.TypeInputValueNumber {
		// Try to parse as number first
		if num, err := strconv.ParseFloat(string(payload), 64); err == nil {
			value = num
		} else {
			// If parsing fails, use as string
			value = string(payload)
		}
	} else {
		value = string(payload)
	}

	// Apply filters
	shouldProcess, err := filters.ApplyFilters(value, config.Filters)
	if err != nil {
		return fmt.Errorf("failed to apply filters: %w", err)
	}
	if !shouldProcess {
		return nil
	}

	log.Printf("Processing message for node %s", nodeUUID)

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
