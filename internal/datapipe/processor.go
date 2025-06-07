package datapipe

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
	"data_pipe/internal/database"
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
	fmt.Println("topic_one", topic)
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

	log.Printf("Processing message for node %s with config: %s", nodeUUID, config)

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

// isConfigActive checks if the configuration is active based on its active period
func isConfigActive(period ActivePeriod) bool {
	now := time.Now()

	switch period.Type {
	case ActivePeriodTypePermanent:
		return true
	case ActivePeriodTypeFromDate:
		return period.Start != nil && now.After(*period.Start)
	case ActivePeriodTypeToDate:
		return period.End != nil && now.Before(*period.End)
	case ActivePeriodTypeDateRange:
		return period.Start != nil && period.End != nil && now.After(*period.Start) && now.Before(*period.End)
	default:
		return false
	}
}

// SetConfigs sets the DataPipeConfigs instance
func (p *Processor) SetConfigs(configs *DataPipeConfigs) {
	p.configs = configs
}
