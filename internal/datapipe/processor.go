package datapipe

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/database"
)

// Processor handles message processing and configuration management
type Processor struct {
	clickhouse *database.ClickHouseDB
	postgres   *database.PostgresDB
	configs    *DataPipeConfigs
}

// NewProcessor creates a new Processor instance
func NewProcessor(clickhouse *database.ClickHouseDB, postgres *database.PostgresDB) *Processor {
	return &Processor{
		clickhouse: clickhouse,
		postgres:   postgres,
		configs:    NewDataPipeConfigs(),
	}
}

// LoadNodeConfigs loads active node configurations from PostgreSQL
func (p *Processor) LoadNodeConfigs(ctx context.Context) error {
	return p.configs.LoadNodeConfigs(ctx, p.postgres)
}

// StartConfigSync starts background processes for configuration synchronization
func (p *Processor) StartConfigSync(ctx context.Context, redisDB *database.RedisDB, mqttClient *mqtt_client.MQTTClient) {
	p.configs.StartConfigSync(ctx, p.postgres, redisDB, mqttClient)
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
