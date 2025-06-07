package datapipe

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"data_pipe/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type Processor struct {
	clickhouseDB *database.ClickHouseDB
	postgresDB   *database.PostgresDB
	configs      *DataPipeConfigs
}

func NewProcessor(clickhouseDB *database.ClickHouseDB, postgresDB *database.PostgresDB) *Processor {
	return &Processor{
		clickhouseDB: clickhouseDB,
		postgresDB:   postgresDB,
		configs:      NewDataPipeConfigs(),
	}
}

// LoadNodeConfigs loads all active node configurations from PostgreSQL
func (p *Processor) LoadNodeConfigs(ctx context.Context) error {
	p.configs.SetPostgres(p.postgresDB)
	return p.configs.LoadFromDB(ctx)
}

// StartConfigSync starts background processes for configuration synchronization
func (p *Processor) StartConfigSync(ctx context.Context, redisClient *redis.Client, mqttClient MQTTClient) {
	p.configs.SetRedis(redisClient)
	p.configs.SetMQTT(mqttClient)
	p.configs.StartConfigSync(ctx)
}

func (p *Processor) ProcessMessage(ctx context.Context, topic string, payload []byte) error {
	// Extract node UUID from topic
	// Topic format: backend_domain/node_uuid
	parts := strings.Split(topic, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid topic format: %s", topic)
	}

	nodeUUID, err := uuid.Parse(parts[1])
	if err != nil {
		return fmt.Errorf("failed to parse node UUID from topic %s: %w", topic, err)
	}

	// Get node configuration
	config, exists := p.configs.Get(nodeUUID)
	if !exists {
		return fmt.Errorf("no configuration found for node %s", nodeUUID)
	}

	log.Printf("Processing message from topic %s with config: %s", topic, config)
	return nil
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
