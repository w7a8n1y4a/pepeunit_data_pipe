package datapipe

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/database"

	"github.com/redis/go-redis/v9"
)

// DataPipeConfigs stores and manages DataPipe configurations for nodes
type DataPipeConfigs struct {
	mu       sync.RWMutex
	configs  map[string]string // map[node.UUID]node.DataPipeYML
	postgres *database.PostgresDB
	redis    *redis.Client
	mqtt     MQTTClient
}

// NewDataPipeConfigs creates a new DataPipeConfigs instance
func NewDataPipeConfigs() *DataPipeConfigs {
	return &DataPipeConfigs{
		configs: make(map[string]string),
	}
}

// SetPostgres sets the PostgreSQL database connection
func (c *DataPipeConfigs) SetPostgres(db *database.PostgresDB) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.postgres = db
}

// SetRedis sets the Redis client
func (c *DataPipeConfigs) SetRedis(client *redis.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.redis = client
}

// SetMQTT sets the MQTT client
func (c *DataPipeConfigs) SetMQTT(client MQTTClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mqtt = client
}

// Set adds or updates a node's configuration
func (c *DataPipeConfigs) Set(nodeUUID, config string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.configs[nodeUUID] = config
}

// Get retrieves a node's configuration
func (c *DataPipeConfigs) Get(nodeUUID string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	config, exists := c.configs[nodeUUID]
	return config, exists
}

// Remove removes a node's configuration
func (c *DataPipeConfigs) Remove(nodeUUID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.configs, nodeUUID)
}

// LoadNodeConfigs loads active node configurations from PostgreSQL
func (c *DataPipeConfigs) LoadNodeConfigs(ctx context.Context, postgresDB *database.PostgresDB) error {
	nodes, err := postgresDB.GetActiveUnitNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active unit nodes: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Clear existing configs
	c.configs = make(map[string]string)

	// Load new configs
	for _, node := range nodes {
		if node.DataPipeYML != nil {
			c.configs[node.UUID.String()] = *node.DataPipeYML
		}
	}

	return nil
}

// StartConfigSync starts background processes for configuration synchronization
func (c *DataPipeConfigs) StartConfigSync(ctx context.Context, postgresDB *database.PostgresDB, redisDB *database.RedisDB, mqttClient *mqtt_client.MQTTClient) {
	// Start periodic sync from PostgreSQL
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.LoadNodeConfigs(ctx, postgresDB); err != nil {
					log.Printf("Failed to sync configurations from PostgreSQL: %v", err)
				}
			}
		}
	}()

	// Start Redis stream processing
	go func() {
		lastID := "$" // Start from the latest message
		log.Printf("Starting Redis stream processing from ID: %s", lastID)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("Reading from Redis stream with lastID: %s", lastID)
				messages, err := redisDB.ReadStream(ctx, "backend_data_pipe_nodes", lastID)
				if err != nil {
					log.Printf("Failed to read from Redis stream: %v", err)
					time.Sleep(time.Second) // Wait before retrying
					continue
				}

				if len(messages) > 0 {
					log.Printf("Received %d messages from Redis stream", len(messages))
					for _, msg := range messages {
						log.Printf("Processing message ID: %s", msg.ID)
						lastID = msg.ID

						// Parse message
						action, ok := msg.Values["action"].(string)
						if !ok {
							log.Printf("Invalid message format: missing action")
							continue
						}

						unitNodeData, ok := msg.Values["unit_node_data"].(string)
						fmt.Println("unitNodeData", unitNodeData)
						if !ok {
							log.Printf("Invalid message format: missing unit_node_data")
							continue
						}

						var unitNode struct {
							UUID        string  `json:"uuid"`
							TopicName   string  `json:"topic_name"`
							DataPipeYML *string `json:"data_pipe_yml"`
						}

						if err := json.Unmarshal([]byte(unitNodeData), &unitNode); err != nil {
							log.Printf("Failed to parse unit node data: %v", err)
							continue
						}

						// Update configurations and subscriptions
						switch action {
						case "Update":
							if unitNode.DataPipeYML != nil {
								c.Set(unitNode.UUID, *unitNode.DataPipeYML)
								topic := fmt.Sprintf("%s/%s", "backend_domain", unitNode.UUID)
								if err := mqttClient.Subscribe(topic, 0); err != nil {
									log.Printf("Failed to subscribe to topic %s: %v", topic, err)
								}
							}
						case "Delete":
							topic := fmt.Sprintf("%s/%s", "backend_domain", unitNode.UUID)
							if err := mqttClient.Unsubscribe(topic); err != nil {
								log.Printf("Failed to unsubscribe from topic %s: %v", topic, err)
							}
							c.Remove(unitNode.UUID)
						default:
							log.Printf("Unknown action: %s", action)
						}
					}
				}
			}
		}
	}()
}

// GetAll returns all configurations
func (c *DataPipeConfigs) GetAll() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	configs := make(map[string]string, len(c.configs))
	for k, v := range c.configs {
		configs[k] = v
	}
	return configs
}
