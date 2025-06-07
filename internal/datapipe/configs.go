package datapipe

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"data_pipe/internal/database"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type DataPipeConfigs struct {
	mu       sync.RWMutex
	configs  map[uuid.UUID]string
	postgres *database.PostgresDB
	redis    *redis.Client
	mqtt     MQTTClient
}

func NewDataPipeConfigs() *DataPipeConfigs {
	return &DataPipeConfigs{
		configs: make(map[uuid.UUID]string),
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

// LoadFromDB loads all active node configurations from PostgreSQL
func (c *DataPipeConfigs) LoadFromDB(ctx context.Context) error {
	if c.postgres == nil {
		return fmt.Errorf("postgres database not set")
	}

	nodes, err := c.postgres.GetActiveUnitNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active unit nodes: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a map of existing configs for quick lookup
	existingConfigs := make(map[uuid.UUID]struct{})
	for uuid := range c.configs {
		existingConfigs[uuid] = struct{}{}
	}

	// Update configs and track which ones are still active
	activeConfigs := make(map[uuid.UUID]struct{})
	for _, node := range nodes {
		activeConfigs[node.UUID] = struct{}{}
		if _, exists := existingConfigs[node.UUID]; !exists {
			// New node, add to configs
			if node.DataPipeYML != nil {
				c.configs[node.UUID] = *node.DataPipeYML
				if c.mqtt != nil {
					topic := fmt.Sprintf("%s/%s", node.TopicName, node.UUID)
					if err := c.mqtt.Subscribe(topic, 1); err != nil {
						log.Printf("Failed to subscribe to topic %s: %v", topic, err)
					}
				}
			}
		}
	}

	// Remove configs for nodes that are no longer active
	for uuid := range existingConfigs {
		if _, active := activeConfigs[uuid]; !active {
			delete(c.configs, uuid)
			if c.mqtt != nil {
				// We need to get the node to get its topic name
				if node, err := c.postgres.GetUnitNodeByUUID(ctx, uuid); err == nil {
					topic := fmt.Sprintf("%s/%s", node.TopicName, uuid)
					if err := c.mqtt.Unsubscribe(topic); err != nil {
						log.Printf("Failed to unsubscribe from topic %s: %v", topic, err)
					}
				}
			}
		}
	}

	return nil
}

// StartConfigSync starts background processes for configuration synchronization
func (c *DataPipeConfigs) StartConfigSync(ctx context.Context) {
	// Start periodic sync from PostgreSQL
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.LoadFromDB(ctx); err != nil {
					log.Printf("Failed to sync configs from PostgreSQL: %v", err)
				}
			}
		}
	}()

	// Start Redis stream processing
	if c.redis != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Read from Redis stream
					streams, err := c.redis.XRead(ctx, &redis.XReadArgs{
						Streams: []string{"unit_nodes_updates", "0"},
						Block:   0,
					}).Result()

					if err != nil {
						log.Printf("Failed to read from Redis stream: %v", err)
						time.Sleep(time.Second)
						continue
					}

					for _, stream := range streams {
						for _, message := range stream.Messages {
							var update struct {
								Action string            `json:"action"`
								Node   database.UnitNode `json:"node"`
							}

							if err := json.Unmarshal([]byte(message.Values["data"].(string)), &update); err != nil {
								log.Printf("Failed to unmarshal update: %v", err)
								continue
							}

							c.mu.Lock()
							switch update.Action {
							case "add", "update":
								if update.Node.DataPipeYML != nil {
									c.configs[update.Node.UUID] = *update.Node.DataPipeYML
									if c.mqtt != nil {
										topic := fmt.Sprintf("%s/%s", update.Node.TopicName, update.Node.UUID)
										if err := c.mqtt.Subscribe(topic, 1); err != nil {
											log.Printf("Failed to subscribe to topic %s: %v", topic, err)
										}
									}
								}
							case "remove":
								delete(c.configs, update.Node.UUID)
								if c.mqtt != nil {
									topic := fmt.Sprintf("%s/%s", update.Node.TopicName, update.Node.UUID)
									if err := c.mqtt.Unsubscribe(topic); err != nil {
										log.Printf("Failed to unsubscribe from topic %s: %v", topic, err)
									}
								}
							}
							c.mu.Unlock()
						}
					}
				}
			}
		}()
	}
}

// Get returns the configuration for a given node UUID
func (c *DataPipeConfigs) Get(nodeUUID uuid.UUID) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	config, exists := c.configs[nodeUUID]
	return config, exists
}

// GetAll returns all configurations
func (c *DataPipeConfigs) GetAll() map[uuid.UUID]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	configs := make(map[uuid.UUID]string, len(c.configs))
	for k, v := range c.configs {
		configs[k] = v
	}
	return configs
}
