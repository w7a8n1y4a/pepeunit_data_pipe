package datapipe

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"data_pipe/internal/config"
	"data_pipe/internal/database"
	"data_pipe/internal/mqtt_client"
	"data_pipe/internal/types"

	"github.com/redis/go-redis/v9"
)

// DataPipeConfigs stores and manages DataPipe configurations for nodes
type DataPipeConfigs struct {
	mu      sync.RWMutex
	configs map[string]string // map[node.UUID]node.DataPipeYML
	cfg     *config.Config
}

// NewDataPipeConfigs creates a new DataPipeConfigs instance
func NewDataPipeConfigs(cfg *config.Config) *DataPipeConfigs {
	return &DataPipeConfigs{
		configs: make(map[string]string),
		cfg:     cfg,
	}
}

// Get retrieves a node's configuration
func (c *DataPipeConfigs) Get(nodeUUID string) (types.DataPipeConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	configStr, exists := c.configs[nodeUUID]
	if !exists {
		return types.DataPipeConfig{}, false
	}

	var config types.DataPipeConfig
	if err := json.Unmarshal([]byte(configStr), &config); err != nil {
		return types.DataPipeConfig{}, false
	}
	return config, true
}

// LoadNodeConfigs loads active node configurations from PostgreSQL
func (c *DataPipeConfigs) LoadNodeConfigs(ctx context.Context, postgresDB *database.PostgresDB) error {
	nodes, err := postgresDB.GetActiveUnitNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active unit nodes: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a map of existing configs for quick lookup
	existingConfigs := make(map[string]struct{})
	for uuid := range c.configs {
		existingConfigs[uuid] = struct{}{}
	}

	// Update configs and track which ones are still active
	activeConfigs := make(map[string]struct{})
	for _, node := range nodes {
		activeConfigs[node.UUID.String()] = struct{}{}
		if node.DataPipeYML != nil {
			c.configs[node.UUID.String()] = *node.DataPipeYML
		}
	}

	// Remove configs for nodes that are no longer active
	for uuid := range existingConfigs {
		if _, active := activeConfigs[uuid]; !active {
			delete(c.configs, uuid)
		}
	}

	return nil
}

// StartConfigSync starts background processes for configuration synchronization
func (c *DataPipeConfigs) StartConfigSync(ctx context.Context, postgresDB *database.PostgresDB, redisDB *database.RedisDB, mqttClient *mqtt_client.MQTTClient) {
	// Start periodic sync
	go func() {
		ticker := time.NewTicker(time.Duration(c.cfg.CONFIG_SYNC_INTERVAL) * time.Second)
		defer ticker.Stop()

		log.Println("Success Config Update periodic gorutine start")

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Println("Run gorutine update configs by main timer")
				if err := c.LoadNodeConfigs(ctx, postgresDB); err != nil {
					log.Printf("Failed to sync configurations by Gorutine: %v", err)
				}
			}
		}
	}()

	// Start Redis stream processing
	go func() {
		lastID := "$" // Start from the latest message

		// Create a buffer for topics
		topicBuffer := make([]string, 0)
		bufferSize := 0
		stopCh := make(chan struct{})

		// Start timer goroutine
		go func() {
			ticker := time.NewTicker(time.Duration(c.cfg.BUFFER_FLUSH_INTERVAL) * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if bufferSize > 0 {
						log.Printf("Run update configs by Redis buffer timer")
						if err := c.LoadNodeConfigs(ctx, postgresDB); err != nil {
							log.Printf("Failed to update configurations: %v", err)
						}
						topicBuffer = make([]string, 0)
						bufferSize = 0
					}
				case <-stopCh:
					return
				case <-ctx.Done():
					return
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				close(stopCh)
				log.Println("Redis stream processing goroutine received context cancellation")
				return
			default:
				messages, err := redisDB.ReadStream(ctx, "backend_data_pipe_nodes", lastID)
				if err != nil {
					if err != redis.Nil {
						log.Printf("Error reading from Redis stream: %v", err)
					}
					continue
				}

				// Process messages
				for _, message := range messages {
					lastID = message.ID
					values := message.Values

					if unitNodeData, ok := values["unit_node_data"].(string); ok {
						var nodeData map[string]interface{}
						if err := json.Unmarshal([]byte(unitNodeData), &nodeData); err != nil {
							log.Printf("Error parsing unit_node_data JSON: %v", err)
							continue
						}

						if topic, ok := nodeData["topic_name"].(string); ok {
							// Add topic to buffer
							topicBuffer = append(topicBuffer, topic)
							bufferSize++

							// Check if we need to flush based on buffer size
							if bufferSize >= c.cfg.BUFFER_MAX_SIZE {
								log.Printf("Run update configs by Redis buffer size")
								if err := c.LoadNodeConfigs(ctx, postgresDB); err != nil {
									log.Printf("Failed to update configurations: %v", err)
								}
								topicBuffer = make([]string, 0)
								bufferSize = 0
							}
						}
					}
				}
			}
		}
	}()
}
