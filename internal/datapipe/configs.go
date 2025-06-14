package datapipe

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
	"data_pipe/internal/database"
	"data_pipe/internal/types"

	"github.com/redis/go-redis/v9"
)

// DataPipeConfigs stores and manages DataPipe configurations for nodes
type DataPipeConfigs struct {
	mu       sync.RWMutex
	configs  map[string]string // map[node.UUID]node.DataPipeYML
	postgres *database.PostgresDB
	redis    *redis.Client
	mqtt     MQTTClient
	cfg      *config.Config
}

// NewDataPipeConfigs creates a new DataPipeConfigs instance
func NewDataPipeConfigs(cfg *config.Config) *DataPipeConfigs {
	return &DataPipeConfigs{
		configs: make(map[string]string),
		cfg:     cfg,
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

// MessageBuffer holds messages for batch processing
type MessageBuffer struct {
	mu       sync.Mutex
	messages map[string]struct{} // map[topic]struct{}
	ticker   *time.Ticker
	done     chan struct{}
	closed   bool
	mqtt     *mqtt_client.MQTTClient
	maxSize  int
}

func NewMessageBuffer(mqtt *mqtt_client.MQTTClient) *MessageBuffer {
	b := &MessageBuffer{
		messages: make(map[string]struct{}),
		done:     make(chan struct{}),
		mqtt:     mqtt,
		maxSize:  1000,
	}

	log.Printf("Creating new message buffer with ticker")
	// Start ticker in a separate goroutine
	b.ticker = time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-b.ticker.C:
				b.mu.Lock()
				if len(b.messages) > 0 {
					// Создаем копию сообщений для обработки
					topics := make(map[string]byte, len(b.messages))
					for t := range b.messages {
						topics[t] = 0
					}
					// Очищаем буфер
					b.messages = make(map[string]struct{})
					b.mu.Unlock()

					// Подписываемся на топики
					if err := b.mqtt.SubscribeMultiple(topics); err != nil {
						log.Printf("Failed to subscribe to topics: %v", err)
					} else {
						log.Printf("Successfully subscribed to %d topics", len(topics))
					}
					log.Printf("Active subs after Redis get message: %d", b.mqtt.GetSubscriptionCount())
				} else {
					b.mu.Unlock()
				}
			case <-b.done:
				return
			}
		}
	}()

	return b
}

func (b *MessageBuffer) Add(topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		log.Printf("Buffer is closed, ignoring topic: %s", topic)
		return
	}

	b.messages[topic] = struct{}{}
	currentSize := len(b.messages)

	// Если достигли максимального размера, сбрасываем буфер немедленно
	if currentSize >= b.maxSize {
		// Создаем копию сообщений для обработки
		topics := make(map[string]byte, len(b.messages))
		for t := range b.messages {
			topics[t] = 0
		}
		// Очищаем буфер
		b.messages = make(map[string]struct{})
		// Разблокируем мьютекс перед вызовом SubscribeMultiple
		b.mu.Unlock()
		// Подписываемся на топики
		if err := b.mqtt.SubscribeMultiple(topics); err != nil {
			log.Printf("Failed to subscribe to topics: %v", err)
		} else {
			log.Printf("Successfully subscribed to %d topics", len(topics))
		}
		log.Printf("Active subs after Redis get message: %d", b.mqtt.GetSubscriptionCount())
		// Блокируем мьютекс обратно
		b.mu.Lock()
	}
}

func (b *MessageBuffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	log.Printf("Closing message buffer")
	b.closed = true
	if b.ticker != nil {
		b.ticker.Stop()
	}
	close(b.done)
}

// StartConfigSync starts background processes for configuration synchronization
func (c *DataPipeConfigs) StartConfigSync(ctx context.Context, postgresDB *database.PostgresDB, redisDB *database.RedisDB, mqttClient *mqtt_client.MQTTClient) {
	// Start periodic sync from PostgreSQL
	go func() {
		ticker := time.NewTicker(time.Duration(c.cfg.CONFIG_SYNC_INTERVAL) * time.Second)
		defer ticker.Stop()

		log.Printf("Starting PostgreSQL periodic configuration sync (every %d seconds)", c.cfg.CONFIG_SYNC_INTERVAL)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.LoadNodeConfigs(ctx, postgresDB); err != nil {
					log.Printf("Failed to sync configurations from PostgreSQL: %v", err)
				} else {
					// Update MQTT subscriptions after successful sync
					nodes, err := postgresDB.GetActiveUnitNodes(ctx)
					if err != nil {
						log.Printf("Failed to get active nodes for MQTT subscription update: %v", err)
						continue
					}

					// Create a map of topics to subscribe to
					topics := make(map[string]byte)
					for _, node := range nodes {
						if node.DataPipeYML != nil {
							topic := fmt.Sprintf("%s/%s", c.cfg.BACKEND_DOMAIN, node.UUID)
							topics[topic] = 0
						}
					}

					// Subscribe to all topics at once
					if err := mqttClient.SubscribeMultiple(topics); err != nil {
						log.Printf("Failed to subscribe to topics: %v", err)
					} else {
						log.Printf("Successfully subscribed to %d topics", len(topics))
					}
				}
			}
		}
	}()

	// Create message buffer for Redis updates
	msgBuffer := NewMessageBuffer(mqttClient)

	// Start Redis stream processing
	go func() {
		lastID := "$" // Start from the latest message
		log.Printf("Starting Redis stream processing")

		// Start message processor
		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Printf("Context done, closing message buffer")
					msgBuffer.Close()
					return
				case <-msgBuffer.done:
					return
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				messages, err := redisDB.ReadStream(ctx, "backend_data_pipe_nodes", lastID)
				if err != nil {
					log.Printf("Failed to read from Redis stream: %v", err)
					time.Sleep(time.Second) // Wait before retrying
					continue
				}

				if len(messages) > 0 {
					for _, msg := range messages {
						lastID = msg.ID

						// Parse message
						action, ok := msg.Values["action"].(string)
						if !ok {
							continue
						}

						unitNodeData, ok := msg.Values["unit_node_data"].(string)
						if !ok {
							continue
						}

						var unitNode struct {
							UUID        string  `json:"uuid"`
							TopicName   string  `json:"topic_name"`
							DataPipeYML *string `json:"data_pipe_yml"`
						}

						if err := json.Unmarshal([]byte(unitNodeData), &unitNode); err != nil {
							continue
						}

						// Update configurations and subscriptions
						switch action {
						case "Update":
							if unitNode.DataPipeYML != nil {
								c.Set(unitNode.UUID, *unitNode.DataPipeYML)
								topic := fmt.Sprintf("%s/%s", c.cfg.BACKEND_DOMAIN, unitNode.UUID)
								msgBuffer.Add(topic)
							}
						case "Delete":
							topic := fmt.Sprintf("%s/%s", c.cfg.BACKEND_DOMAIN, unitNode.UUID)
							if err := mqttClient.Unsubscribe(topic); err != nil {
								log.Printf("Failed to unsubscribe from topic %s: %v", topic, err)
							}
							c.Remove(unitNode.UUID)
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
