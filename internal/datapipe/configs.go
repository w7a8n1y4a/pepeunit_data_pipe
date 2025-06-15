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
	cfg      *config.Config
}

func NewMessageBuffer(mqtt *mqtt_client.MQTTClient, cfg *config.Config) *MessageBuffer {
	b := &MessageBuffer{
		messages: make(map[string]struct{}),
		done:     make(chan struct{}),
		mqtt:     mqtt,
		maxSize:  cfg.BUFFER_MAX_SIZE,
		cfg:      cfg,
	}

	// Start ticker in a separate goroutine
	b.ticker = time.NewTicker(time.Duration(b.cfg.BUFFER_FLUSH_INTERVAL) * time.Second)
	go func() {
		log.Printf("Success Ticker goroutine started")
		for {
			select {
			case <-b.ticker.C:
				b.mu.Lock()
				if len(b.messages) > 0 {
					// Create list of topics for subscription
					topics := make([]string, 0, len(b.messages))
					for t := range b.messages {
						topics = append(topics, t)
					}
					// Clear the buffer
					b.messages = make(map[string]struct{})
					b.mu.Unlock()

					log.Println("Run update subs by redis buffer timer")
					b.mqtt.GetSubscriptionBuffer().UpdateFromDatabase()
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

	// If we've reached max size, flush immediately
	if currentSize >= b.maxSize {
		// Create list of topics for subscription
		topics := make([]string, 0, len(b.messages))
		for t := range b.messages {
			topics = append(topics, t)
		}
		// Clear the buffer
		b.messages = make(map[string]struct{})
		b.mu.Unlock()

		log.Println("Run update subs by redis buffer size")
		b.mqtt.GetSubscriptionBuffer().UpdateFromDatabase()
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
	// Start periodic sync
	go func() {
		ticker := time.NewTicker(time.Duration(c.cfg.CONFIG_SYNC_INTERVAL) * time.Second)
		defer ticker.Stop()

		log.Println("Success PostgreSQL periodic gorutine start")

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.LoadNodeConfigs(ctx, postgresDB); err != nil {
					log.Printf("Failed to sync configurations by Gorutine: %v", err)
				} else {

					// Update subscriptions using the buffer
					log.Println("Run update subs by Gorutine")
					mqttClient.GetSubscriptionBuffer().UpdateFromDatabase()
				}
			}
		}
	}()

	// Start Redis stream processing
	go func() {
		lastID := "$" // Start from the latest message
		log.Printf("Success starting Redis stream processing")

		for {
			select {
			case <-ctx.Done():
				return
			default:
				messages, err := redisDB.ReadStream(ctx, "backend_data_pipe_nodes", lastID)
				if err != nil {
					if err != redis.Nil {
						log.Printf("Error reading from Redis stream: %v", err)
					}
					continue
				}

				if len(messages) == 0 {
					continue
				}

				// Собираем все топики из сообщений
				var allTopics []string
				for _, message := range messages {
					lastID = message.ID
					values := message.Values
					if topic, ok := values["topic"].(string); ok {
						allTopics = append(allTopics, topic)
					}
				}

				// Обновляем подписки одним пакетом
				if len(allTopics) > 0 {
					log.Printf("Updating subscriptions for %d topics from Redis", len(allTopics))
					mqttClient.GetSubscriptionBuffer().UpdateFromDatabase()
				}

				// Подтверждаем обработку всех сообщений
				for _, message := range messages {
					// Добавляем сообщение в поток подтверждений
					_, err := redisDB.AddToStream(ctx, "backend_data_pipe_nodes_ack", map[string]interface{}{
						"original_id": message.ID,
						"status":      "processed",
					})
					if err != nil {
						log.Printf("Error acknowledging message %s: %v", message.ID, err)
					}
				}
			}
		}
	}()
}
