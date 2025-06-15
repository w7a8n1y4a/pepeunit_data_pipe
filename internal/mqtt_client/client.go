package mqtt_client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"data_pipe/internal/config"
	"data_pipe/internal/database"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/golang-jwt/jwt/v5"
)

type SubscribeOptions struct {
	Topic string
	QoS   byte
}

type ClientOption func(*MQTTClient)

func WithReconnectInterval(interval time.Duration) ClientOption {
	return func(c *MQTTClient) {}
}

type MQTTClient struct {
	cfg                *config.Config
	cm                 *autopaho.ConnectionManager
	router             *paho.StandardRouter
	subscriptions      map[string]paho.SubscribeOptions
	messageHandler     func(topic string, payload []byte)
	subMu              sync.RWMutex // Mutex for subscriptions
	connMu             sync.RWMutex // Mutex for connection state
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	connected          bool
	connectOnce        sync.Once
	subscriptionBuffer *SubscriptionBuffer
	postgresDB         *database.PostgresDB
	loadConfigs        func(ctx context.Context) error
}

func New(cfg *config.Config, messageHandler func(topic string, payload []byte), postgresDB *database.PostgresDB, loadConfigs func(ctx context.Context) error) (*MQTTClient, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	if messageHandler == nil {
		return nil, errors.New("message handler cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	client := &MQTTClient{
		cfg:            cfg,
		router:         paho.NewStandardRouter(),
		subscriptions:  make(map[string]paho.SubscribeOptions),
		messageHandler: messageHandler,
		ctx:            ctx,
		cancel:         cancel,
		postgresDB:     postgresDB,
		loadConfigs:    loadConfigs,
	}

	// Initialize subscription buffer
	client.subscriptionBuffer = NewSubscriptionBuffer(client)

	return client, nil
}

func (c *MQTTClient) Connect() error {
	var connectErr error
	c.connectOnce.Do(func() {
		token, err := generateToken(c.cfg)
		if err != nil {
			connectErr = fmt.Errorf("failed to generate token: %w", err)
			return
		}

		mqttURL := fmt.Sprintf(
			"mqtt://%s:%d",
			c.cfg.MQTT_HOST,
			c.cfg.MQTT_PORT,
		)

		serverURL, err := url.Parse(mqttURL)
		if err != nil {
			connectErr = fmt.Errorf("failed to parse MQTT URL: %w", err)
			return
		}

		cliCfg := autopaho.ClientConfig{
			ServerUrls:                    []*url.URL{serverURL},
			KeepAlive:                     uint16(c.cfg.MQTT_KEEPALIVE),
			CleanStartOnInitialConnection: true,
			SessionExpiryInterval:         0,
			OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
				c.connMu.Lock()
				wasConnected := c.connected
				c.connected = true
				c.connMu.Unlock()

				if wasConnected {
					log.Println("Re sub")
					if err := c.resubscribe(); err != nil {
						log.Printf("Failed to resubscribe: %v", err)
					}
				}
				log.Printf("Success connected to MQTT broker at %s:%d", c.cfg.MQTT_HOST, c.cfg.MQTT_PORT)
			},
			OnConnectError: func(err error) {
				log.Printf("MQTT connection error: %v", err)
				c.connMu.Lock()
				c.connected = false
				c.connMu.Unlock()
				// Try to reconnect
				go c.reconnect()
			},
			ClientConfig: paho.ClientConfig{
				ClientID: c.cfg.BACKEND_DOMAIN,
				Router:   c.router,
				OnClientError: func(err error) {
					log.Printf("MQTT client error: %v", err)
					c.connMu.Lock()
					c.connected = false
					c.connMu.Unlock()
					// Try to reconnect
					go c.reconnect()
				},
				OnServerDisconnect: func(disconnect *paho.Disconnect) {
					c.connMu.Lock()
					c.connected = false
					c.connMu.Unlock()
					log.Printf("MQTT server disconnected: %v", disconnect)
					// Try to reconnect
					go c.reconnect()
				},
			},
			ConnectUsername: token,
		}

		// Register message handler for all topics
		c.router.RegisterHandler("#", func(p *paho.Publish) {
			c.messageHandler(p.Topic, p.Payload)
		})

		// Also register for specific topics
		c.router.RegisterHandler("+", func(p *paho.Publish) {
			c.messageHandler(p.Topic, p.Payload)
		})

		cm, err := autopaho.NewConnection(c.ctx, cliCfg)
		if err != nil {
			connectErr = fmt.Errorf("failed to create connection manager: %w", err)
			return
		}

		c.cm = cm

		if err := c.cm.AwaitConnection(c.ctx); err != nil {
			connectErr = fmt.Errorf("failed to wait for connection: %w", err)
			return
		}

		c.wg.Add(1)
		go c.connectionMonitor()
	})

	return connectErr
}

// reconnect attempts to reconnect to the MQTT broker
func (c *MQTTClient) reconnect() {
	// Add a small delay to prevent rapid reconnection attempts
	time.Sleep(time.Second)

	c.connMu.Lock()
	if c.connected {
		c.connMu.Unlock()
		return
	}
	c.connMu.Unlock()

	log.Printf("Attempting to reconnect to MQTT broker...")
	if err := c.Connect(); err != nil {
		log.Printf("Failed to reconnect to MQTT broker: %v", err)
	}
}

func (c *MQTTClient) connectionMonitor() {
	defer c.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.connMu.RLock()
			connected := c.connected
			c.connMu.RUnlock()

			if !connected {
				log.Printf("MQTT connection lost, attempting to reconnect...")
				go c.reconnect()
			}
		}
	}
}

func (c *MQTTClient) resubscribe() error {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	if len(c.subscriptions) == 0 {
		return nil
	}

	subs := make([]paho.SubscribeOptions, 0, len(c.subscriptions))
	for _, opts := range c.subscriptions {
		subs = append(subs, opts)
	}

	_, err := c.cm.Subscribe(c.ctx, &paho.Subscribe{
		Subscriptions: subs,
	})
	if err != nil {
		return fmt.Errorf("failed to resubscribe: %w", err)
	}

	return nil
}

func (c *MQTTClient) Disconnect() {
	c.cancel()
	if c.cm != nil {
		_ = c.cm.Disconnect(context.Background())
	}
	c.wg.Wait()
}

func (c *MQTTClient) Unsubscribe(topic string) error {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	// Remove from local subscriptions map
	c.subMu.Lock()
	delete(c.subscriptions, topic)
	c.subMu.Unlock()

	c.connMu.RLock()
	connected := c.connected
	cm := c.cm
	c.connMu.RUnlock()

	if connected && cm != nil {
		_, err := cm.Unsubscribe(ctx, &paho.Unsubscribe{
			Topics: []string{topic},
		})
		if err != nil {
			return fmt.Errorf("failed to unsubscribe from topic %s: %w", topic, err)
		}
		log.Printf("Active subscriptions: %d", c.GetSubscriptionCount())
	}

	return nil
}

func generateToken(cfg *config.Config) (string, error) {
	claims := jwt.MapClaims{
		"domain": cfg.BACKEND_DOMAIN,
		"type":   "Backend",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	signedToken, err := token.SignedString([]byte(cfg.BACKEND_SECRET_KEY))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %v", err)
	}

	return signedToken, nil
}

// GetSubscriptionCount returns the number of current subscriptions
func (c *MQTTClient) GetSubscriptionCount() int {
	c.subMu.RLock()
	defer c.subMu.RUnlock()
	return len(c.subscriptions)
}

// UnsubscribeMultiple unsubscribes from multiple topics
func (c *MQTTClient) UnsubscribeMultiple(topics []string) error {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	lockAcquired := make(chan struct{})
	go func() {
		c.subMu.Lock()
		close(lockAcquired)
	}()

	select {
	case <-lockAcquired:
		defer c.subMu.Unlock()
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for lock while unsubscribing from multiple topics")
	}

	// Remove from local subscriptions map
	for _, topic := range topics {
		delete(c.subscriptions, topic)
	}

	if c.connected && c.cm != nil {
		// Create unsubscribe packet
		unsubscribe := &paho.Unsubscribe{
			Topics: topics,
		}

		// Send unsubscribe request
		if _, err := c.cm.Unsubscribe(ctx, unsubscribe); err != nil {
			return fmt.Errorf("failed to unsubscribe from multiple topics: %w", err)
		}
	}

	return nil
}

// SubscribeMultipleWithCallback subscribes to multiple topics with a callback
func (c *MQTTClient) SubscribeMultipleWithCallback(filters map[string]byte, callback func(topic string, err error)) error {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	// Add to local subscriptions map
	c.subMu.Lock()
	for topic, qos := range filters {
		c.subscriptions[topic] = paho.SubscribeOptions{
			Topic: topic,
			QoS:   qos,
		}
	}
	c.subMu.Unlock()

	c.connMu.RLock()
	connected := c.connected
	cm := c.cm
	c.connMu.RUnlock()

	if connected && cm != nil {
		// Create subscribe packet
		subscribe := &paho.Subscribe{
			Subscriptions: make([]paho.SubscribeOptions, 0, len(filters)),
		}

		for topic, qos := range filters {
			subscribe.Subscriptions = append(subscribe.Subscriptions, paho.SubscribeOptions{
				Topic: topic,
				QoS:   qos,
			})
		}

		// Send subscribe request
		resp, err := cm.Subscribe(ctx, subscribe)
		if err != nil {
			// If subscription fails, remove the topics from our local map
			c.subMu.Lock()
			for topic := range filters {
				delete(c.subscriptions, topic)
			}
			c.subMu.Unlock()
			return fmt.Errorf("failed to subscribe to multiple topics: %w", err)
		}

		log.Printf("Success sub %d", len(filters))

		// Process subscription results
		for i, topic := range subscribe.Subscriptions {
			if i < len(resp.Reasons) {
				callback(topic.Topic, nil)
			} else {
				callback(topic.Topic, fmt.Errorf("no response for topic"))
			}
		}
	}

	return nil
}

// GetActiveTopics returns a list of topics for active nodes
func (c *MQTTClient) GetActiveTopics(ctx context.Context) ([]string, error) {
	// Get active nodes from database
	nodes, err := c.postgresDB.GetActiveUnitNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get active nodes: %w", err)
	}

	// Create topics list for subscription
	topics := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.DataPipeYML != nil {
			topic := fmt.Sprintf("%s/%s", c.cfg.BACKEND_DOMAIN, node.UUID)
			topics = append(topics, topic)
		}
	}

	return topics, nil
}

// GetSubscriptionBuffer returns the subscription buffer
func (c *MQTTClient) GetSubscriptionBuffer() *SubscriptionBuffer {
	return c.subscriptionBuffer
}
