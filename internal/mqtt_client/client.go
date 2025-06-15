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
	cfg            *config.Config
	cm             *autopaho.ConnectionManager
	router         *paho.StandardRouter
	messageHandler func(topic string, payload []byte)
	connMu         sync.RWMutex // Mutex for connection state
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	connected      bool
	connectOnce    sync.Once
	postgresDB     *database.PostgresDB
	loadConfigs    func(ctx context.Context) error
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
		messageHandler: messageHandler,
		ctx:            ctx,
		cancel:         cancel,
		postgresDB:     postgresDB,
		loadConfigs:    loadConfigs,
	}

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
					log.Println("Reconnecting...")
					if err := c.subscribe(); err != nil {
						log.Printf("Failed to resubscribe: %v", err)
					}
				} else {
					// Initial connection, subscribe to the topic
					if err := c.subscribe(); err != nil {
						log.Printf("Failed to subscribe: %v", err)
					}
				}
				log.Printf("Success Connected to MQTT broker at %s:%d", c.cfg.MQTT_HOST, c.cfg.MQTT_PORT)
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

// subscribe subscribes to the single topic BACKEND_DOMAIN/+
func (c *MQTTClient) subscribe() error {
	topic := fmt.Sprintf("%s/+", c.cfg.BACKEND_DOMAIN)

	_, err := c.cm.Subscribe(c.ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{
			{
				Topic: topic,
				QoS:   0,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	log.Printf("Success subscribed to topic pattern: %s", topic)
	return nil
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
	return 1 // Since we're subscribing to a single topic
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
