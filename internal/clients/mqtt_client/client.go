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
	subscriptions  map[string]paho.SubscribeOptions
	messageHandler func(topic string, payload []byte)
	mu             sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	connected      bool
	connectOnce    sync.Once
}

func New(cfg *config.Config, messageHandler func(topic string, payload []byte)) (*MQTTClient, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}

	if messageHandler == nil {
		return nil, errors.New("message handler cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &MQTTClient{
		cfg:            cfg,
		router:         paho.NewStandardRouter(),
		subscriptions:  make(map[string]paho.SubscribeOptions),
		messageHandler: messageHandler,
		ctx:            ctx,
		cancel:         cancel,
	}, nil
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
			CleanStartOnInitialConnection: false,
			SessionExpiryInterval:         86400,
			OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
				c.mu.Lock()
				wasConnected := c.connected
				c.connected = true
				c.mu.Unlock()

				if !wasConnected {
					if err := c.resubscribe(); err != nil {
						log.Printf("Failed to resubscribe: %v", err)
					}
					log.Printf("Success connected to MQTT broker at %s:%d", c.cfg.MQTT_HOST, c.cfg.MQTT_PORT)
				}
			},
			OnConnectError: func(err error) {
				log.Printf("MQTT connection error: %v", err)
			},
			ClientConfig: paho.ClientConfig{
				ClientID: c.cfg.BACKEND_DOMAIN,
				Router:   c.router,
				OnClientError: func(err error) {
					log.Printf("MQTT client error: %v", err)
					c.mu.Lock()
					c.connected = false
					c.mu.Unlock()
				},
				OnServerDisconnect: func(disconnect *paho.Disconnect) {
					c.mu.Lock()
					c.connected = false
					c.mu.Unlock()
					log.Printf("MQTT server disconnected: %v", disconnect)
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

func (c *MQTTClient) connectionMonitor() {
	defer c.wg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			connected := c.connected
			c.mu.RUnlock()

			if !connected {
				log.Printf("MQTT connection lost, attempting to reconnect...")
			}
		}
	}
}

func (c *MQTTClient) Subscribe(topic string, qos byte) error {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()

	lockAcquired := make(chan struct{})
	go func() {
		c.mu.Lock()
		close(lockAcquired)
	}()

	select {
	case <-lockAcquired:
		defer c.mu.Unlock()
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for lock while subscribing to %s", topic)
	}

	c.subscriptions[topic] = paho.SubscribeOptions{
		Topic: topic,
		QoS:   qos,
	}

	if c.connected && c.cm != nil {
		_, err := c.cm.Subscribe(c.ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{
				{Topic: topic, QoS: qos},
			},
		})
		if err != nil {
			delete(c.subscriptions, topic)
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
		}
		log.Printf("Active subscriptions: %d", len(c.subscriptions))
	}

	return nil
}

func (c *MQTTClient) resubscribe() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

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

	lockAcquired := make(chan struct{})
	go func() {
		c.mu.Lock()
		close(lockAcquired)
	}()

	select {
	case <-lockAcquired:
		defer c.mu.Unlock()
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for lock while unsubscribing from %s", topic)
	}

	delete(c.subscriptions, topic)

	if c.connected && c.cm != nil {
		_, err := c.cm.Unsubscribe(c.ctx, &paho.Unsubscribe{
			Topics: []string{topic},
		})
		if err != nil {
			return fmt.Errorf("failed to unsubscribe from topic %s: %w", topic, err)
		}
		log.Printf("Active subscriptions: %d", len(c.subscriptions))
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

// GetSubscriptions returns a map of current subscriptions
func (c *MQTTClient) GetSubscriptions() map[string]struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a copy of the subscriptions map
	subs := make(map[string]struct{}, len(c.subscriptions))
	for topic := range c.subscriptions {
		subs[topic] = struct{}{}
	}
	return subs
}

// GetSubscriptionCount returns the number of current subscriptions
func (c *MQTTClient) GetSubscriptionCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.subscriptions)
}
