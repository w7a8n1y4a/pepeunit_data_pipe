package mqtt_client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
	"net/url"

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
	return func(c *MQTTClient) {
		// This can be expanded if we add reconnect interval to the client struct
	}
}

type MQTTClient struct {
	cfg            *config.Config
	cm             *autopaho.ConnectionManager
	router         *paho.StandardRouter
	subscriptions  map[string]paho.SubscribeOptions // Current subscriptions
	messageHandler func(topic string, payload []byte)
	mu             sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	connected      bool
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

// Connect establishes the MQTT connection
func (c *MQTTClient) Connect() error {
    token, err := generateToken(c.cfg)
    if err != nil {
        return fmt.Errorf("failed to generate token: %w", err)
    }
	
    mqttURL := fmt.Sprintf(
        "mqtt://%s:%d",
        c.cfg.MQTT_HOST,
        c.cfg.MQTT_PORT,
    )

    serverURL, err := url.Parse(mqttURL)
    if err != nil {
        return fmt.Errorf("failed to parse MQTT URL: %w", err)
    }

    cliCfg := autopaho.ClientConfig{
        ServerUrls: []*url.URL{serverURL},
        KeepAlive:  30,
        CleanStartOnInitialConnection: false,
        SessionExpiryInterval: 60 * 60 * 24, // 1 day
        OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
            c.mu.Lock()
            c.connected = true
            c.mu.Unlock()
            log.Println("MQTT connection established")

            // Resubscribe to all topics on reconnection
            if err := c.resubscribe(); err != nil {
                log.Printf("Failed to resubscribe: %v", err)
            }
        },
        OnConnectError: func(err error) {
            log.Printf("MQTT connection error: %v", err)
        },
        ClientConfig: paho.ClientConfig{
            ClientID: c.cfg.BACKEND_DOMAIN,
            Router:   c.router,
        },
		ConnectUsername: token,
        ConnectPassword: nil,
	}

	// Setup router to handle incoming messages
c.router.RegisterHandler("*", func(p *paho.Publish) {
    c.messageHandler(p.Topic, p.Payload)
})

    // Start connection manager
    cm, err := autopaho.NewConnection(c.ctx, cliCfg)
    if err != nil {
        return fmt.Errorf("failed to create connection manager: %w", err)
    }

    c.cm = cm

    // Start reconnection handler
    c.wg.Add(1)
    go c.reconnectionHandler()

    return nil
}

func (c *MQTTClient) reconnectionHandler() {
    defer c.wg.Done()

    ticker := time.NewTicker(5 * time.Second)
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
                log.Println("MQTT connection lost, waiting for automatic reconnection...")
            }
        }
    }
}

// Subscribe adds a new topic subscription
func (c *MQTTClient) Subscribe(topic string, qos byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Add to subscriptions map
	c.subscriptions[topic] = paho.SubscribeOptions{
		Topic: topic,
		QoS:   qos,
	}

	// If connected, subscribe immediately
	if c.connected {
		_, err := c.cm.Subscribe(c.ctx, &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{
				{Topic: topic, QoS: qos},
			},
		})
		if err != nil {
			delete(c.subscriptions, topic)
			return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
		}
	}

	return nil
}

// resubscribe renews all current subscriptions
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

// Disconnect closes the MQTT connection
func (c *MQTTClient) Disconnect() {
	c.cancel()
	if c.cm != nil {
		_ = c.cm.Disconnect(context.Background())
	}
	c.wg.Wait()
}

// generateToken creates a JWT token for MQTT authentication
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
