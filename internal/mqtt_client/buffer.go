package mqtt_client

import (
	"fmt"
	"log"
	"time"
)

// SubscriptionStatus represents the current state of a subscription
type SubscriptionStatus int

const (
	// StatusNone means the topic is not subscribed
	StatusNone SubscriptionStatus = iota
	// StatusSubscribed means the topic is currently subscribed
	StatusSubscribed
	// StatusPending means the topic needs to be subscribed
	StatusPending
	// StatusUnsubscribe means the topic needs to be unsubscribed
	StatusUnsubscribe
)

// SubscriptionBuffer manages MQTT subscriptions with buffering
type SubscriptionBuffer struct {
	client        *MQTTClient
	subscriptions map[string]SubscriptionStatus
	flushChan     chan struct{}
	flushInterval time.Duration
	currentTopics []string
}

// NewSubscriptionBuffer creates a new subscription buffer
func NewSubscriptionBuffer(client *MQTTClient) *SubscriptionBuffer {
	return &SubscriptionBuffer{
		client:        client,
		subscriptions: make(map[string]SubscriptionStatus),
		flushChan:     make(chan struct{}, 1),
		flushInterval: 100 * time.Millisecond,
		currentTopics: make([]string, 0),
	}
}

func (b *SubscriptionBuffer) UpdateFromDatabase() error {
	// Get active topics from database
	activeTopics, err := b.client.GetActiveTopics(b.client.ctx)
	if err != nil {
		return fmt.Errorf("failed to get active topics: %w", err)
	}

	// Create sets for efficient lookup
	currentTopics := make(map[string]struct{})
	for _, topic := range b.currentTopics {
		currentTopics[topic] = struct{}{}
	}

	activeTopicsSet := make(map[string]struct{})
	for _, topic := range activeTopics {
		activeTopicsSet[topic] = struct{}{}
	}

	// Find topics to unsubscribe and subscribe
	var toUnsubscribe []string
	var toSubscribe []string

	// Find topics to unsubscribe (in current but not in active)
	for topic := range currentTopics {
		if _, exists := activeTopicsSet[topic]; !exists {
			toUnsubscribe = append(toUnsubscribe, topic)
		}
	}

	// Find topics to subscribe (in active but not in current)
	for topic := range activeTopicsSet {
		if _, exists := currentTopics[topic]; !exists {
			toSubscribe = append(toSubscribe, topic)
		}
	}

	// Update subscriptions if needed
	if len(toUnsubscribe) > 0 || len(toSubscribe) > 0 {
		// Unsubscribe from topics
		if len(toUnsubscribe) > 0 {
			if err := b.client.UnsubscribeMultiple(toUnsubscribe); err != nil {
				return fmt.Errorf("failed to unsubscribe: %w", err)
			}
		}

		// Subscribe to new topics
		if len(toSubscribe) > 0 {
			// Create subscription map
			subscriptions := make(map[string]byte)
			for _, topic := range toSubscribe {
				subscriptions[topic] = 0 // QoS 0
			}

			if err := b.client.SubscribeMultipleWithCallback(subscriptions, func(topic string, err error) {
				if err != nil {
					log.Printf("Failed to subscribe to topic %s: %v", topic, err)
				}
			}); err != nil {
				return fmt.Errorf("failed to subscribe: %w", err)
			}
		}

		// Update current topics
		b.currentTopics = activeTopics

		// Load configurations for new topics
		if err := b.client.loadConfigs(b.client.ctx); err != nil {
			log.Printf("Failed to load node configurations: %v", err)
		}

		log.Printf("Successfully updated subscriptions: unsubscribed from %d topics, subscribed to %d topics", len(toUnsubscribe), len(toSubscribe))
	} else {
		log.Printf("No subscription changes needed")
	}

	return nil
}
