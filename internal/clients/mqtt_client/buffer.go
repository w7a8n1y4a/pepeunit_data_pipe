package mqtt_client

import (
	"context"
	"log"
	"sync"
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

// SubscriptionBuffer manages MQTT topic subscriptions with batching
type SubscriptionBuffer struct {
	mu            sync.Mutex
	subscriptions map[string]SubscriptionStatus
	maxSize       int
	flushInterval time.Duration
	ticker        *time.Ticker
	done          chan struct{}
	client        *MQTTClient
}

// NewSubscriptionBuffer creates a new subscription buffer
func NewSubscriptionBuffer(client *MQTTClient, maxSize int, flushInterval time.Duration) *SubscriptionBuffer {
	b := &SubscriptionBuffer{
		subscriptions: make(map[string]SubscriptionStatus),
		maxSize:       maxSize,
		flushInterval: flushInterval,
		done:          make(chan struct{}),
		client:        client,
	}

	b.ticker = time.NewTicker(flushInterval)
	go b.flushLoop()

	return b
}

// BulkAdd adds multiple topics to the buffer
func (b *SubscriptionBuffer) BulkAdd(topics []string) {
	b.mu.Lock()
	for _, topic := range topics {
		b.subscriptions[topic] = StatusPending
	}
	needFlush := len(b.subscriptions) >= b.maxSize
	b.mu.Unlock()

	if needFlush {
		// Create a context with timeout for the flush operation
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create a channel to signal completion
		done := make(chan struct{})

		// Trigger flush to apply changes
		go func() {
			if err := b.Flush(); err != nil {
				log.Printf("Failed to flush subscriptions: %v", err)
			}
			close(done)
		}()

		// Wait for flush to complete or timeout
		select {
		case <-done:
			log.Printf("Successfully added %d topics to buffer", len(topics))
		case <-ctx.Done():
			log.Printf("Timeout waiting for subscription flush to complete")
		}
	}
}

// UpdateFromDatabase updates the subscription status based on active nodes
func (b *SubscriptionBuffer) UpdateFromDatabase(activeTopics []string) {
	b.mu.Lock()
	// Mark all current subscriptions for potential unsubscription
	for topic := range b.subscriptions {
		if b.subscriptions[topic] == StatusSubscribed {
			b.subscriptions[topic] = StatusUnsubscribe
		}
	}

	// Mark active topics for subscription
	for _, topic := range activeTopics {
		b.subscriptions[topic] = StatusPending
	}
	b.mu.Unlock()

	// Create a context with timeout for the flush operation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a channel to signal completion
	done := make(chan struct{})

	// Trigger flush to apply changes
	go func() {
		if err := b.Flush(); err != nil {
			log.Printf("Failed to flush subscriptions: %v", err)
		}
		close(done)
	}()

	// Wait for flush to complete or timeout
	select {
	case <-done:
		log.Printf("Successfully updated subscriptions for %d topics", len(activeTopics))
	case <-ctx.Done():
		log.Printf("Timeout waiting for subscription flush to complete")
	}
}

// Flush processes all pending subscriptions and unsubscriptions
func (b *SubscriptionBuffer) Flush() error {
	b.mu.Lock()
	// Prepare subscriptions and unsubscriptions
	toSubscribe := make(map[string]byte)
	toUnsubscribe := make([]string, 0)

	for topic, status := range b.subscriptions {
		switch status {
		case StatusPending:
			toSubscribe[topic] = 0
			b.subscriptions[topic] = StatusSubscribed
		case StatusUnsubscribe:
			toUnsubscribe = append(toUnsubscribe, topic)
			delete(b.subscriptions, topic)
		}
	}
	b.mu.Unlock()

	// Process unsubscriptions first
	if len(toUnsubscribe) > 0 {
		log.Printf("Unsubscribing from %d topics", len(toUnsubscribe))
		if err := b.client.UnsubscribeMultiple(toUnsubscribe); err != nil {
			log.Printf("Failed to unsubscribe from topics: %v", err)
			return err
		}
	}

	// Process new subscriptions
	if len(toSubscribe) > 0 {
		log.Printf("Subscribing to %d topics", len(toSubscribe))
		if err := b.client.SubscribeMultiple(toSubscribe); err != nil {
			log.Printf("Failed to subscribe to topics: %v", err)
			return err
		}
	}

	return nil
}

func (b *SubscriptionBuffer) flushLoop() {
	for {
		select {
		case <-b.ticker.C:
			if err := b.Flush(); err != nil {
				log.Printf("Error during periodic subscription flush: %v", err)
			}
		case <-b.done:
			// Perform one final flush before closing
			if err := b.Flush(); err != nil {
				log.Printf("Error during final subscription flush: %v", err)
			}
			return
		}
	}
}

// Close stops the subscription buffer
func (b *SubscriptionBuffer) Close() {
	close(b.done)
	b.ticker.Stop()
}
