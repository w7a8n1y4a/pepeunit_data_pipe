package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
	"data_pipe/internal/database"
	"data_pipe/internal/datapipe"

	"github.com/redis/go-redis/v9"
)

func main() {
	// 1. Load environment variables
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Initialize database connections
	clickhouseDB, err := database.NewClickHouse(cfg.CLICKHOUSE_DATABASE_URL)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer clickhouseDB.Close()

	postgresDB, err := database.NewPostgres(cfg.SQLALCHEMY_DATABASE_URL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer postgresDB.Close()

	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.REDIS_URL,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer redisClient.Close()

	// 3. Initialize DataPipe processor and load configurations
	processor := datapipe.NewProcessor(clickhouseDB, postgresDB)
	if err := processor.LoadNodeConfigs(ctx); err != nil {
		log.Fatalf("Failed to load node configurations: %v", err)
	}

	// 4. Create message handler
	messageHandler := func(topic string, payload []byte) {
		if err := processor.ProcessMessage(ctx, topic, payload); err != nil {
			log.Printf("Failed to process message from topic %s: %v", topic, err)
			return
		}
		log.Printf("Successfully processed message from topic %s", topic)
	}

	// 5. Initialize MQTT client
	mqttClient, err := mqtt_client.New(cfg, messageHandler)
	if err != nil {
		log.Fatalf("Failed to create MQTT client: %v", err)
	}

	if err := mqttClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", err)
	}
	defer mqttClient.Disconnect()

	// 6. Start configuration synchronization
	processor.StartConfigSync(ctx, redisClient, mqttClient)

	// Subscribe to initial set of topics
	activeNodes, err := postgresDB.GetActiveUnitNodes(ctx)
	if err != nil {
		log.Fatalf("Failed to get active unit nodes: %v", err)
	}

	for _, node := range activeNodes {
		if node.DataPipeYML != nil {
			fullTopicName := fmt.Sprintf("%s/%s", cfg.BACKEND_DOMAIN, node.UUID)
			log.Printf("Subscribing to topic: %s", fullTopicName)

			if err := mqttClient.Subscribe(fullTopicName, 0); err != nil {
				log.Printf("Failed to subscribe to topic %s: %v", fullTopicName, err)
				continue
			}
		}
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down...")
}
