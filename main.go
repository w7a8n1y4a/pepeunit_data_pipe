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
	redisDB, err := database.NewRedis(cfg.REDIS_URL)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisDB.Close()

	// 3. Initialize DataPipe processor and load configurations
	processor := datapipe.NewProcessor(clickhouseDB, postgresDB, cfg)
	configs := datapipe.NewDataPipeConfigs(cfg)
	processor.SetConfigs(configs)
	if err := processor.LoadNodeConfigs(ctx); err != nil {
		log.Fatalf("Failed to load node configurations: %v", err)
	}

	// 4. Create message handler
	messageHandler := func(topic string, payload []byte) {
		fmt.Println("topic", topic)
		if err := processor.ProcessMessage(ctx, topic, payload); err != nil {
			log.Printf("Failed to process message from topic %s: %v", topic, err)
			return
		}
		log.Printf("Successfully processed message from topic %s", topic)
	}

	// 5. Initialize MQTT client
	log.Printf("Initializing MQTT client with broker at %s:%d", cfg.MQTT_HOST, cfg.MQTT_PORT)
	mqttClient, err := mqtt_client.New(cfg, messageHandler)
	if err != nil {
		log.Fatalf("Failed to create MQTT client: %v", err)
	}

	log.Printf("Connecting to MQTT broker...")
	if err := mqttClient.Connect(); err != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", err)
	}
	log.Printf("Successfully connected to MQTT broker")
	defer mqttClient.Disconnect()

	// 6. Start configuration synchronization
	log.Printf("Starting configuration synchronization...")
	processor.StartConfigSync(ctx, redisDB, mqttClient)

	// Subscribe to initial set of topics
	log.Printf("Getting active unit nodes for initial subscription...")
	activeNodes, err := postgresDB.GetActiveUnitNodes(ctx)
	if err != nil {
		log.Fatalf("Failed to get active unit nodes: %v", err)
	}

	log.Printf("Found %d active nodes", len(activeNodes))
	for _, node := range activeNodes {
		if node.DataPipeYML != nil {
			fullTopicName := fmt.Sprintf("%s/%s", cfg.BACKEND_DOMAIN, node.UUID)
			log.Printf("Subscribing to topic: %s", fullTopicName)

			if err := mqttClient.Subscribe(fullTopicName, 0); err != nil {
				log.Printf("Failed to subscribe to topic %s: %v", fullTopicName, err)
				continue
			}
			log.Printf("Successfully subscribed to topic: %s", fullTopicName)
		}
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down...")
}
