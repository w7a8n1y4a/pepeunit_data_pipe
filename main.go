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
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	messageHandler := func(topic string, payload []byte) {
		log.Printf("Received message on topic %s: %s", topic, string(payload))
	}

	client, err := mqtt_client.New(cfg, messageHandler)
	if err != nil {
		log.Fatalf("Failed to create MQTT client: %v", err)
	}

	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", err)
	}
	defer client.Disconnect()

	db, err := database.New(cfg.SQLALCHEMY_DATABASE_URL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	activeNodes, err := db.GetActiveUnitNodes(ctx)
	if err != nil {
		log.Fatalf("Failed to get active unit nodes: %v", err)
	}

	for _, node := range activeNodes {
		log.Printf("Processing node: %s (Topic: %s)", node.UUID, node.TopicName)

		fullTopicName := fmt.Sprintf("%s/%s", cfg.BACKEND_DOMAIN, node.UUID)
		log.Printf("Subscribing to topic: %s", fullTopicName)

		if err := client.Subscribe(fullTopicName, 0); err != nil {
			errorMsg := fmt.Sprintf("Failed to subscribe to topic: %v", err)
			if err := db.UpdateUnitNodeStatus(ctx, node.UUID, database.DataPipeStatusError, &errorMsg); err != nil {
				log.Printf("Failed to update node status: %v", err)
			}
			continue
		}

		var emptyError *string = nil
		if err := db.UpdateUnitNodeStatus(ctx, node.UUID, database.DataPipeStatusActive, emptyError); err != nil {
			log.Printf("Failed to update node status: %v", err)
		}

		if node.DataPipeYML != nil {
			log.Printf("Pipeline config for %s: %s", fullTopicName, *node.DataPipeYML)
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down...")
}
