package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
	"data_pipe/internal/database"

	"github.com/google/uuid"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize ClickHouse connection
	clickhouseDB, err := database.NewClickHouse(cfg.CLICKHOUSE_DATABASE_URL)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer clickhouseDB.Close()

	// Create example data
	now := time.Now().UTC().Round(time.Millisecond) // Round to milliseconds for DateTime64(3)
	nodeUUID := uuid.New()

	// Example NLastEntries
	nLastEntries := []database.NLastEntry{
		{
			UUID:           uuid.New(),
			UnitNodeUUID:   nodeUUID,
			State:          "25.5",
			StateType:      "Number",
			CreateDateTime: now,
			MaxCount:       100,
			Size:           50,
		},
		{
			UUID:           uuid.New(),
			UnitNodeUUID:   nodeUUID,
			State:          "30.2",
			StateType:      "Number",
			CreateDateTime: now.Add(time.Minute),
			MaxCount:       100,
			Size:           50,
		},
		{
			UUID:           uuid.New(),
			UnitNodeUUID:   nodeUUID,
			State:          "28.7",
			StateType:      "Number",
			CreateDateTime: now.Add(2 * time.Minute),
			MaxCount:       100,
			Size:           50,
		},
	}

	// Example WindowEntries
	windowEntries := []database.WindowEntry{
		{
			UUID:               uuid.New(),
			UnitNodeUUID:       nodeUUID,
			State:              "25.5",
			StateType:          "Number",
			CreateDateTime:     now,
			ExpirationDateTime: now.Add(24 * time.Hour),
			Size:               50,
		},
		{
			UUID:               uuid.New(),
			UnitNodeUUID:       nodeUUID,
			State:              "30.2",
			StateType:          "Number",
			CreateDateTime:     now.Add(time.Minute),
			ExpirationDateTime: now.Add(24 * time.Hour),
			Size:               50,
		},
		{
			UUID:               uuid.New(),
			UnitNodeUUID:       nodeUUID,
			State:              "28.7",
			StateType:          "Number",
			CreateDateTime:     now.Add(2 * time.Minute),
			ExpirationDateTime: now.Add(24 * time.Hour),
			Size:               50,
		},
	}

	// Example AggregationEntries
	aggregationEntries := []database.AggregationEntry{
		{
			UUID:                uuid.New(),
			UnitNodeUUID:        nodeUUID,
			State:               28.13, // average of 25.5, 30.2, 28.7
			AggregationType:     "Avg",
			CreateDateTime:      now.Add(3 * time.Minute),
			StartWindowDateTime: now,
			EndWindowDateTime:   now.Add(2 * time.Minute),
		},
		{
			UUID:                uuid.New(),
			UnitNodeUUID:        nodeUUID,
			State:               25.5, // min of 25.5, 30.2, 28.7
			AggregationType:     "Min",
			CreateDateTime:      now.Add(3 * time.Minute),
			StartWindowDateTime: now,
			EndWindowDateTime:   now.Add(2 * time.Minute),
		},
		{
			UUID:                uuid.New(),
			UnitNodeUUID:        nodeUUID,
			State:               30.2, // max of 25.5, 30.2, 28.7
			AggregationType:     "Max",
			CreateDateTime:      now.Add(3 * time.Minute),
			StartWindowDateTime: now,
			EndWindowDateTime:   now.Add(2 * time.Minute),
		},
	}

	// Insert data into ClickHouse
	ctx := context.Background()

	if err := clickhouseDB.BulkCreateNLastEntries(ctx, nLastEntries); err != nil {
		log.Printf("Failed to insert NLastEntries: %v", err)
	} else {
		log.Println("Successfully inserted NLastEntries")
	}

	if err := clickhouseDB.BulkCreateWindowEntries(ctx, windowEntries); err != nil {
		log.Printf("Failed to insert WindowEntries: %v", err)
	} else {
		log.Println("Successfully inserted WindowEntries")
	}

	if err := clickhouseDB.BulkCreateAggregationEntries(ctx, aggregationEntries); err != nil {
		log.Printf("Failed to insert AggregationEntries: %v", err)
	} else {
		log.Println("Successfully inserted AggregationEntries")
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
