package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"fmt"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
		
	fmt.Printf("Config: %+v\n", cfg)

	// Create message handler
	messageHandler := func(topic string, payload []byte) {
		log.Printf("Received message on topic %s: %s", topic, string(payload))
	}

	// Create MQTT client
	client, err := mqtt_client.New(cfg, messageHandler)
	if err != nil {
		log.Fatalf("Failed to create MQTT client: %v", err)
	}

	// Connect to MQTT broker
	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", err)
	}
	defer client.Disconnect()

	// Subscribe to topics
	if err := client.Subscribe("localunit.pepeunit.com/bcc4d500-5417-4ecd-b1f8-436964709fea", 1); err != nil {
		log.Printf("Failed to subscribe to topic: %v", err)
	}

	// Add another subscription later
	time.Sleep(5 * time.Second)
	if err := client.Subscribe("localunit.pepeunit.com/fc486c3e-7d2b-4fdb-b559-ca49db1416b4", 1); err != nil {
		log.Printf("Failed to subscribe to topic: %v", err)
	}

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down...")
}
