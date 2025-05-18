package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"data_pipe/internal/clients/mqtt_client"
	"data_pipe/internal/config"
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

	fmt.Println("one")

	if err := client.Connect(); err != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", err)
	}
	defer client.Disconnect()

	fmt.Println("two")

	if err := client.Subscribe("localunit.pepeunit.com/bcc4d500-5417-4ecd-b1f8-436964709fea", 0); err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}

	if err := client.Subscribe("localunit.pepeunit.com/ad1edef4-dfc7-488e-b7d2-6022e56eb0db", 0); err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
	fmt.Println("three")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down...")
}
