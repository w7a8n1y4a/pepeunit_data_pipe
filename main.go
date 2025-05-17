package main

import (
	"fmt"
	"log"

	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
)

type Config struct {
	SQLALCHEMY_DATABASE_URL string `env:"SQLALCHEMY_DATABASE_URL" envDefault:""`
	CLICKHOUSE_DATABASE_URL string `env:"CLICKHOUSE_DATABASE_URL" envDefault:""`
	REDIS_URL               string `env:"REDIS_URL" envDefault:""`
	MQTT_HOST				string `env:"MQTT_HOST" envDefault:""`
	MQTT_SECURE				bool   `env:"MQTT_SECURE" envDefault:"true"`
	MQTT_PORT				int    `env:"MQTT_PORT" envDefault:"1883"`
 	MQTT_KEEPALIVE			int    `env:"MQTT_KEEPALIVE" envDefault:"60"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("No .env file found or error loading .env file: %v", err)
	}

	cfg := Config{}
	if err := env.Parse(&cfg); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Config: %+v\n", cfg)
}
