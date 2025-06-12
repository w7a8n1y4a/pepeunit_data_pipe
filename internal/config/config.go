package config

import (
	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
)

type Config struct {
	BACKEND_DOMAIN            string `env:"BACKEND_DOMAIN" envDefault:""`
	BACKEND_SECRET_KEY        string `env:"BACKEND_SECRET_KEY" envDefault:""`
	SQLALCHEMY_DATABASE_URL   string `env:"SQLALCHEMY_DATABASE_URL" envDefault:""`
	CLICKHOUSE_DATABASE_URL   string `env:"CLICKHOUSE_DATABASE_URL" envDefault:""`
	REDIS_URL                 string `env:"REDIS_URL" envDefault:""`
	MQTT_HOST                 string `env:"MQTT_HOST" envDefault:""`
	MQTT_PORT                 int    `env:"MQTT_PORT" envDefault:"1883"`
	MQTT_KEEPALIVE            int    `env:"MQTT_KEEPALIVE" envDefault:"60"`
	NRECORDS_CLEANUP_INTERVAL int    `env:"NRECORDS_CLEANUP_INTERVAL" envDefault:"60"` // Interval in seconds
	BUFFER_FLUSH_INTERVAL     int    `env:"BUFFER_FLUSH_INTERVAL" envDefault:"5"`      // Buffer flush interval in seconds
	BUFFER_MAX_SIZE           int    `env:"BUFFER_MAX_SIZE" envDefault:"200"`          // Maximum number of entries in buffer
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
