package config

import (
	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
)

type Config struct {
	PU_DP_DOMAIN                    string `env:"PU_DP_DOMAIN" envDefault:""`
	PU_DP_SECRET_KEY                string `env:"PU_DP_SECRET_KEY" envDefault:""`
	PU_DP_SQLALCHEMY_DATABASE_URL   string `env:"PU_DP_SQLALCHEMY_DATABASE_URL" envDefault:""`
	PU_DP_CLICKHOUSE_DATABASE_URL   string `env:"PU_DP_CLICKHOUSE_DATABASE_URL" envDefault:""`
	PU_DP_REDIS_URL                 string `env:"PU_DP_REDIS_URL" envDefault:"redis://redis:6379/0"`
	PU_DP_MQTT_HOST                 string `env:"PU_DP_MQTT_HOST" envDefault:""`
	PU_DP_MQTT_PORT                 int    `env:"PU_DP_MQTT_PORT" envDefault:"1883"`
	PU_DP_MQTT_KEEPALIVE            int    `env:"PU_DP_MQTT_KEEPALIVE" envDefault:"60"`
	PU_DP_CONFIG_SYNC_INTERVAL      int    `env:"PU_DP_CONFIG_SYNC_INTERVAL" envDefault:"60"`      // Configuration sync interval in seconds
	PU_DP_NRECORDS_CLEANUP_INTERVAL int    `env:"PU_DP_NRECORDS_CLEANUP_INTERVAL" envDefault:"60"` // Interval in seconds
	PU_DP_BUFFER_FLUSH_INTERVAL     int    `env:"PU_DP_BUFFER_FLUSH_INTERVAL" envDefault:"5"`      // Buffer flush interval in seconds
	PU_DP_BUFFER_MAX_SIZE           int    `env:"PU_DP_BUFFER_MAX_SIZE" envDefault:"1000"`         // Maximum number of entries in buffer
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
