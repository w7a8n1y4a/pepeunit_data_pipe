package main

import (
	"fmt"
	"log"
	
	"github.com/golang-jwt/jwt/v5"
	"github.com/caarlos0/env"
	"github.com/joho/godotenv"
)

type Config struct {
	BACKEND_SECRET_KEY      string `env:"BACKEND_SECRET_KEY" envDefault:""`
	SQLALCHEMY_DATABASE_URL string `env:"SQLALCHEMY_DATABASE_URL" envDefault:""`
	CLICKHOUSE_DATABASE_URL string `env:"CLICKHOUSE_DATABASE_URL" envDefault:""`
	REDIS_URL               string `env:"REDIS_URL" envDefault:""`
	MQTT_HOST				string `env:"MQTT_HOST" envDefault:""`
	MQTT_SECURE				bool   `env:"MQTT_SECURE" envDefault:"true"`
	MQTT_PORT				int    `env:"MQTT_PORT" envDefault:"1883"`
 	MQTT_KEEPALIVE			int    `env:"MQTT_KEEPALIVE" envDefault:"60"`
}

func generateToken(secretKey string) (string, error) {
	claims := jwt.MapClaims{
		"domain": "example.com",
		"type":   "Backend",
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	signedToken, err := token.SignedString([]byte(secretKey))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %v", err)
	}

	return signedToken, nil
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
	
	token, err := generateToken(cfg.BACKEND_SECRET_KEY)
	if err != nil {
		fmt.Println("Error generating token:", err)
		return
	}
	
	fmt.Println("Generated JWT token:", token)

}
