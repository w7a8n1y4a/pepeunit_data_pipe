package database

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

type RedisDB struct {
	client *redis.Client
}

func NewRedis(url string) (*RedisDB, error) {
	// Remove redis:// prefix
	redisURL := strings.TrimPrefix(url, "redis://")

	// Extract DB number from suffix
	dbNum := 0
	if strings.HasSuffix(redisURL, "/0") {
		redisURL = strings.TrimSuffix(redisURL, "/0")
	} else if strings.HasSuffix(redisURL, "/1") {
		redisURL = strings.TrimSuffix(redisURL, "/1")
		dbNum = 1
	} else if strings.HasSuffix(redisURL, "/2") {
		redisURL = strings.TrimSuffix(redisURL, "/2")
		dbNum = 2
	}

	client := redis.NewClient(&redis.Options{
		Addr:     redisURL,
		Password: "", // no password set
		DB:       dbNum,
	})

	// Test connection
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Printf("Success connected to Redis at %s (DB: %d)", redisURL, dbNum)
	return &RedisDB{client: client}, nil
}

func (db *RedisDB) Close() {
	if db.client != nil {
		db.client.Close()
	}
}

// ReadStream reads messages from a Redis stream
func (db *RedisDB) ReadStream(ctx context.Context, streamName string, lastID string) ([]redis.XMessage, error) {
	streams, err := db.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{streamName, lastID},
		Block:   0,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to read from Redis stream: %w", err)
	}

	if len(streams) == 0 {
		return nil, nil
	}

	return streams[0].Messages, nil
}

// AddToStream adds a message to a Redis stream
func (db *RedisDB) AddToStream(ctx context.Context, streamName string, values map[string]interface{}) (string, error) {
	id, err := db.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: values,
	}).Result()

	if err != nil {
		return "", fmt.Errorf("failed to add message to Redis stream: %w", err)
	}

	return id, nil
}

// GetStreamLength returns the number of messages in a stream
func (db *RedisDB) GetStreamLength(ctx context.Context, streamName string) (int64, error) {
	length, err := db.client.XLen(ctx, streamName).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get stream length: %w", err)
	}
	return length, nil
}

// GetLastMessageID returns the ID of the last message in a stream
func (db *RedisDB) GetLastMessageID(ctx context.Context, streamName string) (string, error) {
	messages, err := db.client.XRevRange(ctx, streamName, "+", "-").Result()
	if err != nil {
		return "", fmt.Errorf("failed to get last message ID: %w", err)
	}
	if len(messages) == 0 {
		return "0", nil
	}
	return messages[0].ID, nil
}
