package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration for the backend service.
type Config struct {
	HTTPAddr         string
	DatabaseURL      string
	RedisAddr        string
	RedisPassword    string
	StreamName       string
	ConsumerGroup    string
	ConsumerName     string
	PendingTTL       time.Duration
	ShutdownTimeout  time.Duration
	ReadBlockTimeout time.Duration
}

// Load reads configuration from environment variables with sensible defaults.
func Load() Config {
	return Config{
		HTTPAddr:         getEnv("HTTP_ADDR", ":8080"),
		DatabaseURL:      getEnv("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"),
		RedisAddr:        getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    getEnv("REDIS_PASSWORD", ""),
		StreamName:       getEnv("STREAM_NAME", "order-events"),
		ConsumerGroup:    getEnv("CONSUMER_GROUP", "order-processors"),
		ConsumerName:     getEnv("CONSUMER_NAME", "worker-1"),
		PendingTTL:       getDuration("PENDING_TTL", 5*time.Minute),
		ShutdownTimeout:  getDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
		ReadBlockTimeout: getDuration("READ_BLOCK_TIMEOUT", 2*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if secs, err := strconv.Atoi(v); err == nil {
			return time.Duration(secs) * time.Second
		}
	}
	return fallback
}
