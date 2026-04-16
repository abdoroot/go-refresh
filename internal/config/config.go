package config

import (
	"os"
)

type Config struct {
	ServerPort  string
	RedisHost   string
	DatabaseURL string
}

func Load() Config {
	return Config{
		ServerPort:  getEnv("PORT", "8080"),
		RedisHost:   getEnv("REDIS_HOST", "localhost:6379"),
		DatabaseURL: getEnv("DATABASE_URL", ""),
	}
}

func getEnv(key, fallback string) string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	return v
}
