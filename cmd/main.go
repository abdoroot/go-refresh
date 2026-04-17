package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/abdoroot/go-refresh/internal/config"
	"github.com/abdoroot/go-refresh/internal/dispatch"
	"github.com/abdoroot/go-refresh/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

const DB_CONN_TIMEOUT = time.Second * 2

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(".env not found")
	}

	config := config.Load()
	postgresConn, err := createPostgresConn(config)
	if err != nil {
		slog.Error("error getting postgres connection", "port", config.ServerPort)
		return
	}
	DispatcherRepo := storage.NewDispatcherRepo(postgresConn)

	c, err := createRedisClient(config)
	if err != nil {
		slog.Error("error getting redis client", "port", config.RedisHost)
		return
	}

	cacheStore := storage.NewRedisStorage(c)

	api := dispatch.NewDispatcherAPI(config, DispatcherRepo, cacheStore)

	if err := api.Run(); err != nil && err != http.ErrServerClosed {
		slog.Error("error starting api server at", "port", config.RedisHost)
	}
}

func createPostgresConn(config config.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DB_CONN_TIMEOUT)
	defer cancel()

	pool, err := pgxpool.New(ctx, config.DatabaseURL)
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func createRedisClient(config config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisHost,
		Password: "",
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), storage.RedisConnectionTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return client, nil
}
