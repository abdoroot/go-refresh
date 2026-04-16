package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/abdoroot/go-refresh/internal/config"
	"github.com/abdoroot/go-refresh/internal/dispatch"
	"github.com/abdoroot/go-refresh/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

const DB_CONN_TIMEOUT = time.Second * 2

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(".env not found")
	}

	config := config.Load()
	postgresConn, err := getPostgresConn(config)
	if err != nil {
		slog.Error("error getting potgres connection", "port", config.RedisHost)
		return
	}
	DispatcherRepo := storage.NewDispatcherRepo(postgresConn)

	api := dispatch.NewDispatcherAPI(config, DispatcherRepo)

	if err := api.Run(); err != nil && err != http.ErrServerClosed {
		slog.Error("error starting api server at", "port", config.RedisHost)
	}
}

func getPostgresConn(config config.Config) (*pgx.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DB_CONN_TIMEOUT)
	defer cancel()

	conn, err := pgx.Connect(ctx, config.DatabaseURL)
	if err != nil {
		return conn, err
	}

	return conn, nil
}
