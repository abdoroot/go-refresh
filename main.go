package main

import (
	"log/slog"
	"net/http"
	"os"
)

func main() {
	srvPort, ok := os.LookupEnv("PORT")
	if !ok {
		slog.Warn("port env not set; using hard-coded port")
		srvPort = "8080"
	}

	redisHost, ok := os.LookupEnv("REDIS_HOST")
	if !ok {
		slog.Warn("REDIS_HOST env not set; using default host")
		redisHost = "localhost:6379"
	}

	api := NewDispatcherAPI(srvPort, redisHost)

	if err := api.Serve(); err != nil && err != http.ErrServerClosed {
		slog.Error("error starting api server at", "port", srvPort)
	}
}
