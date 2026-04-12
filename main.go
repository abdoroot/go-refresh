package main

import (
	"log/slog"
	"net/http"
	"os"
)

func main() {
	port, ok := os.LookupEnv("PORT")
	if !ok {
		slog.Warn("port env not set; using hard-coded port")
		port = "8080"
	}

	api := NewWeDispatcherAPI(port)

	if err := api.Serve(); err != nil && err != http.ErrServerClosed {
		slog.Error("error starting api server at", "port", port)
	}
}
