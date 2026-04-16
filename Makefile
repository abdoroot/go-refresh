APP_NAME ?= go-refresh
PORT ?= 8080

.PHONY: build run test

build:
	go build -o $(APP_NAME) ./cmd

run:
	PORT=$(PORT) REDIS_HOST=$(REDIS_HOST) go run ./cmd

test:
	go test -count=1 ./...