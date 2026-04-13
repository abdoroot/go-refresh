APP_NAME ?= go-refresh
PORT ?= 8080
REDIS_HOST ?= localhost:6379

.PHONY: build run test

build:
	go build -o $(APP_NAME) .

run:
	PORT=$(PORT) REDIS_HOST=$(REDIS_HOST) go run .

test:
	go test ./...
