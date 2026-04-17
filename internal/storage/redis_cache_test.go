package storage

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestNewRedisStorage(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	r := NewRedisStorage(client)
	if r == nil || r.client == nil {
		t.Fatalf("expected redis storage with client")
	}
}

func TestRedisStoragePushPopError(t *testing.T) {
	r := NewRedisStorage(redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}))
	ctx := context.Background()

	if err := r.Push(ctx, "k", "1"); err == nil {
		t.Fatalf("expected push error")
	}
	if _, err := r.Pop(ctx, "k"); err == nil {
		t.Fatalf("expected pop error")
	}
}

func TestRedisStorageClose(t *testing.T) {
	r := &RedisStorage{}
	if err := r.Close(); err != nil {
		t.Fatalf("expected nil close for nil client, got %v", err)
	}

	r = NewRedisStorage(redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"}))
	if err := r.Close(); err != nil {
		t.Fatalf("expected close success, got %v", err)
	}
}
