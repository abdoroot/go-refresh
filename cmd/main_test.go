package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/abdoroot/go-refresh/internal/config"
)

func TestCreatePostgresConnInvalidURL(t *testing.T) {
	_, err := createPostgresConn(config.Config{DatabaseURL: "://invalid"})
	if err == nil {
		t.Fatalf("expected postgres connection error")
	}
}

func TestCreateRedisClientInvalidHost(t *testing.T) {
	_, err := createRedisClient(config.Config{RedisHost: "127.0.0.1:0"})
	if err == nil {
		t.Fatalf("expected redis connection error")
	}
}

func TestMainPanicsWithoutEnvFile(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir temp: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldWD)
	})

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when .env is missing")
		}
	}()
	main()
}

func TestMainReturnsOnDatabaseError(t *testing.T) {
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir temp: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldWD)
	})

	envPath := filepath.Join(tmp, ".env")
	content := "PORT=8080\nREDIS_HOST=127.0.0.1:6379\nDATABASE_URL=://invalid\n"
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	main()
}
