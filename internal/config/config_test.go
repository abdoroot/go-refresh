package config

import "testing"

func TestGetEnvFallback(t *testing.T) {
	t.Setenv("MISSING_KEY", "")
	got := getEnv("MISSING_KEY", "fallback")
	if got != "fallback" {
		t.Fatalf("expected fallback, got %q", got)
	}
}

func TestGetEnvValue(t *testing.T) {
	t.Setenv("MY_KEY", "value")
	got := getEnv("MY_KEY", "fallback")
	if got != "value" {
		t.Fatalf("expected value, got %q", got)
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("PORT", "")
	t.Setenv("REDIS_HOST", "")
	t.Setenv("DATABASE_URL", "")

	cfg := Load()

	if cfg.ServerPort != "8080" {
		t.Fatalf("expected default port 8080, got %q", cfg.ServerPort)
	}
	if cfg.RedisHost != "localhost:6379" {
		t.Fatalf("expected default redis host, got %q", cfg.RedisHost)
	}
	if cfg.DatabaseURL != "" {
		t.Fatalf("expected empty database url, got %q", cfg.DatabaseURL)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("PORT", "9090")
	t.Setenv("REDIS_HOST", "redis:6379")
	t.Setenv("DATABASE_URL", "postgres://test")

	cfg := Load()

	if cfg.ServerPort != "9090" {
		t.Fatalf("expected port 9090, got %q", cfg.ServerPort)
	}
	if cfg.RedisHost != "redis:6379" {
		t.Fatalf("expected redis host redis:6379, got %q", cfg.RedisHost)
	}
	if cfg.DatabaseURL != "postgres://test" {
		t.Fatalf("expected db url postgres://test, got %q", cfg.DatabaseURL)
	}
}
