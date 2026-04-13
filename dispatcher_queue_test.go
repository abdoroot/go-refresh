package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
)

type redisCommandHook struct {
	process func(context.Context, redis.Cmder) error
}

func (h redisCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h redisCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.process == nil {
			return nil
		}
		return h.process(ctx, cmd)
	}
}

func (h redisCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func newTestDispatcher(process func(context.Context, redis.Cmder) error) *DispatcherAPI {
	rdb := redis.NewClient(&redis.Options{
		Addr: "redis.test:6379",
		Dialer: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			return nil, errors.New("unexpected redis dial")
		},
	})
	rdb.AddHook(redisCommandHook{process: process})

	return &DispatcherAPI{
		rdb:     rdb,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		lastKey: &atomic.Uint32{},
	}
}

func TestGetNewJobIDReturnsRedisError(t *testing.T) {
	redisErr := errors.New("redis set failed")
	api := newTestDispatcher(func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "set") {
			cmd.SetErr(redisErr)
			return redisErr
		}
		return nil
	})

	_, err := api.getNewJobID()
	if !errors.Is(err, redisErr) {
		t.Fatalf("expected redis error, got %v", err)
	}
}

func TestHandleCreateJobReturnsJobSetError(t *testing.T) {
	redisErr := errors.New("redis job hset failed")
	api := newTestDispatcher(func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "hset") {
			cmd.SetErr(redisErr)
			return redisErr
		}
		return nil
	})

	_, err := api.handleCreateJob(strings.NewReader(`{"channel":"email","recipient":"user@example.com","message":"hello"}`))
	if !errors.Is(err, redisErr) {
		t.Fatalf("expected redis error, got %v", err)
	}
}

func TestHandleCreateJobReturnsQueuePushError(t *testing.T) {
	redisErr := errors.New("redis rpush failed")
	api := newTestDispatcher(func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "rpush") {
			cmd.SetErr(redisErr)
			return redisErr
		}
		return nil
	})

	_, err := api.handleCreateJob(strings.NewReader(`{"channel":"email","recipient":"user@example.com","message":"hello"}`))
	if !errors.Is(err, redisErr) {
		t.Fatalf("expected redis error, got %v", err)
	}
}

func TestUpdateDispatchJobStatusReturnsRedisGetError(t *testing.T) {
	redisErr := errors.New("redis hgetall failed")
	api := newTestDispatcher(func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "hgetall") {
			cmd.SetErr(redisErr)
			return redisErr
		}
		return nil
	})

	err := api.updateDispatchJobStatus(1, dispatchStatusProcessing)
	if !errors.Is(err, redisErr) {
		t.Fatalf("expected redis error, got %v", err)
	}
}
