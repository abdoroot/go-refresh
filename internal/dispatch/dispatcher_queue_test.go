package dispatch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
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
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if h.process == nil {
				continue
			}
			if err := h.process(ctx, cmd); err != nil {
				return err
			}
		}
		return nil
	}
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
		rdb:    rdb,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
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

func TestUpdateDispatchJobStatusReturnsRedisExistsError(t *testing.T) {
	redisErr := errors.New("redis exists failed")
	api := newTestDispatcher(func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "exists") {
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

func TestUpdateDispatchJobStatusReturnsNotFound(t *testing.T) {
	api := newTestDispatcher(func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "exists") {
			redisCmd, ok := cmd.(*redis.IntCmd)
			if !ok {
				t.Fatalf("expected *redis.IntCmd, got %T", cmd)
			}
			redisCmd.SetVal(0)
		}
		return nil
	})

	err := api.updateDispatchJobStatus(1, dispatchStatusProcessing)
	if err == nil || !strings.Contains(err.Error(), "job 1 not found") {
		t.Fatalf("expected job not found error, got %v", err)
	}
}

func TestUpdateDispatchJobStatusUsesRedisHashCommands(t *testing.T) {
	var sawHIncrBy bool
	var sawHSet bool

	api := newTestDispatcher(func(ctx context.Context, cmd redis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "exists") {
			redisCmd, ok := cmd.(*redis.IntCmd)
			if !ok {
				t.Fatalf("expected *redis.IntCmd, got %T", cmd)
			}
			redisCmd.SetVal(1)
		}
		if strings.EqualFold(cmd.Name(), "hincrby") {
			sawHIncrBy = true
		}
		if strings.EqualFold(cmd.Name(), "hset") {
			sawHSet = true
		}
		return nil
	})

	if err := api.updateDispatchJobStatus(1, dispatchStatusProcessing); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !sawHIncrBy {
		t.Fatal("expected HINCRBY command")
	}
	if !sawHSet {
		t.Fatal("expected HSET command")
	}
}
