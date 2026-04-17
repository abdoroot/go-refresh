package storage

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const RedisConnectionTimeout = time.Second * 2

type RedisStorage struct {
	client *redis.Client
}

func NewRedisStrorage(client *redis.Client) *RedisStorage {
	return &RedisStorage{
		client: client,
	}
}

func (r *RedisStorage) Push(ctx context.Context, key string, val any) error {
	return r.client.RPush(ctx, key, val).Err()
}
func (r *RedisStorage) Pop(ctx context.Context, key string) (uint32, error) {
	res, err := r.client.BLPop(ctx, RedisConnectionTimeout, key).Result()
	if err != nil {
		return 0, err
	}

	idString := res[1]
	id, err := strconv.ParseUint(idString, 10, 32)
	
	return uint32(id), nil
}
func (r *RedisStorage) Close() error {
	return r.Close()
}
