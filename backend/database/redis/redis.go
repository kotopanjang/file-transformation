package redis

import (
	"encoding/json"
	"errors"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
	"github.com/rs/zerolog/log"
)

type IRedisStorage interface {
	Set(key string, dataset any) error
	GetByKey(key string, convertValue any) error
}

type RedisStorage struct {
	client *redis.Client
}

func NewRedis(addr, password string) *RedisStorage {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})

	return &RedisStorage{
		client: client,
	}
}

func NewTestRedis() *RedisStorage {
	redisServer, _ := miniredis.Run()
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})

	return &RedisStorage{
		client: redisClient,
	}
}

func (r *RedisStorage) Set(key string, dataset any) error {
	return r.client.Set(key, dataset, 0).Err()
}

func (r *RedisStorage) GetByKey(key string, convertValue any) error {
	val, err := r.client.Get(key).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Err(err).Msg(err.Error())
			return err
		}
	}

	if val != "" {
		err = json.Unmarshal([]byte(val), &convertValue)
		if err != nil {
			log.Err(err).Msg(err.Error())
			return err
		}
	}

	return nil
}
