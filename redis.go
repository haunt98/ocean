package ocean

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type redisOption struct {
	expiration    time.Duration
	generateKeyFn GenerateKeyFn
	serializeFn   SerializeFn
	deserializeFn DeserializeFn
}

type RedisOptionFn func(option *redisOption)

const DefaultExpiration = time.Hour * 24

func WithExpiration(expiration time.Duration) RedisOptionFn {
	return func(option *redisOption) {
		option.expiration = expiration
	}
}

type GenerateKeyFn func(key string) string

var _ GenerateKeyFn = DefaultGenerateKeyFn

var DefaultGenerateKeyFn = func(key string) string {
	return key
}

func WithGenerateKeyFn(generateKeyFn GenerateKeyFn) RedisOptionFn {
	return func(option *redisOption) {
		option.generateKeyFn = generateKeyFn
	}
}

type SerializeFn func(v interface{}) ([]byte, error)

var _ SerializeFn = DefaultSerializeFn

var DefaultSerializeFn = func(v interface{}) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func WithSerializeFn(serializeFn SerializeFn) RedisOptionFn {
	return func(option *redisOption) {
		option.serializeFn = serializeFn
	}
}

type DeserializeFn func(data []byte, v interface{}) error

var _ DeserializeFn = DefaultDeserializeFn

var DefaultDeserializeFn = func(data []byte, v interface{}) error {
	if err := json.Unmarshal(data, v); err != nil {
		return err
	}

	return nil
}

func WithDeserializeFn(deserializeFn DeserializeFn) RedisOptionFn {
	return func(option *redisOption) {
		option.deserializeFn = deserializeFn
	}
}

func DefaultRedisOption() redisOption {
	return redisOption{
		expiration:    DefaultExpiration,
		generateKeyFn: DefaultGenerateKeyFn,
		serializeFn:   DefaultSerializeFn,
		deserializeFn: DefaultDeserializeFn,
	}
}

var _ Ocean = (*redisOcean)(nil)

type redisOcean struct {
	client *redis.Client
	option redisOption
}

func NewRedisOcean(client *redis.Client, optionFns ...RedisOptionFn) Ocean {
	option := DefaultRedisOption()

	for _, optionFn := range optionFns {
		optionFn(&option)
	}

	return &redisOcean{
		client: client,
		option: option,
	}
}

func (o *redisOcean) Get(ctx context.Context, key string, value interface{}) error {
	key = o.option.generateKeyFn(key)

	data, err := o.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("key %s does not exist", key)
		}

		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if err := o.option.deserializeFn(data, value); err != nil {
		return fmt.Errorf("failed to deserialize: %w", err)
	}

	return nil
}

func (o *redisOcean) Set(ctx context.Context, key string, value interface{}) error {
	key = o.option.generateKeyFn(key)

	data, err := o.option.serializeFn(value)
	if err != nil {
		return fmt.Errorf("failed to serialize: %w", err)
	}

	if err := o.client.Set(ctx, key, data, o.option.expiration).Err(); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}
