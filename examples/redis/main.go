package main

import (
	"context"
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/haunt98/assert"
	"github.com/haunt98/ocean"
	"github.com/spf13/viper"
)

func main() {
	viper.AutomaticEnv()

	address := viper.GetString("address")
	assert.True(address != "", "empty address")

	password := viper.GetString("password")

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
	})

	oc := ocean.NewRedisOcean(client)

	key := viper.GetString("key")
	assert.True(key != "", "empty key")

	if err := oc.Set(context.Background(), key, "Hello world"); err != nil {
		log.Fatalf("failed to set: %s\n", err)
	}

	var value string

	if err := oc.Get(context.Background(), key, &value); err != nil {
		log.Fatalf("failed to get: %s\n", err)
	}

	log.Printf("value %s\n", value)
}
