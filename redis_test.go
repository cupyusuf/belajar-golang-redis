package belajar_golang_redis

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)

	// err := client.Close()
	// assert.Nil(t, err)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "Yusuf Supriadi", 3*time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Yusuf Supriadi", result)

	time.Sleep(5 * time.Second)

	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}
