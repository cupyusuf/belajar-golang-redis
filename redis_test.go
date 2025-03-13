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

func TestList(t *testing.T) {
	client.RPush(ctx, "names", "Firlana")
	client.RPush(ctx, "names", "Luchiana")
	client.RPush(ctx, "names", "Dewi")

	assert.Equal(t, "Firlana", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Luchiana", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Dewi", client.LPop(ctx, "names").Val())

	client.Del(ctx, "names")
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "students", "Firlana")
	client.SAdd(ctx, "students", "Firlana")
	client.SAdd(ctx, "students", "Luchiana")
	client.SAdd(ctx, "students", "Luchiana")
	client.SAdd(ctx, "students", "Dewi")
	client.SAdd(ctx, "students", "Dewi")

	assert.Equal(t, int64(3), client.SCard(ctx, "students").Val())
	assert.Equal(t, []string{"Firlana", "Luchiana", "Dewi"}, client.SMembers(ctx, "students").Val())
}
