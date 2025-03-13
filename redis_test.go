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

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{Score: 100, Member: "Yusuf"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 85, Member: "Firlana"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 95, Member: "Shalma"})

	assert.Equal(t, []string{"Firlana", "Shalma", "Yusuf"}, client.ZRange(ctx, "scores", 0, -1).Val())

	assert.Equal(t, "Yusuf", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Shalma", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Firlana", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "Yusuf")
	client.HSet(ctx, "user:1", "email", "yusuf@mail.com")

	user := client.HGetAll(ctx, "user:1").Val()

	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "Yusuf", user["name"])
	assert.Equal(t, "yusuf@mail.com", user["email"])

	client.Del(ctx, "user:1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko A",
		Longitude: 106.818489,
		Latitude:  -6.178966,
	})
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko B",
		Longitude: 106.821568,
		Latitude:  -6.180662,
	})

	distance := client.GeoDist(ctx, "sellers", "Toko A", "Toko B", "km").Val()
	assert.Equal(t, 0.3892, distance)

	sellers := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Longitude:  106.819143,
		Latitude:   -6.180182,
		Radius:     5,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Toko A", "Toko B"}, sellers)
}
