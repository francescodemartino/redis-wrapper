package redis_wrapper

import (
	"github.com/go-redis/redis/v8"
)

var RedisClient *redis.Client

func ConnectToRedis(host string, port string) {
	ConnectToRedisWithPasswordAndDb(host, port, "", 0)
}

func ConnectToRedisWithPasswordAndDb(host string, port string, password string, db int) {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     host + ":" + port,
		Password: password,
		DB:       db,
	})
}
