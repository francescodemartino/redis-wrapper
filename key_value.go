package redis_wrapper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"time"
)

func HasKeyByString(path string, key string) bool {
	err := RedisClient.Get(context.Background(), path+":"+key).Err()
	return !errors.Is(err, redis.Nil)
}

func HasKey(path string, key []byte) bool {
	hashKey := sha256.Sum256(key)
	return HasKeyByString(path, hex.EncodeToString(hashKey[:]))
}

func GetStringByString(path string, key string) (string, bool) {
	value := RedisClient.Get(context.Background(), path+":"+key)
	return value.Val(), !errors.Is(value.Err(), redis.Nil)
}

func GetStructByString(path string, key string, value any) (error, bool) {
	outString, exists := GetStringByString(path, key)
	return json.Unmarshal([]byte(outString), value), exists
}

func GetStruct(path string, key []byte, value any) (error, bool) {
	hashKey := sha256.Sum256(key)
	return GetStructByString(path, hex.EncodeToString(hashKey[:]), value)
}

func GetString(path string, key []byte) (string, bool) {
	hashKey := sha256.Sum256(key)
	return GetStringByString(path, hex.EncodeToString(hashKey[:]))
}

func SetStringByString(path string, key string, value string, expiration time.Duration) {
	RedisClient.Set(context.Background(), path+":"+key, value, expiration)
}

func SetStructByString(path string, key string, value any, expiration time.Duration) error {
	out, err := json.Marshal(value)
	if err == nil {
		SetStringByString(path, key, string(out), expiration)
	}
	return err
}

func SetStruct(path string, key []byte, value any, expiration time.Duration) error {
	hashKey := sha256.Sum256(key)
	return SetStructByString(path, hex.EncodeToString(hashKey[:]), value, expiration)
}

func SetString(path string, key []byte, value string, expiration time.Duration) {
	hashKey := sha256.Sum256(key)
	SetStringByString(path, hex.EncodeToString(hashKey[:]), value, expiration)
}
