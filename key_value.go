package redis_wrapper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
)

func GetStringByString(path string, key string) string {
	value := RedisClient.Get(context.Background(), path+":"+key)
	return value.Val()
}

func GetStructByString(path string, key string, value any) error {
	outString := GetStringByString(path, key)
	return json.Unmarshal([]byte(outString), value)
}

func GetStruct(path string, key []byte, value any) error {
	hashKey := sha256.Sum256(key)
	return GetStructByString(path, hex.EncodeToString(hashKey[:]), value)
}

func GetString(path string, key []byte) string {
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
