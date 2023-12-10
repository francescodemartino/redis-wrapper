package redis_wrapper

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
)

type MessageSub[T any] struct {
	Body T
	Err  error
}

type ChannelRedis[T any] struct {
	channelSub     chan MessageSub[T]
	pubSubMessages *redis.PubSub
}

func (c ChannelRedis[T]) Channel() chan MessageSub[T] {
	return c.channelSub
}

func (c ChannelRedis[T]) Close() {
	c.pubSubMessages.Close()
	close(c.channelSub)
}

func Publish(channel string, message any) error {
	bodyMessage, err := json.Marshal(message)
	if err == nil {
		err = RedisClient.Publish(context.Background(), channel, string(bodyMessage)).Err()
	}
	return err
}

func Subscribe[T any](channels ...string) ChannelRedis[T] {
	channelSub := make(chan MessageSub[T])
	pubSubMessages := RedisClient.Subscribe(context.Background(), channels...)
	channelSubRedis := pubSubMessages.Channel()
	go func() {
		for msg := range channelSubRedis {
			var value T
			err := json.Unmarshal([]byte(msg.Payload), &value)
			channelSub <- MessageSub[T]{
				Body: value,
				Err:  err,
			}
		}
	}()
	return ChannelRedis[T]{
		channelSub:     channelSub,
		pubSubMessages: pubSubMessages,
	}
}
