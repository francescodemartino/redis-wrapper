package redis_wrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
)

const MaxLimit = 500

type ConsumerGroup[T any] struct {
	streamName        string
	consumerGroupName string
}

func XAdd(streamName string, value any) {
	XAddWithMaxLen(streamName, value, MaxLimit)
}

func XAddWithMaxLen(streamName string, value any, maxLen int64) {
	mapValues := make(map[string]interface{})
	mapValues["value"], _ = json.Marshal(value)
	XAddRawWithMaxLen(streamName, mapValues, maxLen)
}

func XAddRaw(streamName string, values map[string]interface{}) {
	XAddRawWithMaxLen(streamName, values, MaxLimit)
}

func XAddRawWithMaxLen(streamName string, values map[string]interface{}, maxLen int64) {
	args := redis.XAddArgs{
		Stream:     streamName,
		NoMkStream: false,
		MaxLen:     maxLen,
		Approx:     true,
		Values:     values,
	}
	cmd := RedisClient.XAdd(context.Background(), &args)
	err := cmd.Err()
	fmt.Println(err)
}

func CreateConsumerGroup[T any](streamName string, consumerGroupName string) *ConsumerGroup[T] {
	RedisClient.XGroupCreateMkStream(context.Background(), streamName, consumerGroupName, "$")
	return &ConsumerGroup[T]{
		streamName:        streamName,
		consumerGroupName: consumerGroupName,
	}
}

func (c *ConsumerGroup[T]) XReadGroupRaw(numConsumer int, consumer func(data map[string]interface{})) {
	for i := 0; i < numConsumer; i++ {
		c.consumeXReadGroupRaw(c.getArgs(), consumer)
	}
	<-make(chan int)
}

func (c *ConsumerGroup[T]) XReadGroup(numConsumer int, consumer func(data T)) {
	for i := 0; i < numConsumer; i++ {
		c.consumeXReadGroup(c.getArgs(), consumer)
	}
	<-make(chan int)
}

func (c *ConsumerGroup[T]) consumeXReadGroupRaw(args redis.XReadGroupArgs, consumer func(data map[string]interface{})) {
	endExecution := make(chan struct{})
	for {
		streamData, _ := RedisClient.XReadGroup(context.Background(), &args).Result()
		for _, value := range streamData {
			for _, message := range value.Messages {
				go func() {
					defer sendEndConsumer(&endExecution)
					consumer(message.Values)
				}()
				<-endExecution
			}
		}
	}
}

func (c *ConsumerGroup[T]) consumeXReadGroup(args redis.XReadGroupArgs, consumer func(data T)) {
	endExecution := make(chan struct{})
	for {
		streamData, _ := RedisClient.XReadGroup(context.Background(), &args).Result()
		for _, value := range streamData {
			for _, message := range value.Messages {
				go func(xMessage *redis.XMessage) {
					defer sendEndConsumer(&endExecution)
					var value T
					err := json.Unmarshal([]byte(xMessage.Values["value"].(string)), &value)
					if err == nil {
						consumer(value)
					}
					<-endExecution
				}(&message)
			}
		}
	}
}

func (c *ConsumerGroup[T]) getArgs() redis.XReadGroupArgs {
	hostName, _ := os.Hostname()
	args := redis.XReadGroupArgs{
		Group:    c.consumerGroupName,
		Consumer: hostName,
		Streams:  []string{c.streamName, ">"},
		Count:    1,
		NoAck:    false,
	}
	return args
}

func sendEndConsumer(endChannel *chan struct{}) {
	if r := recover(); r != nil {
		// TODO add log
		fmt.Println("Recovered from panic error:", r)
	}
	*endChannel <- struct{}{}
}
