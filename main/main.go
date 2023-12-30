package main

import redis_wrapper "gitlab.com/evendo-project/redis-wrapper"

func main() {
	redis_wrapper.ConnectToRedis("localhost", "6379")

	t := redis_wrapper.Test1{
		Num:  12,
		Text: "ciao come va",
		Flag: true,
		Value: redis_wrapper.Test2{
			Count: 1244,
			Name:  "pippo",
		},
	}

	for i := 0; i < 1; i++ {
		redis_wrapper.XAdd("test:stream", t)
		redis_wrapper.XAddRaw("test:stream", map[string]interface{}{
			"name": "come va",
			"age":  12,
		})
	}
}
