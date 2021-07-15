package adapter

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

//RedisPool redis pool
type RedisPool struct {
	clients         []*redis.Client
	roundRobinCount int
}

//First get first client
func (pool *RedisPool) First() *redis.Client {

	return pool.clients[0]
}

//SelectRobin select a client by using round robin
func (pool *RedisPool) SelectRobin() *redis.Client {

	pool.roundRobinCount++
	pool.roundRobinCount = pool.roundRobinCount % len(pool.clients)
	return pool.clients[pool.roundRobinCount]
}

// MARK: implement MemPool

//Init init pool from connection string
func (pool *RedisPool) Init(connectionString string) error {

	clients := strings.Split(connectionString, ",")

	for _, client := range clients {
		var numClient = 1
		hasNumClient := strings.Index(client, "[")

		if hasNumClient > 0 {
			end := strings.Index(client, "]")
			if end > hasNumClient {
				numString := client[hasNumClient:end]
				if tryParse, err := strconv.ParseInt(numString, 10, 64); err == nil {
					numClient = int(tryParse)
				}
			}
			client = client[0:hasNumClient]
		}
		var database = 0

		parts := strings.Split(client, "/")
		if len(parts) == 2 {
			client = parts[0]
			if tryDb, err := strconv.Atoi(parts[1]); err == nil {
				database = tryDb
			}
		}
		password := ""
		parts = strings.Split(client, "@")
		if len(parts) == 2 {
			password = parts[0]
			client = parts[1]
		}

		for i := 0; i < numClient; i++ {
			var redisClient = redis.NewClient(&redis.Options{

				Addr:     client,
				Password: password,
				DB:       database,
			})

			fmt.Println("redis new client ", client)
			pool.clients = append(pool.clients, redisClient)
		}
	}

	fmt.Println("redis pool ", len(pool.clients), " clients.")
	return nil
}

//Set set key
func (pool *RedisPool) Set(key string, value string) error {

	return pool.First().Set(key, value, 0).Err()
}

//SetExpire set key
func (pool *RedisPool) SetExpire(key string, value string, d time.Duration) error {

	return pool.First().Set(key, value, d).Err()
}

//Get get from key
func (pool *RedisPool) Get(key string) (string, error) {

	return pool.First().Get(key).Result()
}

//Del delete session
func (pool *RedisPool) Del(key string) error {

	_, err := pool.First().Del(key).Result()
	return err
}
