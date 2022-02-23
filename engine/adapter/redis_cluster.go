package adapter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	redis "github.com/go-redis/redis/v8"
)

//RedisPool redis pool
type RedisClusterPool struct {
	clients         []*redis.ClusterClient
	roundRobinCount int
	segment         []int
	roundPools      []int
	segmentBegin    []int
}

//First get first client
func (pool *RedisClusterPool) First() *redis.ClusterClient {
	pool.roundPools[0]++
	pool.roundPools[0] = pool.roundPools[0] % pool.segment[0]
	return pool.clients[pool.roundPools[0]]
}

//SelectID get pool by id
func (pool *RedisClusterPool) SelectID(poolID int) *redis.ClusterClient {

	pool.roundPools[poolID]++
	pool.roundPools[poolID] = pool.roundPools[poolID] % pool.segment[poolID]

	index := pool.segmentBegin[poolID] + pool.roundPools[poolID]

	return pool.clients[index]
}

//SelectRobin select a client by using round robin
func (pool *RedisClusterPool) SelectRobin() *redis.ClusterClient {

	pool.roundRobinCount++
	pool.roundRobinCount = pool.roundRobinCount % len(pool.segmentBegin)

	var round *int = &pool.roundPools[pool.roundRobinCount]
	*round++
	*round = *round % pool.segment[pool.roundRobinCount]

	index := pool.segmentBegin[pool.roundRobinCount] + *round
	return pool.clients[index]
}

func (pool *RedisClusterPool) SelectShading(key string) *redis.ClusterClient {

	poolID := hashByKey(key) % len(pool.segmentBegin)

	var round *int = &pool.roundPools[poolID]
	*round++
	*round = *round % pool.segment[poolID]

	index := pool.segmentBegin[poolID] + *round
	return pool.clients[index]
}

// MARK: implement MemPool
//Init init pool from connection string
func (pool *RedisClusterPool) Init(connectionString string) error {

	clients := strings.Split(connectionString, ",")
	last := 0
	for _, client := range clients {
		var numClient = 1
		hasNumClient := strings.Index(client, "[")

		if hasNumClient > 0 {
			end := strings.Index(client, "]")
			if end > hasNumClient {
				numString := client[hasNumClient+1 : end]
				if tryParse, err := strconv.ParseInt(numString, 10, 64); err == nil {
					numClient = int(tryParse)
				}
			}
			client = client[0:hasNumClient]
		}
		//var database = 0

		parts := strings.Split(client, "/")
		if len(parts) == 2 {
			client = parts[0]
			//if tryDb, err := strconv.Atoi(parts[1]); err == nil {
			//database = tryDb
			//}
		}
		password := ""
		parts = strings.Split(client, "@")
		if len(parts) == 2 {
			password = parts[0]
			client = parts[1]
		}

		for i := 0; i < numClient; i++ {
			var redisClient = redis.NewClusterClient(&redis.ClusterOptions{

				Addrs:    []string{client},
				Password: password,
			})

			pool.clients = append(pool.clients, redisClient)

			fmt.Println("redis new client ", client)
		}
		pool.segment = append(pool.segment, numClient)
		pool.roundPools = append(pool.roundPools, 0)
		pool.segmentBegin = append(pool.segmentBegin, last)
		last += numClient
	}
	fmt.Println("redis pool ", len(pool.clients), " clients.")
	return nil
}

//Set set key
func (pool *RedisClusterPool) Set(key string, value string) error {

	return pool.First().Set(context.Background(), key, value, 0).Err()
}
func (pool *RedisClusterPool) SetInt(key string, value int64) error {

	return pool.First().Set(context.Background(), key, value, 0).Err()
}
func (pool *RedisClusterPool) IncrInt(key string) (int64, error) {

	return pool.First().Incr(context.Background(), key).Result()
}
func (pool *RedisClusterPool) DecrInt(key string) (int64, error) {

	return pool.First().Decr(context.Background(), key).Result()
}
func (pool *RedisClusterPool) IncrIntBy(key string, num int64) (int64, error) {

	return pool.First().IncrBy(context.Background(), key, num).Result()
}
func (pool *RedisClusterPool) DecrIntBy(key string, num int64) (int64, error) {

	return pool.First().DecrBy(context.Background(), key, num).Result()
}

//MARK: Shading
//SetShading select pool by shading the key
func (pool *RedisClusterPool) SetShading(key string, value string) error {

	return pool.SelectShading(key).Set(context.Background(), key, value, 0).Err()
}
func (pool *RedisClusterPool) SetIntShading(key string, value int64) error {

	return pool.SelectShading(key).Set(context.Background(), key, value, 0).Err()
}
func (pool *RedisClusterPool) IncrIntShading(key string) (int64, error) {

	return pool.SelectShading(key).Incr(context.Background(), key).Result()
}
func (pool *RedisClusterPool) DescIntShading(key string) (int64, error) {

	return pool.SelectShading(key).Decr(context.Background(), key).Result()
}

func (pool *RedisClusterPool) IncrIntByShading(key string, num int64) (int64, error) {

	return pool.SelectShading(key).IncrBy(context.Background(), key, num).Result()
}

func (pool *RedisClusterPool) DecrIntByShading(key string, num int64) (int64, error) {

	return pool.SelectShading(key).DecrBy(context.Background(), key, num).Result()
}

//SetExpire set key
func (pool *RedisClusterPool) SetExpire(key string, value string, d time.Duration) error {

	return pool.First().Set(context.Background(), key, value, d).Err()
}
func (pool *RedisClusterPool) SetIntExpire(key string, value int64, d time.Duration) error {

	return pool.First().Set(context.Background(), key, value, d).Err()
}

//SetExpire set key with expire
func (pool *RedisClusterPool) SetExpireShading(key string, value string, d time.Duration) error {

	return pool.SelectShading(key).Set(context.Background(), key, value, d).Err()
}

func (pool *RedisClusterPool) SetIntExpireShading(key string, value int64, d time.Duration) error {

	return pool.SelectShading(key).Set(context.Background(), key, value, d).Err()
}

//Get get from key
func (pool *RedisClusterPool) Get(key string) (string, error) {

	return pool.First().Get(context.Background(), key).Result()
}
func (pool *RedisClusterPool) GetInt(key string) (int64, error) {
	return pool.First().IncrBy(context.Background(), key, 0).Result()
}

//GetShading get from key that set by shading
func (pool *RedisClusterPool) GetShading(key string) (string, error) {

	return pool.SelectShading(key).Get(context.Background(), key).Result()
}

func (pool *RedisClusterPool) GetIntShading(key string) (int64, error) {

	return pool.SelectShading(key).IncrBy(context.Background(), key, 0).Result()
}

//Del delete session
func (pool *RedisClusterPool) Del(key string) error {

	_, err := pool.First().Del(context.Background(), key).Result()
	return err
}

//Del delete a key that set by shading
func (pool *RedisClusterPool) DelShading(key string) error {

	_, err := pool.SelectShading(key).Del(context.Background(), key).Result()
	return err
}

func (pool *RedisClusterPool) FindKey(keyPattern string) ([]string, error) {

	keys := []string{}

	for poolID := 0; poolID < len(pool.segmentBegin); poolID++ {

		cursor := uint64(0)
		var findMax = int64(100)
		for {
			cmd := pool.SelectID(poolID).Scan(context.Background(), cursor, keyPattern, findMax)
			rkeys, rcursor, rerr := cmd.Result()
			if rerr != nil {
				return nil, rerr
			}
			cursor = rcursor
			for _, key := range rkeys {
				keys = append(keys, key)
			}
			if len(rkeys) < int(findMax) {
				break
			}
		}
	}
	return keys, nil
}

func (pool *RedisClusterPool) IsNotExistedError(err error) bool {

	return err == redis.Nil
}
