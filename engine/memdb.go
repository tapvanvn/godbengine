package engine

import "time"

//MemPool memory pool
type MemPool interface {

	//Init init pool from connection string
	Init(connectionString string) error

	//Set set key
	Set(key string, value string) error

	//SetExpire set key
	SetExpire(key string, value string, d time.Duration) error

	//Get get from key
	Get(key string) (string, error)

	//Del delete a key
	Del(key string) error
}
