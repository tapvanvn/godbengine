package engine

import "time"

//MemPool memory pool
type MemPool interface {

	//Init init pool from connection string
	Init(connectionString string) error

	//MARK: SET FUNCTIONS

	//Set set key
	Set(key string, value string) error

	//SetShading select pool by shading the key
	SetShading(key string, value string) error

	//SetExpire set key with expire
	SetExpire(key string, value string, d time.Duration) error

	//SetExpireShading select pool by shading the key, set key value with expire
	SetExpireShading(key string, value string, d time.Duration) error

	//MARK: GET FUNCTIONS

	//Get get from key
	Get(key string) (string, error)

	//GetShading get from key that set by shading
	GetShading(key string) (string, error)

	//MARK: DEL FUNCTIONS

	//Del delete a key
	Del(key string) error

	//Del delete a key that set by shading
	DelShading(key string) error

	//MARK: QUERY FUNCTIONS

	//find all key in pattern
	FindKey(keyPattern string) ([]string, error)
}
