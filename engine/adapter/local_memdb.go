package adapter

import (
	"sync"
	"time"

	"github.com/tapvanvn/godbengine/engine"
)

// This LocalMemDB design for testing on local only. On production or multiple user system considering using others.
type value struct {
	isInt64 bool
}

type LocalMemDB struct {
	storageString map[string]string
	storageInt64  map[string]int64
	expire        map[string]int64
	muxString     sync.Mutex
	muxInt64      sync.Mutex
	muxExpire     sync.Mutex
}

func (memdb *LocalMemDB) Init(connectionString string) error {

	memdb.storageInt64 = map[string]int64{}
	memdb.storageString = map[string]string{}
	memdb.expire = map[string]int64{}
	return nil
}

//Set set key
func (memdb *LocalMemDB) Set(key string, value string) error {
	memdb.muxString.Lock()
	defer memdb.muxString.Unlock()
	memdb.storageString[key] = value
	return nil
}

func (memdb *LocalMemDB) SetInt(key string, value int64) error {
	memdb.muxInt64.Lock()
	defer memdb.muxInt64.Unlock()
	memdb.storageInt64[key] = value
	return nil
}

func (memdb *LocalMemDB) IncrInt(key string) (int64, error) {
	memdb.muxInt64.Lock()
	defer memdb.muxInt64.Unlock()
	value := int64(0)
	if val, ok := memdb.storageInt64[key]; ok {
		value = val
	}
	value++
	memdb.storageInt64[key] = value
	return value, nil
}

func (memdb *LocalMemDB) DecrInt(key string) (int64, error) {
	memdb.muxInt64.Lock()
	defer memdb.muxInt64.Unlock()
	value := int64(0)
	if val, ok := memdb.storageInt64[key]; ok {
		value = val
	}
	value--
	memdb.storageInt64[key] = value
	return value, nil
}

func (memdb *LocalMemDB) IncrIntBy(key string, num int64) (int64, error) {
	memdb.muxInt64.Lock()
	defer memdb.muxInt64.Unlock()
	value := int64(0)
	if val, ok := memdb.storageInt64[key]; ok {
		value = val
	}
	value += num
	memdb.storageInt64[key] = value
	return value, nil
}

func (memdb *LocalMemDB) DecrIntBy(key string, num int64) (int64, error) {
	memdb.muxInt64.Lock()
	defer memdb.muxInt64.Unlock()
	value := int64(0)
	if val, ok := memdb.storageInt64[key]; ok {
		value = val
	}
	value -= num
	memdb.storageInt64[key] = value
	return value, nil
}

//SetShading select pool by shading the key
func (memdb *LocalMemDB) SetShading(key string, value string) error {
	return memdb.Set(key, value)
}
func (memdb *LocalMemDB) SetIntShading(key string, value int64) error {
	return memdb.SetInt(key, value)
}
func (memdb *LocalMemDB) IncrIntShading(key string) (int64, error) {
	return memdb.IncrInt(key)
}
func (memdb *LocalMemDB) DescIntShading(key string) (int64, error) {
	return memdb.DecrInt(key)
}
func (memdb *LocalMemDB) IncrIntByShading(key string, num int64) (int64, error) {
	return memdb.IncrIntBy(key, num)
}
func (memdb *LocalMemDB) DecrIntByShading(key string, num int64) (int64, error) {
	return memdb.DecrIntBy(key, num)
}

//SetExpire set key with expire
func (memdb *LocalMemDB) SetExpire(key string, value string, d time.Duration) error {
	memdb.muxString.Lock()
	defer memdb.muxString.Unlock()
	memdb.storageString[key] = value

	memdb.muxExpire.Lock()
	defer memdb.muxExpire.Unlock()
	memdb.expire[key] = time.Now().Unix() + int64(d)
	return nil
}
func (memdb *LocalMemDB) SetIntExpire(key string, value int64, d time.Duration) error {
	memdb.muxInt64.Lock()
	defer memdb.muxInt64.Unlock()
	memdb.storageInt64[key] = value
	memdb.muxExpire.Lock()
	defer memdb.muxExpire.Unlock()
	memdb.expire[key] = time.Now().Unix() + int64(d)
	return nil
}

//SetExpireShading select pool by shading the key, set key value with expire
func (memdb *LocalMemDB) SetExpireShading(key string, value string, d time.Duration) error {
	return nil
}
func (memdb *LocalMemDB) SetIntExpireShading(key string, value int64, d time.Duration) error {
	return nil
}

//MARK: GET FUNCTIONS

//Get get from key
func (memdb *LocalMemDB) Get(key string) (string, error) {
	memdb.muxString.Lock()
	defer memdb.muxString.Unlock()
	if val, ok := memdb.storageString[key]; ok {
		memdb.muxExpire.Lock()
		defer memdb.muxExpire.Unlock()
		if exp, ok := memdb.expire[key]; ok {
			if time.Now().Unix() < exp {
				return val, nil
			}
		} else {
			return val, nil
		}
	}
	return "", nil
}
func (memdb *LocalMemDB) GetInt(key string) (int64, error) {
	memdb.muxInt64.Lock()
	defer memdb.muxInt64.Unlock()
	if val, ok := memdb.storageInt64[key]; ok {
		memdb.muxExpire.Lock()
		defer memdb.muxExpire.Unlock()
		if exp, ok := memdb.expire[key]; ok {
			if time.Now().Unix() < exp {
				return val, nil
			}
		} else {
			return val, nil
		}
	}
	return 0, nil
}

//GetShading get from key that set by shading
func (memdb *LocalMemDB) GetShading(key string) (string, error) {
	return memdb.Get(key)
}
func (memdb *LocalMemDB) GetIntShading(key string) (int64, error) {
	return memdb.GetInt(key)
}

//MARK: DEL FUNCTIONS

//Del delete a key
func (memdb *LocalMemDB) Del(key string) error {
	memdb.muxString.Lock()
	delete(memdb.storageString, key)
	memdb.muxString.Unlock()
	memdb.muxInt64.Lock()
	delete(memdb.storageInt64, key)
	memdb.muxInt64.Unlock()
	return nil
}

//Del delete a key that set by shading
func (memdb *LocalMemDB) DelShading(key string) error {
	return memdb.Del(key)
}

//MARK: QUERY FUNCTIONS

//find all key in pattern
func (memdb *LocalMemDB) FindKey(keyPattern string) ([]string, error) {
	return nil, engine.NotImplement
}
