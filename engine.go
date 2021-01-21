package engines

import (
	"github.com/tapvanvn/godbengine/engine"
)

var uniqueEngine *engine.Engine = nil

var InitEngineFunc func(*engine.Engine) = nil

//GetEngine engine
func GetEngine() *engine.Engine {

	if uniqueEngine == nil {

		uniqueEngine = &engine.Engine{}

		if InitEngineFunc != nil {

			InitEngineFunc(uniqueEngine)
		}
	}
	return uniqueEngine
}

//Start start engine
func Start(engine *engine.Engine) {
	/*
		//read redis define from env
		redisConnectString := utility.GetEnv("CONNECT_STRING_REDIS")
		fmt.Println("redis:", redisConnectString)
		redisPool := adapter.RedisPool{}

		err := redisPool.Init(redisConnectString)

		if err != nil {

			panic("cannot init redis")
		}

		//read mongodb define
		mongoConnectString := utility.GetEnv("CONNECT_STRING_DOCUMENTDB")
		databaseName := utility.GetEnv("DOCUMENTDB_DATABASE")
		fmt.Println("mongo:", mongoConnectString)
		mongoPool := adapter.MongoPool{}

		err = mongoPool.InitWithDatabase(mongoConnectString, databaseName)

		if err != nil {

			panic("cannot init mongo")
		}

		//mongo file pool
		fileMongoPool := adapter.MongoFilePool{}

		fileMongoPool.Init("file", &mongoPool)

		engine.Init(&redisPool, &mongoPool, &fileMongoPool)
	*/
}
