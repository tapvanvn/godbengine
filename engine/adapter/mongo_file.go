package adapter

//MongoFile wrap file
type MongoFile struct {
	Path string  `bson:"path"`
	Data *[]byte `bson:"data"`
}

//GetID
func (file MongoFile) GetID() string {

	return file.Path
}

//MongoFilePool mongo file pool
type MongoFilePool struct {
	mongoPool  *MongoPool
	collection string
}

//Init init
func (pool *MongoFilePool) Init(collection string, mongoPool *MongoPool) {
	pool.mongoPool = mongoPool
	pool.collection = collection
}
func (pool MongoFilePool) Read(path string) (*[]byte, error) {

	file := MongoFile{}

	if err := pool.mongoPool.Get(pool.collection, path, &file); err == nil {

		return file.Data, nil
	}
	return nil, nil
}

func (pool MongoFilePool) Write(path string, content *[]byte) error {

	file := MongoFile{Path: path, Data: content}

	return pool.mongoPool.Put(pool.collection, &file)
}
