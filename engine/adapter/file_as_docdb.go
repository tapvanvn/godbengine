package adapter

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/tapvanvn/godbengine/engine"
)

//FileDocDB simulate a folder as document db
//Just support limited function like get, put, delete
type FileDocDB struct {
	fileClient *FileClient
}

//Init init pool from connection string
func (db *FileDocDB) Init(connectionString string) error {
	client, err := NewFileClient(connectionString)
	if err != nil {
		return err
	}
	db.fileClient = client
	return nil
}

//Insert a document
func (db *FileDocDB) Put(collection string, document engine.Document) error {
	path := fmt.Sprintf("/%s/%s.json", collection, document.GetID())
	content, err := json.Marshal(document)
	if err != nil {
		return err
	}
	return db.fileClient.Write(path, &content)
}

func (db *FileDocDB) Get(collection string, id string, document interface{}) error {
	path := fmt.Sprintf("/%s/%s.json", collection, id)
	content, err := db.fileClient.Read(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(*content, document)
}

func (db *FileDocDB) Del(collection string, id string) error {
	path := fmt.Sprintf("/%s/%s.json", collection, id)
	return db.fileClient.Delete(path)
}

func (db *FileDocDB) IsNoRecordError(err error) bool {
	return os.IsNotExist(err)
}

//all query in transaction must be all done or all fail.
func (db *FileDocDB) MakeTransaction() engine.DBTransaction {
	log.Panic(engine.NotImplement)
	return nil
}

//Query query
func (db *FileDocDB) Query(query engine.DBQuery) engine.DBQueryResult {
	log.Panic(engine.NotImplement)
	return nil
}

func (db *FileDocDB) CleanPagingInfo(query engine.DBQuery) {
	log.Panic(engine.NotImplement)
}

//MARK: Work with collection
func (db *FileDocDB) DelCollection(collection string) error {
	path := fmt.Sprintf("/%s", collection)
	return db.fileClient.Delete(path)
}
