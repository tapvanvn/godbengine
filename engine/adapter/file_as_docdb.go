package adapter

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

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

	return db.PutRaw(collection, document.GetID(), document)
}

func (db *FileDocDB) PutRaw(collection string, id string, document interface{}) error {

	path := fmt.Sprintf("/%s/%s.json", collection, id)

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

	return &FileDocTransaction{
		db:    db,
		items: make([]FileDocDBTransactionItem, 0),
	}
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

func (db *FileDocDB) CreateCollection(collection string) error {
	return engine.NotImplement
}

type FileDocDBTransactionItem struct {
	command    string
	collection string
	document   interface{}
	id         string
}

//MongoTransaction apply DBTransaction
type FileDocTransaction struct {
	items []FileDocDBTransactionItem
	db    *FileDocDB
}

//MARK: MongoTransaction

//Begin dbtransaction begin
func (transaction *FileDocTransaction) Begin() {

}

//Put dbtransaction put
func (transaction *FileDocTransaction) Put(collection string, document engine.Document) {

	transaction.items = append(transaction.items, FileDocDBTransactionItem{command: "put", collection: collection, id: document.GetID(), document: document})
}
func (transaction *FileDocTransaction) PutRaw(collection string, id string, document interface{}) {

	transaction.items = append(transaction.items, FileDocDBTransactionItem{command: "put", collection: collection, id: id, document: document})
}

//Del dbtransaction delete
func (transaction *FileDocTransaction) Del(collection string, id string) {

	transaction.items = append(transaction.items, FileDocDBTransactionItem{command: "del", collection: collection, id: id})
}

//Commit dbtransaction commit
func (transaction *FileDocTransaction) Commit() error {

	now := time.Now()

	fmt.Println("dbtransaction commit")

	for _, item := range transaction.items {

		if item.command == "put" {

			transaction.db.PutRaw(item.collection, item.id, item.document)

		} else if item.command == "del" {

			transaction.db.Del(item.collection, item.id)
		}
	}

	if __measurement {

		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb transcommit %0.2fms\n", float32(delta)/1_000_000)
	}
	return nil
}

//MARK: external function
func (db *FileDocDB) GetCollectionPath(collectionName string) string {

	path := fmt.Sprintf("%s/%s", db.fileClient.absolutePath, collectionName)

	return path
}

func (db *FileDocDB) GetAllDocumentIDs(collectionName string) ([]string, error) {

	path := db.GetCollectionPath(collectionName)

	files, err := os.ReadDir(path)

	if err != nil {

		return nil, err
	}
	ids := []string{}

	for _, file := range files {

		if file.IsDir() {

			continue
		}
		fname := file.Name()

		rpos := strings.LastIndex(fname, ".")
		if rpos <= 0 || fname[rpos+1:] != "json" {
			continue
		}

		ids = append(ids, fname[:rpos])
	}
	return ids, nil
}

func (pool *FileDocDB) CollectVaryInt(collection string, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
func (pool *FileDocDB) CollectVaryString(collection string, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
func (pool *FileDocDB) CollectVaryQueryInt(query engine.DBQuery, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
func (pool *FileDocDB) CollectVaryQueryString(query engine.DBQuery, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
