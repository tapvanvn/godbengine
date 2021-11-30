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

type FileDocDBTransactionItem struct {
	command    string
	collection string
	document   engine.Document
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

	transaction.items = append(transaction.items, FileDocDBTransactionItem{command: "put", collection: collection, document: document})
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

			transaction.db.Put(item.collection, item.document)

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

	path := fmt.Sprintf("/%s", collectionName)

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
		part := strings.Split(file.Name(), ".")

		if len(part) != 2 || part[1] != "json" {

			continue
		}
		ids = append(ids, part[0])
	}
	return ids, nil
}
