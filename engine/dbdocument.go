package engine

import "errors"

var NoDocument = errors.New("no document")
var InvalidQuery = errors.New("Query is not valid")
var NotImplement = errors.New("Not implement")

//Document define a interface for document
type Document interface {
	GetID() string
}

//DBTransaction transaction
type DBTransaction interface {
	Begin()

	Put(collection string, document Document)

	Del(collection string, id string)

	Commit() error
}

//DocumentPool interface to interact with documentation database
type DocumentPool interface {

	//Init init pool from connection string
	Init(connectionString string) error

	//Insert a document
	Put(collection string, document Document) error

	Get(collection string, id string, document interface{}) error

	PutRaw(collection string, id string, document interface{}) error

	Del(collection string, id string) error

	IsNoRecordError(error) bool

	//all query in transaction must be all done or all fail.
	MakeTransaction() DBTransaction

	//Query query
	Query(query DBQuery) DBQueryResult

	CleanPagingInfo(query DBQuery)

	//MARK: Work with collection
	CreateCollection(collection string) error
	DelCollection(collection string) error

	//
	CollectVaryInt(collection string, field string) (map[string]int, error)
	CollectVaryString(collection string, field string) (map[string]int, error)
	CollectVaryQueryInt(query DBQuery, field string) (map[string]int, error)
	CollectVaryQueryString(query DBQuery, field string) (map[string]int, error)
}
