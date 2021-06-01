package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/tapvanvn/godbengine/engine"
	_ "google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type FirestoreClient struct {
	client      *firestore.Client
	collections map[string]*firestore.CollectionRef
}

func (client *FirestoreClient) init(firestoreClient *firestore.Client) {

	client.client = firestoreClient
}

//getCollection get cache collection
func (client *FirestoreClient) getCollection(collectionName string) *firestore.CollectionRef {

	collection := client.client.Collection(collectionName)

	return collection
}

//FirestorePool pool implement DocumentPool
type FirestorePool struct {
	database        string
	clients         []*FirestoreClient
	roundRobinCount int
}

//First get first client
func (pool *FirestorePool) First() *FirestoreClient {

	return pool.clients[0]
}

//SelectRobin select a client by using round robin
func (pool *FirestorePool) SelectRobin() *FirestoreClient {

	pool.roundRobinCount++
	pool.roundRobinCount = pool.roundRobinCount % len(pool.clients)
	return pool.clients[pool.roundRobinCount]
}

//Init init pool from connection string
func (pool *FirestorePool) Init(connectionString string) error {

	clients := strings.Split(connectionString, ",")

	pool.database = "default"

	for _, client := range clients {

		pos := strings.Index(client, ":")
		if pos > -1 {
			projectID := client[:pos]
			credentialPath := client[pos+1:]
			firestoreClient, err := firestore.NewClient(context.TODO(), projectID, option.WithCredentialsFile(credentialPath))
			if err != nil {
				log.Fatalf("Failed to create client: %v", err)
			}

			engClient := &FirestoreClient{}
			engClient.init(firestoreClient)
			pool.clients = append(pool.clients, engClient)
		} else {

			projectID := client

			firestoreClient, err := firestore.NewClient(context.TODO(), projectID)
			if err != nil {
				log.Fatalf("Failed to create client: %v", err)
			}

			engClient := &FirestoreClient{}
			engClient.init(firestoreClient)
			pool.clients = append(pool.clients, engClient)
		}
	}
	fmt.Println("firestore pool ", len(pool.clients), " clients.")

	return nil
}

//MARK: FirestoreQueryResult

//FirestoreQueryResult result of query
type FirestoreQueryResult struct {
	SelectOne   bool
	Err         error
	Ctx         context.Context
	isAvailable bool
	Total       int64
	Iter        *firestore.DocumentIterator
}

//Close close
func (result FirestoreQueryResult) Close() {

	result.isAvailable = false
	if result.Iter != nil {
		result.Iter.Stop()
		result.Iter = nil
	}
}

//IsAvailable check if isavailable
func (result FirestoreQueryResult) IsAvailable() bool {
	return result.isAvailable
}

//Error implement get Error
func (result FirestoreQueryResult) Error() error {
	return result.Err
}

//Next get next document
func (result FirestoreQueryResult) Next(document interface{}) error {

	if !result.SelectOne {

		doc, err := result.Iter.Next()
		if err != nil {
			return err
		}

		err = doc.DataTo(document)
		if err != nil {

			return err
		}
		return nil
	}
	return errors.New("select on cursor while requested single query")
}

//Count count total document
func (result FirestoreQueryResult) Count() int64 {
	//TODO: check this
	return result.Total
}

//GetOne get single result document
func (result FirestoreQueryResult) GetOne(document interface{}) error {

	return errors.New("get single result while requested many document query")
}

//Get get document
func (pool *FirestorePool) Get(collection string, id string, document interface{}) error {

	col := pool.First().getCollection(collection)

	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()
	doc, err := col.Doc(id).Get(ctx)
	if err != nil {
		return err
	}
	doc.DataTo(document)
	return nil
}

//Put document
func (pool *FirestorePool) Put(collection string, document engine.Document) error {
	col := pool.First().getCollection(collection)
	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()
	_, err := col.Doc(document.GetID()).Set(ctx, document)
	if err != nil {
		return err
	}
	return nil
}

//Del delete document
func (pool *FirestorePool) Del(collection string, id string) error {

	col := pool.First().getCollection(collection)
	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()
	_, err := col.Doc(id).Delete(ctx)
	if err != nil {
		return err
	}
	return nil
}

//Query query document
func (pool *FirestorePool) Query(query engine.DBQuery) engine.DBQueryResult {

	col := pool.First().getCollection(query.Collection)

	ctx := context.TODO()
	queryResult := FirestoreQueryResult{Err: nil, Ctx: ctx}

	if col == nil {

		queryResult.Err = errors.New("get collection fail")

		return queryResult
	}

	var fsQuery firestore.Query = col.Query

	for _, filterItem := range query.Fields {

		if filterItem.Operator == "=" {

			fsQuery = fsQuery.Where(filterItem.Field, "==", filterItem.Value)

		} else if filterItem.Operator == "!=" ||
			filterItem.Operator == ">" ||
			filterItem.Operator == ">=" ||
			filterItem.Operator == "<" ||
			filterItem.Operator == "<=" ||
			filterItem.Operator == "in" {

			fsQuery = fsQuery.Where(filterItem.Field, filterItem.Operator, filterItem.Value)
		} else if filterItem.Operator == "+=" ||
			filterItem.Operator == "+<" ||
			filterItem.Operator == "+>" ||
			filterItem.Operator == "regex" {
			queryResult.isAvailable = false
			return queryResult
		}

		if query.SelectOne {
			queryResult.SelectOne = true
			queryResult.isAvailable = true
			for _, sort := range query.SortFields {

				if sort.Inscrease {
					fsQuery = fsQuery.OrderBy(sort.Field, firestore.Asc)
				} else {
					fsQuery = fsQuery.OrderBy(sort.Field, firestore.Asc)
				}
			}
			queryResult.Iter = fsQuery.Documents(queryResult.Ctx)
			queryResult.isAvailable = true

		} else {
			queryResult.SelectOne = false
			paging := query.GetPaging()
			if paging != nil && paging.PageSize > 0 {
				fsQuery = fsQuery.Limit(paging.PageSize).StartAt(paging.PageNum * paging.PageSize)
			}
			for _, sort := range query.SortFields {

				if sort.Inscrease {
					fsQuery = fsQuery.OrderBy(sort.Field, firestore.Asc)
				} else {
					fsQuery = fsQuery.OrderBy(sort.Field, firestore.Asc)
				}
			}

			queryResult.Iter = fsQuery.Documents(queryResult.Ctx)
			queryResult.isAvailable = true
			//TODO: apply error and total
		}
	}

	return queryResult
}

//IsNoRecordError check if error is no record error
func (pool *FirestorePool) IsNoRecordError(err error) bool {

	//return err == mongo.ErrNoDocuments
	return false
}

//MakeTransaction create new transaction
func (pool *FirestorePool) MakeTransaction() engine.DBTransaction {

	/*trans := MongoTransaction{mongoClient: pool.SelectRobin(),
		items:    make([]MongoTransactionItem, 0),
		database: pool.database}

	return &trans*/
	return nil
}
