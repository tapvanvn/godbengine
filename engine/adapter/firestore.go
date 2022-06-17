package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/tapvanvn/godbengine/engine"
	"google.golang.org/api/iterator"
	_ "google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

//MARK: PagingHelper
type FirestorePagingItem struct {
	pageSize       int
	pageEndID      map[int]*firestore.DocumentSnapshot
	persistentPage int
}

func (pagingItem *FirestorePagingItem) adaptPaging(query engine.DBQuery, pool *FirestorePool, fsQuery firestore.Query) (firestore.Query, bool) {

	paging := query.GetPaging()

	if paging.PageNum == 0 {

		return fsQuery.Limit(pagingItem.pageSize), true
	}
	if paging.PageNum <= pagingItem.persistentPage {

		return fsQuery.Limit(pagingItem.pageSize).StartAfter(pagingItem.pageEndID[paging.PageNum]), true
	}

	fetchQuery, isComplete := pool.fetchQueryWithOutPaging(query)

	if !isComplete {
		return fsQuery, false
	}

	ctx := context.TODO()

	hasOrder := len(query.SortFields) > 0

	if pagingItem.persistentPage == 0 {

		if !hasOrder {

			docs, err := fetchQuery.OrderBy(firestore.DocumentID, firestore.Asc).Limit(pagingItem.pageSize).Documents(ctx).GetAll()
			if err != nil {
				return fsQuery, false
			}
			numDoc := len(docs)
			pagingItem.pageEndID[0] = docs[numDoc-1]
		} else {

			docs, err := fetchQuery.Limit(pagingItem.pageSize).Documents(ctx).GetAll()
			if err != nil {
				return fsQuery, false
			}
			numDoc := len(docs)
			pagingItem.pageEndID[0] = docs[numDoc-1]
		}
	}

	for i := pagingItem.persistentPage + 1; i < paging.PageNum; i++ {

		if !hasOrder {

			docs, err := fetchQuery.OrderBy(firestore.DocumentID, firestore.Asc).Limit(pagingItem.pageSize).StartAfter(pagingItem.pageEndID[i-1]).Documents(ctx).GetAll()
			if err != nil {

				return fsQuery, false
			}
			numDoc := len(docs)

			pagingItem.pageEndID[i] = docs[numDoc-1]

			if numDoc < pagingItem.pageSize {

				return fsQuery, false
			}
		} else {

			docs, err := fetchQuery.Limit(pagingItem.pageSize).StartAfter(pagingItem.pageEndID[i-1]).Documents(ctx).GetAll()
			if err != nil {
				return fsQuery, false
			}
			numDoc := len(docs)

			pagingItem.pageEndID[i] = docs[numDoc-1]

			if numDoc < pagingItem.pageSize {

				return fsQuery, false
			}
		}
		pagingItem.persistentPage = i
	}
	if !hasOrder {
		return fsQuery.OrderBy(firestore.DocumentID, firestore.Asc).Limit(pagingItem.pageSize).StartAfter(pagingItem.pageEndID[paging.PageNum-1]), true
	} else {
		return fsQuery.Limit(pagingItem.pageSize).StartAfter(pagingItem.pageEndID[paging.PageNum-1]), true
	}
}

type FirestorePagingHelper map[int]*FirestorePagingItem

var __fspaging_helper = map[string]FirestorePagingHelper{}

func getPagingHelper(query engine.DBQuery) *FirestorePagingItem {

	paging := query.GetPaging()

	if paging != nil && paging.PageSize > 0 && query.SelectOne == false {

		signature := query.GetSignature()

		if helper, ok := __fspaging_helper[signature]; ok {

			if _, ok := helper[paging.PageSize]; !ok {

				helper[paging.PageSize] = &FirestorePagingItem{
					pageSize:  paging.PageSize,
					pageEndID: map[int]*firestore.DocumentSnapshot{},
				}
			}
			return helper[paging.PageSize]

		} else {

			item := &FirestorePagingItem{

				pageSize:  paging.PageSize,
				pageEndID: map[int]*firestore.DocumentSnapshot{},
			}
			__fspaging_helper[signature] = FirestorePagingHelper{}

			__fspaging_helper[signature][paging.PageSize] = item

			return item
		}
	}
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

	if !result.isAvailable {

		return engine.InvalidQuery
	}
	if !result.SelectOne {

		doc, err := result.Iter.Next()

		if err != nil {
			if err == iterator.Done {
				return engine.NoDocument
			} else {
				return err
			}
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

	if result.SelectOne {

		docs, err := result.Iter.GetAll()
		if err != nil {
			return err
		}
		if len(docs) != 1 {
			return engine.NoDocument
		}
		return docs[0].DataTo(document)
	}
	return errors.New("get single result while requested many document query")
}

//MARK: Transaction
type FirestoreTransactionItem struct {
	command    string
	collection string
	document   interface{}
	id         string
}

//Begin dbtransaction begin
func (transaction *FirestoreTransaction) Begin() {

}

//Put dbtransaction put
func (transaction *FirestoreTransaction) Put(collection string, document engine.Document) {

	transaction.items = append(transaction.items, FirestoreTransactionItem{command: "put", collection: collection, document: document, id: document.GetID()})
}
func (transaction *FirestoreTransaction) PutRaw(collection string, id string, document interface{}) {

	transaction.items = append(transaction.items, FirestoreTransactionItem{command: "put", collection: collection, document: document, id: id})
}

//Del dbtransaction delete
func (transaction *FirestoreTransaction) Del(collection string, id string) {

	transaction.items = append(transaction.items, FirestoreTransactionItem{command: "del", collection: collection, id: id})
}

//Commit dbtransaction commit
func (transaction *FirestoreTransaction) Commit() error {
	now := time.Now()
	batch := transaction.client.client.Batch()

	ctx := context.Background()

	for _, item := range transaction.items {

		col := transaction.client.getCollection(item.collection)

		if col == nil {

			return errors.New("get collection fail")
		}

		if item.command == "put" {

			batch.Set(col.Doc(item.id), item.document)

		} else if item.command == "del" {

			batch.Delete(col.Doc(item.id))

		} else if item.command == "del_collection" {

			return engine.NotImplement
		}
	}
	_, err := batch.Commit(ctx)
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb transcommit %0.2fms\n", float32(delta)/1_000_000)
	}
	return err
}

//MARK: Pool

//FirestoreTransaction apply DBTransaction
type FirestoreTransaction struct {
	database string
	client   *FirestoreClient
	items    []FirestoreTransactionItem
}

//Get get document
func (pool *FirestorePool) Get(collection string, id string, document interface{}) error {

	now := time.Now()
	col := pool.First().getCollection(collection)

	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()
	doc, err := col.Doc(id).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return engine.NoDocument
		} else {
			return err
		}
	}
	doc.DataTo(document)
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb get %s.%s %0.2fms\n", collection, id, float32(delta)/1_000_000)
	}
	return nil
}

//Put document
func (pool *FirestorePool) Put(collection string, document engine.Document) error {

	return pool.PutRaw(collection, document.GetID(), document)
}

func (pool *FirestorePool) PutRaw(collection string, id string, document interface{}) error {
	now := time.Now()
	col := pool.First().getCollection(collection)
	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()
	_, err := col.Doc(id).Set(ctx, document)
	if err != nil {
		return err
	}
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb put %s.%s %0.2fms\n", collection, id, float32(delta)/1_000_000)
	}
	return nil
}

//Del delete document
func (pool *FirestorePool) Del(collection string, id string) error {
	now := time.Now()
	col := pool.First().getCollection(collection)

	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()
	_, err := col.Doc(id).Delete(ctx)
	if err != nil {
		return err
	}
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb del %s.%s %0.2fms\n", collection, id, float32(delta)/1_000_000)
	}
	return nil
}

type FirestoreQueryItem struct {
	isComplete bool
	fsquery    firestore.Query
}

var __fsquery = map[string]*FirestoreQueryItem{}

func (pool *FirestorePool) fetchQueryWithOutPaging(query engine.DBQuery) (firestore.Query, bool) {

	col := pool.First().getCollection(query.Collection)

	var fsQuery firestore.Query = col.Query
	/*
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

				return fsQuery, false
			}
		}

		for _, sort := range query.SortFields {

			if sort.Inscrease {

				fsQuery = fsQuery.OrderBy(sort.Field, firestore.Asc)

			} else {

				fsQuery = fsQuery.OrderBy(sort.Field, firestore.Desc)
			}
		}

		return fsQuery, true
	*/
	return fsQuery, true
}

//Query query document
func (pool *FirestorePool) Query(query engine.DBQuery) engine.DBQueryResult {
	//now := time.Now()
	//col := pool.First().getCollection(query.Collection)

	ctx := context.TODO()
	queryResult := &FirestoreQueryResult{Err: engine.NotImplement, Ctx: ctx}
	/*
		if col == nil {

			queryResult.Err = errors.New("get collection fail")

			return queryResult
		}

		fsQuery, complete := pool.fetchQueryWithOutPaging(query)
		if !complete {
			queryResult.isAvailable = false
			return queryResult
		}

		if query.SelectOne {

			queryResult.SelectOne = true
			queryResult.isAvailable = true
			fsQuery = fsQuery.Limit(1)
			queryResult.Iter = fsQuery.Documents(queryResult.Ctx)

		} else {

			queryResult.SelectOne = false
			paging := query.GetPaging()
			if paging != nil && paging.PageSize > 0 {

				//Process Paging
				helperItem := getPagingHelper(query)

				adatpPagingQuery, hasPage := helperItem.adaptPaging(query, pool, fsQuery)

				if hasPage {

					fsQuery = adatpPagingQuery
					queryResult.isAvailable = true
					queryResult.Iter = fsQuery.Documents(queryResult.Ctx)

				} else {
					fmt.Println("no page", paging.PageNum)
					queryResult.isAvailable = false
				}
			} else {
				queryResult.Iter = fsQuery.Documents(queryResult.Ctx)
				queryResult.isAvailable = true
			}

			//TODO: apply error and total
		}
		if __measurement {
			delta := time.Now().Sub(now).Nanoseconds()
			fmt.Printf("mersure docdb query %s %0.2fms\n", query.Collection, float32(delta)/1_000_000)
		}*/
	return queryResult
}

func (pool *FirestorePool) CleanPagingInfo(query engine.DBQuery) {
	if query.GetPaging() != nil {
		helperItem := getPagingHelper(query)
		helperItem.persistentPage = 0
	}
}

//IsNoRecordError check if error is no record error
func (pool *FirestorePool) IsNoRecordError(err error) bool {

	return err == engine.NoDocument
}

//MakeTransaction create new transaction
func (pool *FirestorePool) MakeTransaction() engine.DBTransaction {

	trans := FirestoreTransaction{client: pool.SelectRobin(),
		items: make([]FirestoreTransactionItem, 0)}

	return &trans
}

//TODO: Work on paging helper refresh cache system.

//MARK: Woking with collection
func (pool *FirestorePool) DelCollection(collection string) error {

	return engine.NotImplement
}
func (pool *FirestorePool) CreateCollection(collection string) error {

	//Firestore auto create collection if is's not existed
	return nil
}

func (pool *FirestorePool) CollectVaryInt(collection string, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
func (pool *FirestorePool) CollectVaryString(collection string, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
func (pool *FirestorePool) CollectVaryQueryInt(query engine.DBQuery, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
func (pool *FirestorePool) CollectVaryQueryString(query engine.DBQuery, field string) (map[string]int, error) {
	return nil, engine.NotImplement
}
