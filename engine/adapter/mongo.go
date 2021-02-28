package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/tapvanvn/godbengine/engine"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//MARK: Mongo Client

//MongoClient ...
type MongoClient struct {
	client      *mongo.Client
	collections map[string]*mongo.Collection
}

//MongoTransactionItem transaction item
type MongoTransactionItem struct {
	command    string
	collection string
	document   engine.Document
	id         string
}

//MongoTransaction apply DBTransaction
type MongoTransaction struct {
	database    string
	mongoClient *MongoClient
	items       []MongoTransactionItem
}

func (client *MongoClient) init(mongoClient *mongo.Client) {

	client.client = mongoClient
	client.collections = map[string]*mongo.Collection{}
}

//getCollection get cache collection
func (client *MongoClient) getCollection(databaseName string, collectionName string, cache bool) *mongo.Collection {

	if !cache {

		return client.client.Database(databaseName).Collection(collectionName)
	}
	if col, ok := client.collections[collectionName]; ok {

		return col
	}
	collection := client.client.Database(databaseName).Collection(collectionName)

	client.collections[collectionName] = collection

	return collection
}

//MARK: Mongo Query Result

//MongoQueryResult result of query
type MongoQueryResult struct {
	SelectOne    bool
	Err          error
	Cursor       *mongo.Cursor
	SingleResult *mongo.SingleResult
	Ctx          context.Context
	isAvailable  bool
	Total        int64
}

//Close close
func (result MongoQueryResult) Close() {

	if result.Cursor != nil {
		result.Cursor.Close(result.Ctx)
	}
	result.isAvailable = false
}

//IsAvailable check if isavailable
func (result MongoQueryResult) IsAvailable() bool {
	return result.isAvailable
}

//Error implement get Error
func (result MongoQueryResult) Error() error {
	return result.Err
}

//Next get next document
func (result MongoQueryResult) Next(document interface{}) error {

	if !result.SelectOne {

		if result.Cursor.Next(result.Ctx) {

			if err := result.Cursor.Decode(document); err != nil {

				return err
			}
			return nil
		}
		return errors.New("no more")
	}
	return errors.New("select on cursor while requested single query")
}

//Count count total document
func (result MongoQueryResult) Count() int64 {

	return result.Total
}

//GetOne get single result document
func (result MongoQueryResult) GetOne(document interface{}) error {

	if result.SelectOne {

		return result.SingleResult.Decode(document)
	}
	return errors.New("get single result while requested many document query")
}

// MARK: Mongo Pool

//MongoPool mongo pool implement DocumentPool
type MongoPool struct {
	database        string
	clients         []*MongoClient
	roundRobinCount int
}

//First get first client
func (pool *MongoPool) First() *MongoClient {

	return pool.clients[0]
}

//SelectRobin select a client by using round robin
func (pool *MongoPool) SelectRobin() *MongoClient {

	pool.roundRobinCount++
	pool.roundRobinCount = pool.roundRobinCount % len(pool.clients)
	return pool.clients[pool.roundRobinCount]
}

//Init init pool from connection string
func (pool *MongoPool) Init(connectionString string) error {

	clients := strings.Split(connectionString, ",")

	pool.database = "default"

	for _, client := range clients {

		clientOptions := options.Client().ApplyURI(client)

		mongoClient, err := mongo.Connect(context.TODO(), clientOptions)
		if err != nil {

			log.Fatal(err)
		}

		fmt.Println("mongo new client ", client)

		client := &MongoClient{}
		client.init(mongoClient)

		pool.clients = append(pool.clients, client)
	}
	fmt.Println("mongo pool ", len(pool.clients), " clients.")

	return nil
}

//InitWithDatabase init with database name
func (pool *MongoPool) InitWithDatabase(connectionString string, database string) error {
	err := pool.Init(connectionString)
	if err != nil {
		return err
	}
	pool.database = database
	return nil
}

//Get get document
func (pool *MongoPool) Get(collection string, id string, document interface{}) error {

	col := pool.First().getCollection(pool.database, collection, true)
	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()

	opts := options.FindOne().SetProjection(bson.M{"_id": 0})

	filter := bson.M{"__id": id}

	result := col.FindOne(ctx, filter, opts)

	if result.Err() != nil {

		return result.Err()
	}

	return result.Decode(document)
}

//Put document
func (pool *MongoPool) Put(collection string, document engine.Document) error {

	col := pool.First().getCollection(pool.database, collection, true)

	if col == nil {

		return errors.New("get collection fail")
	}
	ctx := context.TODO()

	opts := options.Update().SetUpsert(true)

	filter := bson.D{bson.E{Key: "__id", Value: document.GetID()}}

	update := bson.M{
		"$set": document,
	}

	_, err := col.UpdateOne(ctx, filter, update, opts)

	return err
}

//Del delete document
func (pool *MongoPool) Del(collection string, id string) error {

	col := pool.First().getCollection(pool.database, collection, true)
	if col == nil {

		return errors.New("get collection fail")
	}
	ctx := context.TODO()

	opts := &options.DeleteOptions{}

	filter := bson.M{"__id": id}

	_, err := col.DeleteOne(ctx, filter, opts)

	return err
}

//IsNoRecordError check if error is no record error
func (pool *MongoPool) IsNoRecordError(err error) bool {

	return err == mongo.ErrNoDocuments
}

//MakeTransaction create new transaction
func (pool *MongoPool) MakeTransaction() engine.DBTransaction {

	trans := MongoTransaction{mongoClient: pool.SelectRobin(),
		items:    make([]MongoTransactionItem, 0),
		database: pool.database}

	return &trans
}

//Query query document
func (pool *MongoPool) Query(query engine.DBQuery) engine.DBQueryResult {

	col := pool.First().getCollection(pool.database, query.Collection, true)

	ctx := context.TODO()
	queryResult := MongoQueryResult{Err: nil, Ctx: ctx}

	if col == nil {

		queryResult.Err = errors.New("get collection fail")

		return queryResult
	}

	filterA := bson.A{}

	currFilter := bson.M{}

	for _, filterItem := range query.Fields {

		if filterItem.Operator == "=" {

			currFilter[filterItem.Field] = filterItem.Value

		} else if filterItem.Operator == ">" {

			currFilter[filterItem.Field] = bson.M{

				"$gt": filterItem.Value,
			}
		} else if filterItem.Operator == "<" {

			currFilter[filterItem.Field] = bson.M{

				"$lt": filterItem.Value,
			}
		} else if filterItem.Operator == "+=" {

			if len(currFilter) > 0 {
				filterA = append(filterA, currFilter)
			}

			filterA = append(filterA, bson.M{

				"$or": bson.A{
					bson.M{filterItem.Field: bson.M{"$exists": false}},
					bson.M{filterItem.Field: filterItem.Value},
				},
			})

			currFilter = bson.M{}

		} else if filterItem.Operator == "+<" {

			if len(currFilter) > 0 {
				filterA = append(filterA, currFilter)
			}

			filterA = append(filterA, bson.M{

				"$or": bson.A{
					bson.M{filterItem.Field: bson.M{"$exists": false}},
					bson.M{filterItem.Field: bson.M{

						"$lt": filterItem.Value,
					}},
				},
			})

			currFilter = bson.M{}
		} else if filterItem.Operator == "+>" {

			if len(currFilter) > 0 {
				filterA = append(filterA, currFilter)
			}

			filterA = append(filterA, bson.M{

				"$or": bson.A{
					bson.M{filterItem.Field: bson.M{"$exists": false}},
					bson.M{filterItem.Field: bson.M{

						"$gt": filterItem.Value,
					}},
				},
			})

			currFilter = bson.M{}
		} else if filterItem.Operator == "regex" {

			pattern := fmt.Sprintf("%v", filterItem.Value)

			currFilter[filterItem.Field] = bson.M{

				"$regex": primitive.Regex{Pattern: pattern},
			}
		}
		if len(currFilter) > 0 {
			filterA = append(filterA, currFilter)
		}
	}
	filter := bson.M{}

	if len(filterA) > 0 {

		filter["$and"] = filterA
	}

	if query.SelectOne {

		opts := options.FindOne().SetProjection(bson.M{"_id": 0})

		queryResult.SelectOne = true
		queryResult.SingleResult = col.FindOne(ctx, filter, opts)
		queryResult.Err = queryResult.SingleResult.Err()
		queryResult.isAvailable = true

	} else {

		opts := options.Find().SetProjection(bson.M{"_id": 0})
		paging := query.GetPaging()
		if paging != nil && paging.PageSize > 0 {
			opts = opts.SetLimit(int64(paging.PageSize))
			opts = opts.SetSkip(int64(paging.PageNum * paging.PageSize))
		}
		total, err := col.CountDocuments(ctx, filter, options.Count())
		if err != nil {

			fmt.Println(err.Error())
		}
		result, err := col.Find(ctx, filter, opts)
		if err != nil {

			fmt.Println(err.Error())
		}
		//defer result.Close(ctx)

		queryResult.SelectOne = false
		queryResult.Err = err
		queryResult.Cursor = result
		queryResult.isAvailable = true
		queryResult.Total = total

	}

	return queryResult
}

//MARK: MongoTransaction

//Begin dbtransaction begin
func (transaction *MongoTransaction) Begin() {

}

//Put dbtransaction put
func (transaction *MongoTransaction) Put(collection string, document engine.Document) {

	transaction.items = append(transaction.items, MongoTransactionItem{command: "put", collection: collection, document: document})
}

//Del dbtransaction delete
func (transaction *MongoTransaction) Del(collection string, id string) {

	transaction.items = append(transaction.items, MongoTransactionItem{command: "del", collection: collection, id: id})
}

//Commit dbtransaction commit
func (transaction *MongoTransaction) Commit() error {

	ctx := context.Background()

	fmt.Println("dbtransaction commit")

	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		// Important: You must pass sessCtx as the Context parameter to the operations for them to be executed in the
		// transaction.
		for _, item := range transaction.items {

			col := transaction.mongoClient.getCollection(transaction.database, item.collection, false)

			if col == nil {

				return nil, errors.New("get collection fail")
			}

			if item.command == "put" {

				fmt.Print("transaction put", transaction.database, item.collection)

				opts := options.Update().SetUpsert(true)

				filter := bson.D{bson.E{Key: "__id", Value: item.document.GetID()}}

				update := bson.M{
					"$set": item.document,
				}

				_, err := col.UpdateOne(sessCtx, filter, update, opts)

				if err != nil {
					fmt.Println(" ", err.Error())
					return nil, err
				}
				fmt.Println(" success")
			} else if item.command == "del" {

				opts := &options.DeleteOptions{}

				filter := bson.M{"__id": item.id}

				_, err := col.DeleteOne(sessCtx, filter, opts)

				if err != nil {

					return nil, err
				}
			}
		}

		return nil, nil
	}

	session, err := transaction.mongoClient.client.StartSession()

	if err != nil {

		return err
	}
	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, callback)

	if err != nil {
		return err
	}

	fmt.Printf("result: %v\n", result)

	return nil
}
