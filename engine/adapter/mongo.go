package adapter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/tapvanvn/gocondition"
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

func (client *MongoClient) cleanCacheCollection(databaseName string, collectionName string) {

	delete(client.collections, collectionName)
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
		return engine.NoDocument
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

		err := result.SingleResult.Decode(document)
		if err == mongo.ErrNoDocuments {
			return engine.NoDocument
		} else {
			return err
		}
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
	var err error = nil

	for _, client := range clients {

		var numClient = 1
		hasNumClient := strings.Index(client, "[")

		if hasNumClient > 0 {

			end := strings.Index(client, "]")
			if end > hasNumClient {

				numString := client[hasNumClient+1 : end]
				if tryParse, err := strconv.ParseInt(numString, 10, 64); err == nil {
					numClient = int(tryParse)
				}
			}
			client = client[0:hasNumClient]
		}
		//detect params
		questionMark := strings.Index(client, "?")
		var tlsConfig *tls.Config = nil

		if questionMark > 0 {
			prefix := client[:questionMark]
			client = client[questionMark+1:]
			parts := strings.Split(client, "&")
			remains := []string{}
			hasSSL := false
			sslPath := ""

			//ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem
			for _, part := range parts {
				if strings.HasPrefix(part, "ssl") {
					if part == "ssl=true" {
						hasSSL = true
					} else if strings.HasPrefix(part, "ssl_ca_certs") {
						sslPath = part[13:]
					}
					continue
				}
				remains = append(remains, part)
			}
			if len(remains) > 0 {
				client = prefix + "?" + strings.Join(remains, "&")
			} else {
				client = prefix
			}
			if hasSSL {
				tlsConfig, err = getCustomTLSConfig(sslPath)
				if err != nil {

					log.Fatal(err)
				}
			}
		}

		for i := 0; i < numClient; i++ {

			clientOptions := options.Client().ApplyURI(client)
			if tlsConfig != nil {
				clientOptions.SetTLSConfig(tlsConfig)
			}
			mongoClient, err := mongo.Connect(context.TODO(), clientOptions)

			if err != nil {

				log.Fatal(err)
			}

			fmt.Println("mongo new client ", client)

			client := &MongoClient{}
			client.init(mongoClient)

			pool.clients = append(pool.clients, client)
		}
	}
	fmt.Println("mongo pool ", len(pool.clients), " clients.")

	return nil
}

func getCustomTLSConfig(caFile string) (*tls.Config, error) {
	fmt.Println("load TLS Config ", caFile)
	tlsConfig := new(tls.Config)
	certs, err := ioutil.ReadFile(caFile)

	if err != nil {
		return tlsConfig, err
	}

	tlsConfig.RootCAs = x509.NewCertPool()
	ok := tlsConfig.RootCAs.AppendCertsFromPEM(certs)

	if !ok {
		return tlsConfig, errors.New("Failed parsing pem file")
	}

	return tlsConfig, nil
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
	now := time.Now()
	col := pool.SelectRobin().getCollection(pool.database, collection, true)
	if col == nil {
		return errors.New("get collection fail")
	}
	ctx := context.TODO()

	opts := options.FindOne().SetProjection(bson.M{"_id": 0})

	filter := bson.M{"__id": id}

	result := col.FindOne(ctx, filter, opts)

	if result.Err() != nil {
		err := result.Err()
		if err == mongo.ErrNoDocuments {
			return engine.NoDocument
		} else {
			return err
		}
	}
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb get %s.%s %0.2fms\n", collection, id, float32(delta)/1_000_000)
	}
	return result.Decode(document)
}

//Put document
func (pool *MongoPool) Put(collection string, document engine.Document) error {

	return pool.PutRaw(collection, document.GetID(), document)
}

func (pool *MongoPool) PutRaw(collection string, id string, document interface{}) error {
	now := time.Now()
	col := pool.SelectRobin().getCollection(pool.database, collection, true)

	if col == nil {

		return errors.New("get collection fail")
	}
	ctx := context.TODO()

	opts := options.Update().SetUpsert(true)

	filter := bson.D{bson.E{Key: "__id", Value: id}}

	update := bson.M{
		"$set": document,
	}

	_, err := col.UpdateOne(ctx, filter, update, opts)
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb put %s.%s %0.2fms\n", collection, id, float32(delta)/1_000_000)
	}
	return err
}

//Del delete document
func (pool *MongoPool) Del(collection string, id string) error {
	now := time.Now()
	col := pool.SelectRobin().getCollection(pool.database, collection, true)

	if col == nil {

		return errors.New("get collection fail")
	}
	ctx := context.TODO()

	opts := &options.DeleteOptions{}

	filter := bson.M{"__id": id}

	_, err := col.DeleteOne(ctx, filter, opts)
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb del %s.%s %0.2fms\n", collection, id, float32(delta)/1_000_000)
	}
	return err
}

//IsNoRecordError check if error is no record error
func (pool *MongoPool) IsNoRecordError(err error) bool {

	return err == engine.NoDocument //mongo.ErrNoDocuments
}

//MakeTransaction create new transaction
func (pool *MongoPool) MakeTransaction() engine.DBTransaction {

	trans := MongoTransaction{mongoClient: pool.SelectRobin(),
		items:    make([]MongoTransactionItem, 0),
		database: pool.database}

	return &trans
}

func (pool *MongoPool) buildQueryFilter(filterItem *engine.DBFilterItem) bson.M {

	if filterItem.Operator == "=" {

		return bson.M{filterItem.Field: filterItem.FieldValue}

	} else if filterItem.Operator == "!=" {

		return bson.M{filterItem.Field: bson.M{

			"$ne": filterItem.FieldValue,
		}}

	} else if filterItem.Operator == ">" {

		return bson.M{filterItem.Field: bson.M{

			"$gt": filterItem.FieldValue,
		}}

	} else if filterItem.Operator == ">=" {

		return bson.M{filterItem.Field: bson.M{

			"$gte": filterItem.FieldValue,
		}}

	} else if filterItem.Operator == "<" {

		return bson.M{filterItem.Field: bson.M{

			"$lt": filterItem.FieldValue,
		}}
	} else if filterItem.Operator == "<=" {

		return bson.M{filterItem.Field: bson.M{

			"$lte": filterItem.FieldValue,
		}}

	} else if filterItem.Operator == "+=" {

		return bson.M{

			"$or": bson.A{
				bson.M{filterItem.Field: bson.M{"$exists": false}},
				bson.M{filterItem.Field: filterItem.FieldValue},
			},
		}

	} else if filterItem.Operator == "+<" {

		return bson.M{

			"$or": bson.A{
				bson.M{filterItem.Field: bson.M{"$exists": false}},
				bson.M{filterItem.Field: bson.M{

					"$lt": filterItem.FieldValue,
				}},
			},
		}

	} else if filterItem.Operator == "+>" {

		return bson.M{

			"$or": bson.A{
				bson.M{filterItem.Field: bson.M{"$exists": false}},
				bson.M{filterItem.Field: bson.M{

					"$gt": filterItem.FieldValue,
				}},
			},
		}

	} else if filterItem.Operator == "regex" {

		pattern := fmt.Sprintf("%v", filterItem.FieldValue)

		return bson.M{

			"$regex": primitive.Regex{Pattern: pattern},
		}

	} else if filterItem.Operator == "in" {

		return bson.M{

			filterItem.Field: bson.M{

				"$in": filterItem.FieldValue,
			},
		}
	}
	return bson.M{}
}

func (pool *MongoPool) buildQueryAnd(ruleSet *gocondition.RuleSet) bson.M {

	filter := bson.M{}

	if len(ruleSet.Children) == 0 {

		return filter
	}

	filterA := bson.A{}

	for _, filterItem := range ruleSet.Children {

		switch filterItem.(type) {
		case *engine.DBFilterItem:
			filterA = append(filterA, pool.buildQueryFilter(filterItem.(*engine.DBFilterItem)))
			break
		case *gocondition.RuleSet:
			ruleSet := filterItem.(*gocondition.RuleSet)
			if ruleSet.IsAnd() {
				filterA = append(filterA, pool.buildQueryAnd(ruleSet))
			} else {
				filterA = append(filterA, pool.buildQueryOr(ruleSet))
			}
			break
		default:
			panic(engine.InvalidQuery)
		}
	}

	if len(filterA) > 1 {

		filter["$and"] = filterA

	} else {

		return filterA[0].(bson.M)
	}
	return filter
}

func (pool *MongoPool) buildQueryOr(ruleSet *gocondition.RuleSet) bson.M {

	filter := bson.M{}

	if len(ruleSet.Children) == 0 {
		return filter
	}
	filterA := bson.A{}

	for _, filterItem := range ruleSet.Children {

		switch filterItem.(type) {
		case *engine.DBFilterItem:
			filterA = append(filterA, pool.buildQueryFilter(filterItem.(*engine.DBFilterItem)))
			break
		case *gocondition.RuleSet:
			ruleSet := filterItem.(*gocondition.RuleSet)
			if ruleSet.IsAnd() {
				filterA = append(filterA, pool.buildQueryAnd(ruleSet))
			} else {
				filterA = append(filterA, pool.buildQueryOr(ruleSet))
			}
			break
		default:
			panic(engine.InvalidQuery)
		}
	}

	if len(filterA) > 1 {

		filter["$or"] = filterA

	} else {

		return filterA[0].(bson.M)
	}
	return filter
}

//Query query document
func (pool *MongoPool) Query(query engine.DBQuery) engine.DBQueryResult {

	now := time.Now()
	col := pool.SelectRobin().getCollection(pool.database, query.Collection, true)

	ctx := context.TODO()
	queryResult := MongoQueryResult{Err: nil, Ctx: ctx}

	if col == nil {

		queryResult.Err = errors.New("get collection fail")

		return queryResult
	}

	filter := pool.buildQueryAnd(query.Condition)

	if query.SelectOne {

		opts := options.FindOne().SetProjection(bson.M{"_id": 0})
		for _, sort := range query.SortFields {

			if sort.Inscrease {

				opts = opts.SetSort(bson.M{sort.Field: 1})
			} else {
				opts = opts.SetSort(bson.M{sort.Field: -1})
			}
		}
		queryResult.SelectOne = true
		queryResult.SingleResult = col.FindOne(ctx, filter, opts)
		err := queryResult.SingleResult.Err()
		if err == mongo.ErrNoDocuments {
			queryResult.Err = engine.NoDocument
		} else {
			queryResult.Err = err
		}

		queryResult.isAvailable = true

	} else {

		opts := options.Find().SetProjection(bson.M{"_id": 0})
		paging := query.GetPaging()
		if paging != nil && paging.PageSize > 0 {
			opts = opts.SetLimit(int64(paging.PageSize))
			opts = opts.SetSkip(int64(paging.PageNum * paging.PageSize))
		}
		for _, sort := range query.SortFields {

			if sort.Inscrease {

				opts = opts.SetSort(bson.M{sort.Field: 1})
			} else {
				opts = opts.SetSort(bson.M{sort.Field: -1})
			}
		}
		total, err := col.CountDocuments(ctx, filter, options.Count())
		if err != nil {

			fmt.Println(err.Error())
		}
		result, err := col.Find(ctx, filter, opts)

		queryResult.SelectOne = false

		if err == mongo.ErrNoDocuments {
			queryResult.Err = engine.NoDocument
		} else {
			queryResult.Err = err
		}

		queryResult.Cursor = result
		queryResult.isAvailable = true
		queryResult.Total = total
	}
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb query %s %0.2fms\n", query.Collection, float32(delta)/1_000_000)
	}
	return queryResult
}

//We dont need to implement anything on mongodb
func (pool *MongoPool) CleanPagingInfo(query engine.DBQuery) {

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
	now := time.Now()
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
			} else if item.command == "del_collection" {

				err := col.Drop(sessCtx)
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
	if __measurement {
		delta := time.Now().Sub(now).Nanoseconds()
		fmt.Printf("mersure docdb transcommit %0.2fms\n", float32(delta)/1_000_000)
	}
	return nil
}

//MARK: Work with collection

func (pool *MongoPool) DelCollection(collection string) error {
	ctx := context.Background()
	col := pool.First().getCollection(pool.database, collection, true)
	err := col.Drop(ctx)
	if err == nil {
		for _, client := range pool.clients {
			client.cleanCacheCollection(pool.database, collection)
		}
	}
	return err
}

func (pool *MongoPool) CreateCollection(collection string) error {
	ctx := context.Background()
	return pool.First().client.Database(pool.database).CreateCollection(ctx, collection)
}
