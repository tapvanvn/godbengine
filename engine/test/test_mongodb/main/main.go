package main

import (
	"fmt"
	"log"

	engines "github.com/tapvanvn/godbengine"
	"github.com/tapvanvn/godbengine/engine"
	"github.com/tapvanvn/godbengine/engine/adapter"
)

func startEngine(eng *engine.Engine) {

	connectString := ""
	databaseName := ""
	var documentDB engine.DocumentPool = nil

	mongoPool := &adapter.MongoPool{}
	err := mongoPool.InitWithDatabase(connectString, databaseName)

	if err != nil {

		log.Fatal("cannot init mongo")
	}
	documentDB = mongoPool

	eng.Init(nil, documentDB, nil)
}
func main() {
	engines.InitEngineFunc = startEngine
	eng := engines.GetEngine()
	documentPool := eng.GetDocumentPool()
	query := engine.MakeDBQuery("test", true)

	query.Filter("ItemType", "in", []string{"car", "airplane"})

	queryResult := documentPool.Query(query)

	if queryResult.Error() != nil {

		panic(queryResult.Error())
	}

	defer queryResult.Close()
	/*
		documents := []*map[string]interface{}{}

		for {
			document := &map[string]interface{}{}

			err := queryResult.Next(document)

			if err != nil {
				fmt.Println("load fail" + (err.Error()))
				break
			}
			documents = append(documents, document)
		}
	*/
	fmt.Println("success")
}
