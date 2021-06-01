package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	engines "github.com/tapvanvn/godbengine"
	"github.com/tapvanvn/godbengine/engine"
	adapter "github.com/tapvanvn/godbengine/engine/adapter"
)

type TestFSDocument struct {
	UUID   uuid.UUID `json:"id"`
	Number int       `json:"number" bson:"number"`
}

//GetID implement Document
func (document TestFSDocument) GetID() string {

	return document.UUID.String()
}

func EngineInit(engine *engine.Engine) {

	rootPath, _ := os.Getwd()

	fmt.Println(rootPath)

	projectID := "mydefipet"

	//read mongodb define
	connectString := projectID + ":" + rootPath + "/credential.json"

	pool := adapter.FirestorePool{}

	err := pool.Init(connectString)

	if err != nil {

		panic("cannot init pool")
	}

	//mongo file pool
	engine.Init(nil, &pool, nil)
}
func TestFirestorePool(t *testing.T) {

	engines.InitEngineFunc = EngineInit

	engine := engines.GetEngine()

	t.Log("set test document")

	doc := &TestFSDocument{UUID: uuid.MustParse("36ecce31-266a-489d-a3b0-96df0a7a5cfc"),
		Number: 2}

	err := engine.GetDocumentPool().Put("test", doc)

	if err != nil {

		t.Fail()

		t.Error(err)

		return
	}
	t.Log("get test document", doc.GetID())

	doc2 := TestFSDocument{}

	err = engine.GetDocumentPool().Get("test", doc.GetID(), &doc2)

	if err != nil || doc2.Number != 2 {

		if err == nil {
			t.Log(doc2)
		} else {
			t.Log(err)
		}
		t.Fail()
		return
	}
	fmt.Printf("%v", doc2)

	t.Log("del test document")
	err = engine.GetDocumentPool().Del("test", doc.GetID())

	if err != nil {

		t.Fail()
		return
	}
}

func TestFirestoreQuery(t *testing.T) {
	engines.InitEngineFunc = EngineInit

	eng := engines.GetEngine()

	/*t.Log("set test document")

	for i := 0; i < 100; i++ {

		fmt.Println("put doc:", i)

		doc := TestFSDocument{UUID: uuid.New(),
			Number: i}

		err := eng.GetDocumentPool().Put("test", doc)

		if err != nil {

			t.Fail()

			t.Error(err)

			return
		}
	}*/

	query := engine.MakeDBQuery("test", false)
	query.Filter("Number", "=", 2)
	rs := eng.GetDocumentPool().Query(query)

	defer rs.Close()

	doc2 := &TestFSDocument{}
	err := rs.Next(doc2)
	for {
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println(doc2)
		err = rs.Next(doc2)
	}
}
