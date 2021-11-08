package test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	engines "github.com/tapvanvn/godbengine"
)

type TestDocument struct {
	UUID   uuid.UUID `json:"_id"`
	Number int       `json:"number" bson:"number"`
}

//GetID implement Document
func (document TestDocument) GetID() string {

	return document.UUID.String()
}

func TestDocumentPool(t *testing.T) {

	engine := engines.GetEngine()

	t.Log("set test document")

	doc := TestDocument{UUID: uuid.MustParse("36ecce31-266a-489d-a3b0-96df0a7a5cfc"),
		Number: 2}

	err := engine.GetDocumentPool().Put("test", doc)

	if err != nil {

		t.Fail()

		t.Error(err)

		return
	}
	t.Log("get test document", doc.GetID())

	doc2 := TestDocument{}

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

	t.Log("del test document")
	err = engine.GetDocumentPool().Del("test", doc.GetID())

	if err != nil {

		t.Fail()
		return
	}
}

func TestParse(t *testing.T) {
	client := "abcd[5]"
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
	if numClient != 5 {
		t.Error(numClient, client)
	}
}
