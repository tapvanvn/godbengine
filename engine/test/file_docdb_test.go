package test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	adapter "github.com/tapvanvn/godbengine/engine/adapter"
)

var __file_docdb *adapter.FileDocDB = nil

type testStruct struct {
	ID     int64 `json:"ID" bson:"ID"`
	Number int64 `json:"Number" bson:"Number"`
}

func (document *testStruct) GetID() string {

	return strconv.FormatInt(document.ID, 10)
}

func initFileDB() error {

	rootPath, _ := os.Getwd()

	fmt.Println(rootPath)
	pool := &adapter.FileDocDB{}
	err := pool.Init(rootPath)

	if err != nil {
		return err
	}
	__file_docdb = pool
	return nil
}

func TestFileDocDB(t *testing.T) {
	err := initFileDB()
	if err != nil {
		t.Error(err)
		return
	}
	doc := &testStruct{
		ID:     10,
		Number: 10,
	}
	err = __file_docdb.Put("test_collection", doc)
	if err != nil {
		t.Error(err)
		return
	}
	err = __file_docdb.Get("test_collection", "10", doc)
	if err != nil {
		t.Error(err)
		return
	}
}
