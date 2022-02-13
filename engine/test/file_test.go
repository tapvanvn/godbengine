package test

import (
	"fmt"
	"os"
	"testing"

	adapter "github.com/tapvanvn/godbengine/engine/adapter"
)

var __file_client *adapter.FileClient = nil
var __content = []byte("test")

func engineInit() error {

	rootPath, _ := os.Getwd()

	fmt.Println(rootPath)

	pool, err := adapter.NewFileClient(rootPath)
	if err != nil {
		return err
	}
	__file_client = pool
	return nil
}

func TestFile(t *testing.T) {
	err := engineInit()
	if err != nil {
		t.Error(err)
		return
	}

	err = __file_client.Write("/directory/file.txt", &__content)
	if err != nil {
		t.Error(err)
		return
	}
	content, err := __file_client.Read("/directory/file.txt")
	if err != nil {
		t.Error(err)
		return
	}
	str := string(*content)
	if str != string(__content) {
		t.Error()
	}
	err = __file_client.Delete("/directory/file.txt")
	if err != nil {
		t.Error(err)
		return
	}
}
