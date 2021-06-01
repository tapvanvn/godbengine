package test

import (
	"testing"

	engines "github.com/tapvanvn/godbengine"
)

func TestMemPool(t *testing.T) {

	engine := engines.GetEngine()

	t.Log("set value")
	err := engine.GetMemPool().Set("test_key", "this is a test")

	if err != nil {

		t.Fail()
		return
	}
	t.Log("get value")
	value, err := engine.GetMemPool().Get("test_key")

	if err != nil || value != "this is a test" {

		t.Fail()
		return
	}
	t.Log("del value")
	err = engine.GetMemPool().Del("test_key")

	if err != nil {

		t.Fail()
		return
	}
}
