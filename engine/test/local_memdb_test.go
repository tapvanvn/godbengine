package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/tapvanvn/godbengine/engine/adapter"
)

func TestLocalMemPool(t *testing.T) {

	localMem := &adapter.LocalMemDB{}
	localMem.Init("")
	localMem.SetExpire("test", "value", time.Second) //exprite in 1 second
	time.Sleep(time.Second * 3)
	val, err := localMem.Get("test")
	if err != nil || val != "" {
		fmt.Println("val", val)
		t.Fail()
	}
}
