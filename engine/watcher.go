package engine

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type Watcher struct {
	mux       sync.Mutex
	docMux    sync.Mutex
	timeRange time.Duration
	isRun     bool
	documents map[string]Document
	tick      map[string]int64
	dirty     map[string]bool
	pool      DocumentPool
}

func NewWatcher(timeRange time.Duration, pool DocumentPool) *Watcher {

	return &Watcher{
		timeRange: timeRange,
		isRun:     false,
		documents: map[string]Document{},
		tick:      map[string]int64{},
		dirty:     map[string]bool{},
		pool:      pool,
	}
}

func (watcher *Watcher) run() {

	deadline := time.Now().Unix() - int64(watcher.timeRange.Seconds())

	watcher.mux.Lock()
	defer watcher.mux.Unlock()
	transaction := watcher.pool.MakeTransaction()

	transaction.Begin()
	count := 0
	for key, dirty := range watcher.dirty {
		if dirty {
			tick, _ := watcher.tick[key]
			if tick < deadline {
				watcher.docMux.Lock()
				doc, ok := watcher.documents[key]
				watcher.docMux.Unlock()
				if ok {
					parts := strings.Split(key, "$")
					transaction.Put(parts[0], doc)

					watcher.dirty[key] = false

					count++
					if count == 150 {

						err := transaction.Commit()
						if err != nil {
							//TODO: report error
							fmt.Println("Watcher", err)
						}
						transaction = watcher.pool.MakeTransaction()
						transaction.Begin()
					}
				}
			}
		}
	}
	if count > 0 {
		err := transaction.Commit()
		if err != nil {

			fmt.Println("Watcher", err)

			//TODO: report error
		}
	}
}

func (watcher *Watcher) Update(collection string, docID string) {

	mapID := collection + "$" + docID
	watcher.mux.Lock()
	watcher.tick[mapID] = time.Now().Unix()
	watcher.dirty[mapID] = true
	watcher.mux.Unlock()
}

func (watcher *Watcher) UpdateForce(collection string, docID string) {

	mapID := collection + "$" + docID
	watcher.docMux.Lock()
	if doc, ok := watcher.documents[mapID]; ok {
		if err := watcher.pool.Put(collection, doc); err != nil {
			fmt.Println("Watcher", collection, docID, err)
		}
	}
	watcher.docMux.Unlock()
}

//Load carefull when using this function, each mapid map only to one doc at a time. Reload a document will disrupt other connection
func (watcher *Watcher) Watch(collection string, doc Document) error {

	mapID := collection + "$" + doc.GetID()
	watcher.docMux.Lock()
	watcher.documents[mapID] = doc
	watcher.docMux.Unlock()

	watcher.mux.Lock()

	watcher.tick[mapID] = time.Now().Unix()
	watcher.dirty[mapID] = false
	watcher.mux.Unlock()
	return nil
}
func (watcher *Watcher) WatchPut(collection string, doc Document) error {

	err := watcher.Watch(collection, doc)

	if err != nil {
		return err
	}
	watcher.UpdateForce(collection, doc.GetID())

	return nil
}

func (watcher *Watcher) StopWatch(collection string, doc Document) {
	mapID := collection + "$" + doc.GetID()
	watcher.docMux.Lock()
	delete(watcher.documents, mapID)
	watcher.docMux.Unlock()

	watcher.mux.Lock()
	delete(watcher.dirty, mapID)
	delete(watcher.tick, mapID)
	watcher.mux.Unlock()
}
func (watcher *Watcher) Run() {

	if watcher.isRun {

		return
	}

	watcher.isRun = true
	schedule(watcher.run, watcher.timeRange)
}

func schedule(what func(), delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			what()

			select {
			case <-time.After(delay):
			case <-stop:
				return
			}
		}
	}()

	return stop
}
