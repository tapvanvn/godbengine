package engine

import (
	"strings"
	"sync"
	"time"
)

type Watcher struct {
	mux       sync.Mutex
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
		documents: make(map[string]Document),
		tick:      map[string]int64{},
		dirty:     map[string]bool{},
		pool:      pool,
	}
}

func (watcher *Watcher) run() {

	deadline := time.Now().Unix() - int64(watcher.timeRange.Seconds())

	transaction := watcher.pool.MakeTransaction()

	transaction.Begin()
	count := 0
	for key, dirty := range watcher.dirty {
		if dirty {
			tick, _ := watcher.tick[key]
			if tick < deadline {
				if doc, ok := watcher.documents[key]; ok {
					parts := strings.Split(key, "$")
					transaction.Put(parts[0], doc)
					watcher.mux.Lock()
					watcher.dirty[key] = false
					watcher.mux.Unlock()
					count++
					if count == 150 {

						err := transaction.Commit()
						if err != nil {
							//TODO: report error
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

//Load carefull when using this function, each mapid map only to one doc at a time. Reload a document will disrupt other connection
func (watcher *Watcher) Watch(collection string, doc Document) error {

	mapID := collection + "$" + doc.GetID()

	watcher.mux.Lock()
	watcher.documents[mapID] = doc
	watcher.tick[mapID] = time.Now().Unix()
	watcher.dirty[mapID] = false
	watcher.mux.Unlock()
	return nil
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
