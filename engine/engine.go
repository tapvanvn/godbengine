package engine

import (
	"crypto/rsa"
)

//Engine hold all adapter to ready to work
type Engine struct {
	documentPool         DocumentPool
	memPool              MemPool
	filePool             FilePool
	adminPublicKeyString string
	adminPublicKey       *rsa.PublicKey
}

//Init init engine
func (engine *Engine) Init(memPool MemPool, documentPool DocumentPool, filePool FilePool) {

	engine.memPool = memPool

	engine.documentPool = documentPool

	engine.filePool = filePool
}

//GetMemPool get current mempool
func (engine *Engine) GetMemPool() MemPool {

	return engine.memPool
}

//GetDocumentPool get current document pool
func (engine *Engine) GetDocumentPool() DocumentPool {

	return engine.documentPool
}

//GetFilePool get current file pool
func (engine *Engine) GetFilePool() FilePool {

	return engine.filePool
}
