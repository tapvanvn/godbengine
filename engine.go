package engines

import (
	"github.com/tapvanvn/godbengine/engine"
)

var uniqueEngine *engine.Engine = nil

var InitEngineFunc func(*engine.Engine) = nil

//GetEngine engine
func GetEngine() *engine.Engine {

	if uniqueEngine == nil {

		testEngine := &engine.Engine{}

		if InitEngineFunc != nil {

			InitEngineFunc(uniqueEngine)
		}

		uniqueEngine = testEngine
	}
	return uniqueEngine
}
