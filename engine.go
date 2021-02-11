package engines

import (
	"github.com/tapvanvn/godbengine/engine"
)

var uniqueEngine *engine.Engine = nil

//InitEngineFunc init engine function
var InitEngineFunc func(*engine.Engine) = nil

//GetEngine engine
func GetEngine() *engine.Engine {

	if uniqueEngine == nil {

		testEngine := &engine.Engine{}

		if InitEngineFunc != nil {

			InitEngineFunc(testEngine)
		}

		uniqueEngine = testEngine
	}
	return uniqueEngine
}
