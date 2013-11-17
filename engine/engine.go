package engine

import (
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
)

type DldbEngine struct {
	engineOps engineOperation
}

type DldbReadOptions struct {
}

type DldbWriteOptions struct {
	Sync bool
}

type DldbEngineOperation struct {
	commandId int
	argc      int
	argv      [][]byte
}

type DldbEngineWriteBatch struct {
	batchNum   int
	operations []*DldbEngineOperation
}

type engineOperation interface {
	Init(conf *conf.EngineConfiguration)
	Get(key []byte, options *DldbReadOptions) ([]byte, error)
	Set(key []byte, value []byte, options *DldbWriteOptions) error
	Delete(key []byte, options *DldbWriteOptions) error
	// WriteBatch(writeBatch *DldbEngineWriteBatch, options *DldbEngineWriteBatch) error
}

type engineType func(conf *conf.EngineConfiguration) engineOperation

var engineAdapters = make(map[string]engineType)

func Register(name string, engineOps engineType) {
	if engineOps == nil {
		panic("engine: register engine operation is nil")
	}
	if _, dup := engineAdapters[name]; dup {
		panic("engine: register engine called twice")
	}
	engineAdapters[name] = engineOps
}

func InitEngine(name string, conf *conf.EngineConfiguration) *DldbEngine {
	engine := new(DldbEngine)
	if engineOps, ok := engineAdapters[name]; ok {
		engine.engineOps = engineOps(conf)
	} else {
		panic("engine: unknown engine name")
	}
	return engine
}

func (self *DldbEngine) Get(key []byte, options *DldbReadOptions) ([]byte, error) {
	value, err := self.engineOps.Get(key, options)
	if value != nil {
		log.Trace("Get Value is %s", value)
	}
	return value, err
}

func (self *DldbEngine) Set(key []byte, value []byte, options *DldbWriteOptions) error {
	var err error
	if err = self.engineOps.Set(key, value, options); err == nil {
		log.Trace("Set Key %s success", key)
	}
	return err
}

func (self *DldbEngine) Delete(key []byte, options *DldbWriteOptions) error {
	return self.engineOps.Delete(key, options)
}
