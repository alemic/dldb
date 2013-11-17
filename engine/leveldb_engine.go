package engine

import (
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
	"path"
)

type leveldb_engine struct {
	//db file name
	fileName string
	*levigo.DB
}

func (self *leveldb_engine) Init(conf *conf.EngineConfiguration) {

	self.fileName = path.Join(conf.LeveldbEngineConfiguration.FilePath, conf.LeveldbEngineConfiguration.FileName)
	var err error
	// TODO init options
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(conf.LeveldbEngineConfiguration.LRUCacheSize))
	opts.SetCreateIfMissing(conf.LeveldbEngineConfiguration.CreateIfMissing)
	self.DB, err = levigo.Open(self.fileName, opts)
	if err != nil {
		panic(fmt.Sprintf("init leveldb engine error, %v, %v", self.fileName, err))
	}
}

func (self *leveldb_engine) Get(key []byte, options *DldbReadOptions) ([]byte, error) {
	if self.DB == nil {
		log.Critical("Level DB Get Operation, DB is Nil")
		panic("Level DB Get Operation, DB is Nil")
	}
	readOptions := levigo.NewReadOptions()
	return self.DB.Get(readOptions, key)
}

func (self *leveldb_engine) Set(key []byte, value []byte, options *DldbWriteOptions) error {
	if self.DB == nil {
		log.Critical("Level DB Get Operation, DB is Nil")
		panic("Level DB Get Operation, DB is Nil")
	}
	writeOptions := levigo.NewWriteOptions()
	if options != nil {
		writeOptions.SetSync(options.Sync)
	}
	return self.DB.Put(writeOptions, key, value)
}

func (self *leveldb_engine) Delete(key []byte, options *DldbWriteOptions) error {
	if self.DB == nil {
		log.Critical("Level DB Get Operation, DB is Nil")
		panic("Level DB Get Operation, DB is Nil")
	}
	writeOptions := levigo.NewWriteOptions()
	if options != nil {
		writeOptions.SetSync(options.Sync)
	}
	return self.DB.Delete(writeOptions, key)
}

func NewLevelDBEngine(conf *conf.EngineConfiguration) engineOperation {
	engine := new(leveldb_engine)
	engine.Init(conf)
	return engine
}

func init() {
	Register("leveldb", NewLevelDBEngine)
}
