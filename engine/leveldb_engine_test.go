package engine

import (
	"bytes"
	"github.com/senarukana/dldb/conf"
	"testing"
)

func TestLeveldb(t *testing.T) {
	//test name
	f := new(leveldb_engine)
	c := conf.InitDldbConfiguration("leveldb.conf")
	f.Init(&c.EngineConfiguration)
	if f.fileName != "test.db" {
		t.Errorf("file name = %s, want %s, ", f.fileName, "test.db")
	}

	// test set
	key := []byte("user")
	val := []byte("ted")
	if err := f.Set(key, val, nil); err != nil {
		t.Errorf("set key %v error", key)
	}

	// test get
	v, err := f.Get(key, nil)
	if err != nil {
		t.Error("get key %v error", key)
	}
	if !bytes.Equal(v, val) {
		t.Errorf("get val is %s, want %s", v, val)
	}

	// test delete
	if err := f.Delete(key, nil); err != nil {
		t.Error("delete key %v error", key)
	}
	va, _ := f.Get(key, nil)
	//  va will be nil , if key doesn't exist
	if va != nil {
		t.Error("delete key %v fail", key)
	}
	f.Close()
}
