package engine

import (
	"bytes"
	"github.com/senarukana/dldb/conf"
	"testing"
)

func TestEngine(t *testing.T) {
	c := conf.InitDldbConfiguration("test.conf")
	// test Init Engine
	engine := InitEngine("leveldb", &c.EngineConfiguration)
	key := []byte("user")
	val := []byte("ted")
	if err := engine.Set(key, val, nil); err != nil {
		t.Errorf("set key %s error, error =  %v", key, err)
	}

	v, err := engine.Get(key, nil)
	if err != nil {
		t.Errorf("get key %s error, error =  %v", key, err)
	}
	if !bytes.Equal(v, val) {
		t.Errorf("get val is %s, want %s", v, val)
	}

}
