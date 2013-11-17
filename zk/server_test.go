package zk

import (
	"github.com/senarukana/dldb/log"
	"launchpad.net/gozk/zookeeper"
	"testing"
	"time"
)

var (
	server *ZkServer
	zk     *ZkConn
)

func TestServer(t *testing.T) {
	initServer(t)
	defer destroyServer(t)
	basicTest()
	testMutex(t)

}

func initServer(t *testing.T) {
	server = NewZkServer(2183, "/tmp/zktest", "/home/ted/zookeeper/zookeeper-3.4.5")
	if err := server.Init(); err != nil {
		t.Errorf("couldn't set up zk server: %v", err)
	}
	t.Log("set up successfully")
	zk = server.Zk
}

func destroyServer(t *testing.T) {
	if err := server.Destroy(); err != nil {
		t.Errorf("could not destroy zk server: %v", err)
	}
	t.Log("destroy successfully")
}

func basicTest() {
	zk, session, err := zookeeper.Dial("localhost:2183", 5e9)
	if err != nil {
		log.Trace("Can't connect: %v", err)
	}
	defer zk.Close()

	// Wait for connection.
	event := <-session
	if event.State != zookeeper.STATE_CONNECTED {
		log.Trace("Can't connect: %v", event)
	}

	_, err = zk.Create("/counter", "0", 0, zookeeper.WorldACL(zookeeper.PERM_ALL))
	if err != nil {
		log.Trace("Can't create counter: %v", err)
	} else {
		log.Trace("Counter created!")
	}
}

func tryToLock(t *testing.T, path string, doneChan chan bool) {
	m := NewMutex(zk.Conn, path)
	if err := m.Lock(); err != nil {
		t.Errorf("try lock error:%v", err)
	}
	if err := m.Unlock(); err != nil {
		t.Errorf("try unlock error:%v", err)
	}
	doneChan <- true
}

func tryToTryLock(t *testing.T, path string, doneChan chan bool) {
	m := NewMutex(zk.Conn, path)
	getted, err := m.TryLock()
	if err != nil {
		t.Errorf("try lock error:%v", err)
	}
	if getted {
		t.Error("shouldn't get the lock")
	}
	doneChan <- true
}

func testMutex(t *testing.T) {
	lockPath := "/test/testmutex"
	zk.RecursiveDelete("/test")
	if _, err := zk.RecursiveTouch(lockPath); err != nil {
		t.Errorf("touch error: %v", err)
	}
	m := NewMutex(zk.Conn, lockPath)
	if err := m.Lock(); err != nil {
		t.Errorf("mutex Lock error: %v", err)
	}
	doneChan := make(chan bool)
	go tryToLock(t, lockPath, doneChan)
	timerChan := time.After(1 * time.Second)
	select {
	case <-doneChan:
		t.Error("shouldn't be able to lock")
	case <-timerChan:
	}
	if err := m.Unlock(); err != nil {
		t.Errorf("mutex unlock err :%v", err)
	}
	timerChan = time.After(1 * time.Second)
	select {
	case <-doneChan:
	case <-timerChan:
		t.Error("should be able to lock")
	}

	trylockPath := "/test/testtrymutex"
	if _, err := zk.RecursiveTouch(trylockPath); err != nil {
		t.Errorf("touch error: %v", err)
	}
	m1 := NewMutex(zk.Conn, trylockPath)
	// test try lock
	getted, err := m1.TryLock()
	if err != nil {
		t.Errorf("mutex Lock error: %v", err)
	}
	if !getted {
		t.Error("should be able to lock")
	}
	go tryToTryLock(t, trylockPath, doneChan)
	timerChan = time.After(1 * time.Second)
	select {
	case <-doneChan:
	case <-timerChan:
		t.Error("try lock shouldn't wait")
	}
	if err := m1.Unlock(); err != nil {
		t.Errorf("mutex unlock err :%v", err)
	}

}

// func T
