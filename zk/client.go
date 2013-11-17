package zk

import (
	/*	"errors"
		"fmt"*/
	"github.com/senarukana/dldb/log"
	"launchpad.net/gozk/zookeeper"
	"time"
)

type ZkConn struct {
	*zookeeper.Conn
}

func waitOnConnect(zkEventChan <-chan zookeeper.Event) {
	log.Trace("waiting on zookeeper connection...")
	for {
		event := <-zkEventChan
		if !event.Ok() {
			/*			err := errors.New(fmt.Sprintf("Zookeeper error: %s\n", event.String()))
						panic(err)*/
		}
		if event.Type == zookeeper.EVENT_SESSION {
			switch event.State {
			case zookeeper.STATE_EXPIRED_SESSION:
				panic("Zookeeper connection expired!")
			case zookeeper.STATE_AUTH_FAILED:
				panic("Zookeeper auth failed!")
			case zookeeper.STATE_CLOSED:
				panic("zookeeper connection closed!")
			case zookeeper.STATE_CONNECTING:
				log.Trace("-> zookeeper connecting...")
			case zookeeper.STATE_CONNECTED:
				log.Trace("-> zookeeper connected.")
				return
			}
		}
	}
}

func GetConn(url string) (zk *ZkConn, zkEventChan <-chan zookeeper.Event, err error) {
	log.Trace(url)
	conn, zkEventChan, err := zookeeper.Dial(url, time.Minute)
	zk = new(ZkConn)
	zk.Conn = conn
	_ = <-zkEventChan
	return zk, zkEventChan, err
}
