package proxy

import (
	"fmt"
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/zk"
	"launchpad.net/gozk/zookeeper"
)

type zkManagement struct {
	connection       *zk.ZkConn
	connectEventChan <-chan *zookeeper.Event
	nodeEventChan    []<-chan *zookeeper.Event
}

func initZkManagement(configuration *conf.ZooKeeperClientConfiguration) {
	zkManage := new(zkManagement)
	var err error
	zkManage.connection, zkManage.connectEventChan, err = zk.GetConn(configuration.ZKAddress)
	if err != nil {
		panic(fmt.Sprintf("Get ZooKeeper Connection Failed, %v", err))
	}
	zkManage.nodeEventChan, err = zkManage.connection.MoniterRingNodes(configuration.RingPath)
	if err != nil {
		panic(fmt.Sprintf("Montier Ring Node Failed, %v", err))
	}
	return zkManage
}

func (self *zkManagement) manageRingChange() {
	/*for {
		select {
		case self.nodeEventChan:
			log.Info("")
		}
	}*/
}
