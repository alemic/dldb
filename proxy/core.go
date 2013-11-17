package proxy

import (
	"fmt"
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/ring"
	"github.com/senarukana/dldb/util"
	"github.com/senarukana/dldb/zk"
)

type Request interface{}

type dldbCore struct {
	configure           *conf.DldbProxyConfiguration
	balancers           map[string]*balancer.Balancer
	nodeRing            *ring.Ring
	zkManage            *zkManagement
	servers             map[string]*DldbDataServer
	serverRequestSender map[string]*util.Queue // request queue for different servers
	commandTable        dldbCommands
}

var proxyCore *dldbCore

func (self *dldbCore) registerBalancer(name string, handler balancer.EventHandler) *balancer.Balancer {
	b := balancer.NewBalancer(name, handler, &self.configure.BalancersConfiguration)
	b.Init()
	self.balancers[name] = b
	return b
}

func (self *dldbCore) getBalancer(name string) *balancer.Balancer {
	if balancer, ok := self.balancers[name]; ok {
		return balancer
	} else {
		panic(fmt.Sprintf("balancer %s is not existed, forget to register it?", name))
	}
}

func (self *dldbCore) initLogger() {
	for _, logType := range self.configure.LogConfiguration.Logs {
		if err := log.SetLogger(logType, &self.configure.LogConfiguration); err != nil {
			panic(fmt.Sprintf("init Logger error, %s not existed %v", logType, err))
		}
	}
}

// get ring from the zookeeper and moniter the change of the ring
func (self *dldbCore) initRingAndMoniter() {
	r, err := ring.InitRingDataFromZooKeeper()
	if err != nil {
		panic(fmt.Sprintf("init ring from zookeeper error: %v", err))
	}
	self.nodeRing = r
	self.zkManage = initZkManagement(self.configure.ZooKeeperClientConfiguration)
	// mointer ring change
	go self.zkManage.manageRingChange()
}

// init & connect & moniter data server, init server request quque
func (self *dldbCore) initDataServer() {
	self.servers = make(map[string]*dldbDataServer)
	self.serverRequestSender = make(map[string]*serverRequestSender)
	for _, serverHost := range self.configure.ServersConfiguration.ServersHost {
		server := initServer(serverHost)
		self.servers[serverHost] = server
		server.connect()
		self.serverRequestSender[serverHost] = initServerRequestSender(server, sendBuffer, sendLimits, batchNum, blockingTime)
	}
	go serverHeartbeat(self.servers)
}

func initCore(configureFileName string) {
	proxyCore = new(dldbCore)
	proxyCore.configure = conf.InitDldbConfiguration(configureFileName)
	proxyCore.initLogger()
	proxyCore.initRingAndMoniter()
	proxyCore.connectAndMoniterDataServer()
	proxyCore.balancers = make(map[string]*balancer.Balancer)
	proxyCore.commandTable = initCommandTable()
	// init balancer
	for _, balancerName := range proxyCore.configure.BalancersConfiguration.BalancersName {
		var handler balancer.EventHandler
		switch balancerName {
		case "engine":
			handler = new(engineHandler)
		case "send":
			handler = new(sendHandler)
		default:
			panic("unknown balancer name")
		}
		proxyCore.registerBalancer(balancerName, handler)
	}
}
