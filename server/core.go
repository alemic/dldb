package server

import (
	"fmt"
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/conf"
	"github.com/senarukana/dldb/log"
)

type Message interface{}

type dldbCore struct {
	configure    *conf.DldbServerConfiguration
	balancers    map[string]*balancer.Balancer
	commandTable dldbCommands
}

var dbCore *dldbCore

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

func initCore(configureFileName string) {
	dbCore = new(dldbCore)
	dbCore.configure = conf.InitServerDldbConfiguration(configureFileName)
	dbCore.initLogger()
	dbCore.balancers = make(map[string]*balancer.Balancer)
	dbCore.commandTable = initCommandTable()
	// init balancer
	for _, balancerName := range dbCore.configure.BalancersConfiguration.BalancersName {
		var handler balancer.EventHandler
		switch balancerName {
		case "engine":
			handler = new(engineHandler)
		case "send":
			handler = new(sendHandler)
		default:
			panic("unknown balancer name")
		}
		dbCore.registerBalancer(balancerName, handler)
	}
}
