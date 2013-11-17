package proxy

import (
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/ring"
	"time"
)

type negotiateHandler struct {
	clientSendBalancer  *balancer.Balancer
	serversSender       map[string]*serverRequestSender
	pendingSendBalancer *balancer.Balancer
	writeLowWater       int
	readLowWater        int
	commands            *dldbCommands
	nodeRing            *ring.Ring
}

func (self *negotiateHandler) Init() {
	self.clientSendEngine = proxyCore.getBalancer("clientSend")
	self.serverSendEngine = proxyCore.getBalancer("serverSend")
	self.pendingSendBalancer = proxyCore.getBalancer("pendingSend")
	self.commands = proxyCore.commandTable
	self.writeLowWater = proxyCore.configure.ConsistencyConfiguration.WriteLowWater
	self.readLowWater = proxyCore.configure.ConsistencyConfiguration.ReadLowWater
	self.nodeRing = proxyCore.nodeRing
}

func (self *negotiateHandler) EventHandle(item interface{}) {
	request := item.(*dldbNegotiateRequest)
	log.Trace("[%s]client %s come to a new request", "Negotiate", response.client.socket.RemoteAddr())
	oprationType := self.commands[int(request.clientRequest.request.Header.OpCode)].isModified // true is write, falese is read
	responseChan := make(chan *dldbDataServerResponse, self.nodeRing.GetReplicas())
	replicas := self.nodeRing.GetReplicas()
	for i, serverHost := range self.nodeRing.GetPartNodes(request.partition) {
		serverRequest := initDataServerRequest(request.clientRequest.request, serverHost, oprationType, responseChan)
		self.serverSendBalancer.ProposeRequest(serverRequest)
	}
	// wait for response from serverSendBalancer
	responses = make([]*dldbDataServerResponse, replicas)
	for i := 0; i < replicas; i++ {
		responses[i] = <-responseChan
	}

}

func (self *negotiateHandler) _bestResponse(responses []*dldbDataServerResponse, operationType bool) {
	if operationType {
		//write
		operationLowWater := self.writeLowWater
	} else {
		operationLowWater := self.readLowWater
	}
	var successServers []string
	var failureServers []string
	for _, response := range responses {
		if response.err != nil {
			failureServers = append(failureServers, response.serverHost)
		} else {
			// we need to check the header
			responseHeader := response.header
			// 0~ 100 success
			if responseHeader.StatusCode > 0 && responseHeader.StatusCode < 100 {
				if !operationType {
					successServers = append(successServers, response.serverHost)
				}
			} else {
				failureServers = append(failureServers, response.serverHost)
			}
		}
	}
}

func (self *batchHandler) EventsHandle(item interface{}) {

}
