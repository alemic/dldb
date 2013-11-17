package proxy

import (
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/log"
)

type batchHandler struct {
	clientSendEngine *balancer.Balancer
	serverSendEngine *balancer.Balancer
}

func (self *batchHandler) Init() {
	self.clientSendEngine = proxyCore.getBalancer("clientSend")
	self.serverSendEngine = proxyCore.getBalancer("serverSend")
}

func (self *batchHandler) EventHandle(item interface{}) {
	response := item.(*dldbResponse)
	log.Trace("client %s come to send stage", response.client.socket.RemoteAddr())
	if _, err := response.client.socket.Write(response.response); err != nil {
		log.Trace("write response to client %s error, %v", response.client.socket.RemoteAddr(), err)
		response.client.close()
	} else {
		log.Trace("client %s send response %d complete", response.client.socket.RemoteAddr(), len(response.response))
		// send response success
	}
}

func (self *batchHandler) EventsHandle(item interface{}) {
	requests := item.([]*balancer.IdentifierRequest)
	batchRequest := buildBatchRequest(requests)
	if err
}
