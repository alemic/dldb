package proxy

import (
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/ring"
)

type pendingSendHandler struct {
	serverSendEngine *balancer.Balancer
	servers          []*DldbDataServer
}

func (self *negotiateHandler) Init() {
	self.clientSendEngine = proxyCore.getBalancer("clientSend")
	self.serverSendEngine = proxyCore.getBalancer("serverSend")
	self.nodeRing = proxyCore.nodeRing
}

func (self *negotiateHandler) EventHandle(item interface{}) {
	request := item.(*dldbNegotiateRequest)
	log.Trace("[%s]client %s come to a new request", "Negotiate", response.client.socket.RemoteAddr())
	for _, serverHost := range self.nodeRing.GetPartNodes(request.partition) {

	}
	if _, err := response.client.socket.Write(response.response); err != nil {
		log.Trace("write response to client %s error, %v", response.client.socket.RemoteAddr(), err)
		response.client.close()
	} else {
		log.Trace("client %s send response %d complete", response.client.socket.RemoteAddr(), len(response.response))
		// send response success
	}
}

func (self *batchHandler) EventsHandle(item interface{}) {

}
