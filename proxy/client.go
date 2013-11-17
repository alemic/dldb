package proxy

import (
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/util"
	"net"
	"time"
)

type dldbClient struct {
	clientId           int64
	socket             *net.TCPConn
	server             *DldbServer
	status             int
	isForced           bool
	clientSendBalancer *balancer.Balancer
	negoriateBalancer  *balancer.Balancer
	serverSendBalancer *balancer.Balancer

	/*waitting            bool
	lastInteractionTime int64*/
}

func newClient(server *DldbServer, socket *net.TCPConn) *dldbClient {
	client := new(dldbClient)
	client.server = server
	client.socket = socket
	client.socket.SetNoDelay(true)
	client.clientId = server.clientIncrementID
	server.clients[server.clientIncrementID] = client
	server.clientIncrementID++
	if server.clientIncrementID >= MAXINT64VALUE {
		server.clientIncrementID = 0
	}
	if d := server.clientReadTimeout; d != 0 {
		client.socket.SetReadDeadline(time.Now().Add(d))
	}
	if d := server.clientWriteTimeout; d != 0 {
		client.socket.SetWriteDeadline(time.Now().Add(d))
	}
	client.clientSendBalancer = proxyCore.getBalancer("clientsend")
	client.negoriateBalancer = proxyCore.getBalancer("negotiate")
	client.serverSendBalancer = proxyCore.getBalancer("serversend")
	return client
}

func (self *dldbClient) handleRequest() {
	log.Trace("client %v begin handle request", self.socket.RemoteAddr())
	for !self.isForced {
		request := initClientRequest(self)
		err := request.readBinaryRequest()
		if err != nil {
			// temporary error, send back error msg
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Trace("client %v encounter a net Error", self.socket.RemoteAddr(), err)
				response := buildErrorResponseMessage(ne)
				self.clientSendBalancer.ProposeRequest(request)
			} else {
				log.Trace("client %v encounter a Fatal Error", self.socket.RemoteAddr(), err)
				//timeout eof or fatal error
				self.close()
				return
			}
		} else {
			log.Trace("client %v propose request", self.socket.RemoteAddr())
			// find host
			partition := util.HashFunction(request.header.BodyLength)
			if proxyCore.nodeRing.GetReplicas() == 1 {
				// batch the request
				host := proxyCore.nodeRing.GetPartNodes(partition)[0]
				// make identifer request
				identifierRequest := new(balancer.IdentifierRequest)
				identifierRequest.Id = host
				identifierRequest.Request = &request
				//ok
				if err := self.rtnBalancer.ProposeRequest(identifierRequest); err != nil {
					//oops the server is too busy
					response := buildErrorResponseMessage(err)
					self.clientSendBalancer.ProposeRequest(initClientResponse(client, response))
				}
			} else {
				// send request to multiple nodes and wait for response

			}
		}
	}
	// close client elegantly
	self.close()
	return
}

func (self *dldbClient) elegantClose() {
	self.isForced = true
}

func (self *dldbClient) close() {
	self.socket.Close()
	self.server.clientMutex.Lock()
	delete(self.server.clients, self.clientId)
	log.Info("now it has %d clients", len(self.server.clients))
	self.server.clientMutex.Unlock()
}
