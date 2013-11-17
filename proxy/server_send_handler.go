package proxy

import (
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/log"
	"time"
)

// this handler is a identifier balancer's handler, each server only have one worker to do at a time.
type serverSendHandler struct {
	servers      map[string]*dldbDataServer
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (self *serverSendHandler) Init() {
	self.servers = proxyCore.servers
	self.readTimeout = proxyCore.configure.ServersConfiguration.ReadTimeout
	self.writeTimeout = proxyCore.configure.ServersConfiguration.WriteTimeout
}

func (self *serverSendHandler) EventHandle(item interface{}) {
	log.Trace("[%s] come to a new request", "Server Send")
	request := item.(*balancer.IdentifierRequest).Request.(*dldbDataServerRequest)
	server := self.servers[request.serverHost]
	content := append(request.clientRequest.HeaderBytes, request.clientRequest.Body...)
	response := self._sendRequest(server, content)
	// send response to negotiate handler
	request.responseChan <- response
}

func (self *serverSendHandler) EventsHandle(item interface{}) {
	log.Trace("[%s] come to a new batch request", "Server Send")
	requests := item.([]*balancer.IdentifierRequest)
	// merge requests to batch request
	batchRequest := buildBatchServerRequest(requests)
	serverHost := requests[0].Request.(*dldbDataServerRequest).serverHost
	server := self.servers[serverHost]
	response := self._sendRequest(server, batchRequest)
	// TODO
	// send response to negotiate handler
	request.responseChan <- response
}

func (self *serverSendHandler) _sendRequest(server *dldbDataServer, content []byte) *dldbDataServerResponse {
	var response *dldbDataServerResponse
	if server.status == SERVER_CONNECTED {
		// write request
		server.socket.SetWriteDeadline(time.Now().Add(self.writeTimeout)) // write
		if _, err := server.socket.Write(content); err != nil {
			response = initDataServerResponseError(server.host, err)
			return response
		}

		// read response
		server.socket.SetReadDeadline(time.Now().Add(self.readTimeout)) // read
		response = initDataServerResponse(server)
		if err := response.readBinaryResponse(); err != nil {
			response.err == err
		}
		return response
	} else {
		response = initDataServerResponseError(server.host, ServerTemporaryFailedError(server.host))
		return response
	}
}
