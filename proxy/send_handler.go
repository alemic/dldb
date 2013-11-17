package server

import (
	"github.com/senarukana/dldb/log"
)

type sendHandler struct{}

func (self *sendHandler) Init() {}

func (self *sendHandler) EventHandle(item interface{}) {
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

func (self *sendHandler) EventsHandle(item interface{}) {
	responses := item.([]*dldbResponse)
	for _, response := range responses {
		self.EventHandle(response)
	}
}
