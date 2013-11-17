package core

import (
	"github.com/senarukana/dldb/log"
)

type SendHandler struct{}

func (self *SendHandler) Init() {}

func (self *SendHandler) EventHandle(item interface{}) {
	response := item.(*DldbResponse)
	log.Trace("client %s come to send stage", response.client.socket.RemoteAddr())
	if _, err := response.client.socket.Write(response.response); err != nil {
		log.Trace("write response to client %s error, %v", response.client.socket.RemoteAddr(), err)
		response.client.close()
	} else {

		log.Trace("client %s send response %d complete", response.client.socket.RemoteAddr(), len(response.response))
		// send response success
	}
}

func (self *SendHandler) EventsHandle(item interface{}) {
	responses := item.([]*DldbResponse)
	for _, response := range responses {
		self.EventHandle(response)
	}
}
