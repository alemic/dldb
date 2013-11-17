package server

import (
	"github.com/senarukana/dldb/engine"
	"github.com/senarukana/dldb/log"
)

type engineHandler struct {
	engine *engine.DldbEngine
	// sendbalancer *balancer.Balancer
}

func (self *engineHandler) Init() {
	// self.sendbalancer = dbCore.getBalancer("send")
	// panic(dbCore.configure.EngineConfiguration.EngineName)
	self.engine = engine.InitEngine(dbCore.configure.EngineConfiguration.EngineName, &dbCore.configure.EngineConfiguration)
}

func (self *engineHandler) EventHandle(item interface{}) {
	request := item.(*dldbRequest)
	log.Trace("client %s engine handle", request.client.socket.RemoteAddr())
	var response *dldbResponse
	switch request.header.OpCode {
	case 1:
		readOptions := new(engine.DldbReadOptions)
		if value, err := self.engine.Get(request.argv[1], readOptions); err != nil {
			log.Trace("[client %s] GET operation encounter an error %v", request.client.socket.RemoteAddr(), err)
			response = buildErrorResponseMessage(request.client, err)
		} else {
			// no error, check if value is existed in db
			if value == nil {
				log.Trace("response is empty")
				response = buildEmptyResponseMessage(request.client)
			} else {
				response = buildResponseMessage(request.client, value)
			}
		}
	case 2:
		writeOptions := new(engine.DldbWriteOptions)
		if err := self.engine.Set(request.argv[1], request.argv[2], writeOptions); err != nil {
			log.Trace("[client %s] SET operation encounter an error %v", request.client.socket.RemoteAddr(), err)
			response = buildErrorResponseMessage(request.client, err)
		} else {
			response = buildSuccessResponseMessage(request.client)
		}
	}
	log.Trace("client %s engine handle propse request", request.client.socket.RemoteAddr())
	dbCore.getBalancer("send").ProposeRequest(response)
}

func (self *engineHandler) EventsHandle(item interface{}) {
	requests := item.([]*dldbRequest)
	for _, request := range requests {
		self.EventHandle(request)
	}
	/*	switch client.opCode {
		case 1:
			readOptions := new(engine.DldbReadOptions)
			if value, err := self.engine.Get(client.argv[0], readOptions); err != nil {

			}
		case 2:
			writeOptions := new(engine.DldbWriteOptions)
			self.engine.Set(client.argv[0], client.argv[1], writeOptions)
		}*/
}
