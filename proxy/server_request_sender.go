package proxy

import (
	"github.com/senarukana/dldb/util"
	"time"
)

type serverRequestSender struct {
	queue  *util.Queue
	quit   bool
	server *dldbDataServer
}

func initServerRequestSender(server *dldbDataServer, sendBuffer int, sendLimits int, batchNum int, blockingTime time.Duration) *serverRequestSender {
	sender := new(serverRequestSender)
	sender.queue = util.InitQueue(sendBuffer, sendLimits, batchNum, blockingTime)
	sender.server = server
	sender.quit = false
	return sender
}

func (self *serverRequestSender) push(request *dldbDataServerRequest) error {
	return self.queue.Enqueue(request)
}

func (self *serverRequestSender) loop() {
	for !self.quit {
		requests := self.queue.Dequeue()
		switch len(requests) {
		case 0:
			continue
		case 1:

		}
	}
}

func (self *serverRequestSender) _sendRequest(request *dldbDataServerRequest) {

}
