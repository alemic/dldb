package core

/*import (
	"github.com/senarukana/dldb/balancer"
	"net"
)

type receiveHandler struct {
	sendBalancer   *balancer.Balancer
	engineBalancer *balancer.Balancer
}

func (self *receiveHandler) Init() *receiveHandler {
	self.sendBalancer = dbCore.GetBalancer("send")
	self.engineBalancer = dbCore.GetBalancer("engine")
}

func (self *receiveHandler) EventHandle(item interface{}) {
	client := item.(*dldbClient)
	request := initClientRequest(client)
	err := request.readBinaryRequest()
	if err != nil {
		// temporary error, send back error msg
		if ne, ok := err.(net.Error); ok && (ne.Temporary() || ne.Timeout()) {
			response := buildErrorResponseMessage(client, ne)
			self.sendBalancer.ProposeRequest(response)
		} else if ne, ok := err.(NetError); ok && ne.Temporary() {
			response := buildErrorResponseMessage(client, ne)
			self.sendBalancer.ProposeRequest(response)
		} else {
			// eof or fatal error
			client.close()
		}
	} else {
		if err := engineBalancer.ProposeRequest(request); err != nil {
			//oops the server is too busy
			response := buildErrorResponseMessage(client, err)
			self.sendBalancer.ProposeRequest(request)
		}
	}
}

// don't do any aggregation
func (self *receiveHandler) EventsHandle(item interface{}) {
}
*/
