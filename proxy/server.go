package proxy

import (
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/log"
	"net"
	"time"
)

const (
	SERVER_NOT_CONNECTED     = 0
	SERVER_CONNECTED         = 1
	SERVER_TEMPORARY_FAILED  = 2
	SERVER_FAILED            = 3
	SERVER_HEATBEAT_INTERVAL = time.Second
	SERVER_HEARTBEAT_TIMEOUT = time.Second
	SERVER_MONITER_INTERVAL  = time.Duration(2) * time.Second
	SERVER_CONNECT_TIMEOUT   = time.Duration(50) * time.Millisecond
	SERVER_CONNECT_DELAY     = time.Duration(10) * time.Millisecond
)

type dldbDataServer struct {
	host                string
	status              int
	socket              *net.Conn
	lastInteractionTime int64
	failedTime          int
	nextConnect         int64 //milliseconds
	clientSendEngine    *balancer.Balancer
}

func initServer(host string) *dldbDataServer {
	dataServer := new(dldbDataServer)
	dataServer.host = host
	dataServer.status = SERVER_NOT_CONNECTED
	dataServer.failedTime = 0
	dataServer.nextConnect = 0
	dataServer.clientSendEngine = proxyCore.getBalancer("clientsend")
	return dataServer
}

func (self *dldbDataServer) connect() (err error) {
	// is already connected
	if self.status == SERVER_CONNECTED {
		return nil
	}
	curTime := time.Now().UnixNano() % 1e6 / 1e3
	if curTime > self.nextConnect {
		self.socket, err = net.DialTimeout("tcp", self.host, SERVER_CONNECT_TIMEOUT)
		if err != nil {
			log.Error("Connect to server %s failed, this is the %d times", self.host, self.failedTime)
			self.failedTime++
			self.nextConnect = curTime + SERVER_CONNECT_DELAY*self.failedTime
			self.status = SERVER_TEMPORARY_FAILED
			return err
		}
		self.lastInteractionTime = time.Now().Unix()
		self.status = SERVER_CONNECTED
	}
	return nil
}

func (self *dldbDataServer) handleRequest() {
	log.Trace("client %v begin handle request", self.socket.RemoteAddr())
	for !self.isForced {
		response := initServerResponse(self)
		err := request.readBinaryRequest()
		if response.header.BatchNum == 0 {
			// not batch response, just send back the response to client
			self.clientSendEngine.ProposeRequest(initClientResponse(req, response))
		}
		/*if err != nil {
			// temporary error, send back error msg
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Trace("client %v encounter a net Error", self.socket.RemoteAddr(), err)
				response := buildErrorResponseMessage(self, ne)
				self.sendBalancer.ProposeRequest(response)
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
			// proxy only send to main host
			host := proxyCore.nodeRing.GetPartNodes(partition)[0]
			// make identifer request
			identifierRequest := new(balancer.IdentifierRequest)
			identifierRequest.Id = host
			identifierRequest.Request = initDldbMessage(self.socket, request.Request)
			//ok
			if err := self.rtnBalancer.ProposeRequest(identifierRequest); err != nil {
				//oops the server is too busy
				response := buildErrorResponseMessage(err)
				self.sendBalancer.ProposeRequest(initDldbMessage(self.socket, response))
			}
		}*/
	}
	// close client elegantly
	self.close()
	return
}

func (self *dldbDataServer) isConnected() bool {
	nullBytes = []byte{}
	if n, err := self.socket.Write(nullBytes); err != nil {
		return false
	}
	return true
}

// heartbeat server to check if the server is still connnected
func serverHeartbeat(servers []*dldbDataServer) {
	for {
		c := time.Tick(SERVER_MONITER_INTERVAL)
		for now := range c {
			for _, server := range servers {
				// the last time that interact with the server is to long ago, need to mont
				if server.lastInteractionTime+SERVER_HEATBEAT_INTERVAL < now.Unix() {
					if server.status == SERVER_CONNECTED {
						// check if is still connected
						if server.isConnected() {
							server.lastInteractionTime = time.Now().Unix()
						} else {
							server.status = SERVER_TEMPORARY_FAILED
						}
					} else {
						// need to reconnect to server
						server.connect()
					}

				}
			}
		}
	}
}
