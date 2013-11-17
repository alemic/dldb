package server

/*
	protocal is a little bit like redis

	Message

	SET Command
	------------------------------------------
	*3\r\n   				#Command length
	SET\r\n					#Command Str
	$2\r\n					#Set Record Num
	$4\r\n					#the first key size
	user\r\n				#the first key
	$3\r\n					#the first val size
	ted						#the first val
	$5\r\n					#the second key size
	email\r\n				#the second key
	$18\r\n					#the second val size
	lizhe.ted@gmail.com		#the second val

	GET Command
	------------------------------------------
	*3\r\n   				#Command length
	GET\r\n					#Command Str
	$2\r\n					#Get Record Num
	$4\r\n					#the first key size
	user\r\n				#the first key
	$5\r\n					#the second key size
	email\r\n				#the second key

	SCAN Command					#
	*4\r\n
	SCAN
	$10						#maximum reply records
	$4						#start key size
	abcd					#start key
	$5						#end key size
	zzzzz					#end key val

	ZADD Command			#sorted list add
	*4\r\n   				#Command length
	ZADD\r\n				#Command Str
	$6\r\n					#list name size
	mylist					#sorted list str
	$4\r\n					#the key size
	user\r\n				#the key value
	$3\r\n					#the val size
	ted						#the val value

	ZGET Command


	ZRANGE COMMAND
	*6\r\n
	ZRANGE\r\n
	$6\r\n
	mylist
	0						#start offset
	10						#end offset




	Response

	In a Status Reply the first byte of the reply is "+"
	In an Error Reply the first byte of the reply is "-"
	In an Integer Reply the first byte of the reply is ":"
	In a Bulk Reply the first byte of the reply is "$"
	In a Multi Bulk Reply the first byte of the reply s "*"


	Status reply
	A Status Reply (or: single line reply) is in the form of a single line string starting with "+" terminated by "\r\n". For example:
	+OK

	Error reply
	-ERR unknown command 'foobar'
	-WRONGTYPE Operation against a key holding the wrong kind of value

	Bulk replies
	C: GET mykey
	S: $6\r\nfoobar\r\n

	Multi-bulk replies
	C: SCAN abc fgh 4
	S: *4
	S: $3
	S: foo
	S: $3
	S: bar
	S: $5
	S: Hello
	S: $5
	S: World

*/

import (
	"github.com/senarukana/dldb/balancer"
	"github.com/senarukana/dldb/log"
	"net"
	"time"
)

type dldbClient struct {
	clientId       int64
	socket         *net.TCPConn
	server         *DldbServer
	status         int
	isForced       bool
	sendBalancer   *balancer.Balancer
	engineBalancer *balancer.Balancer

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
	client.sendBalancer = dbCore.getBalancer("send")
	client.engineBalancer = dbCore.getBalancer("engine")
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
			//ok
			if err := self.engineBalancer.ProposeRequest(request); err != nil {
				//oops the server is too busy
				response := buildErrorResponseMessage(self, err)
				self.sendBalancer.ProposeRequest(response)
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
