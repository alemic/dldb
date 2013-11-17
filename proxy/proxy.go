package proxy

import (
	"fmt"
	"github.com/senarukana/dldb/log"
	"net"
	"sync"
	"time"
)

const (
	MAXINT64VALUE = 2>>63 - 1
)

type DldbProxy struct {
	socketPort int
	httpPort   int
	ip         string
	host       string

	//circulate client id
	clientIncrementID  int64
	clientReadTimeout  time.Duration
	clientWriteTimeout time.Duration
	clients            map[int64]*dldbClient
	clientMutex        sync.Mutex

	//circu
	servers map[string]*DldbServer

	running bool
	verbose bool
}

func InitProxy() *DldbProxy {
	initCore("")
	configuration := proxyCore.configure
	server := new(DldbServer)
	server.clients = make(map[int64]*dldbClient)
	server.ip = configuration.ServerConfiguration.Addr
	server.socketPort = configuration.ServerConfiguration.SocketPort
	server.host = server.ip + ":" + fmt.Sprintf("%d", server.socketPort)

	server.clientReadTimeout = configuration.ClientConfiguration.ReadTimeout
	server.clientWriteTimeout = configuration.ClientConfiguration.WriteTimeout
	return server
}

// Serve accepts incoming connections on the Listener l, creating a new client
func (self *DldbProxy) Serve() error {
	addr := self.host
	log.Info(addr)
	localAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		panic(err.Error())
	}
	log.Info(localAddr.String())
	l, err := net.ListenTCP("tcp", localAddr)
	if err != nil {
		panic(err.Error())
	}
	defer l.Close()
	var tempDelay time.Duration // how long to sleep on accept failure
	log.Info("begin listen tcp")
	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Error("accept tcp error %v", err)
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
			} else {
				// a fatal error has occured
				log.Critical("Serve(): Accept fatal error: %v", err)
				panic(fmt.Sprintf("Serve(): Accept fatal error: %v", err))
			}
			// the loggest sleep time is 1 second
			if max := 1 * time.Second; tempDelay > max {
				tempDelay = max
			}
			log.Warn("Serve(): Accept error: %v; retrying in %v", err, tempDelay)
			time.Sleep(tempDelay)
			continue
		}
		log.Info("Come to a new client!")
		tempDelay = 0
		c := newClient(self, conn)
		go c.handleRequest()
	}
}
