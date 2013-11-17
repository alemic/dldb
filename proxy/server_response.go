package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/senarukana/dldb/log"
	"net"
	"unsafe"
)

type dldbDataServerResponse struct {
	serverHost string
	server     *dldbDataServer
	header     *BinaryResponseHeader
	head       []byte
	body       []byte
	err        error
}

func initDataServerResponse(server *dldbDataServer) *dldbDataServerResponse {
	response := new(dldbDataServerResponse)
	response.server = server
	return response
}

func initDataServerResponseError(serverHost string, err error) {
	response := new(dldbDataServerResponse)
	response.serverHost = serverHost
	response.err = err
	return response
}

// do read until it reads the specified buffer size
/*func (self *dldbClientRequest) obstinateRead(readSize int, buffer []byte) (int n, err error) {
	if d := self.client.server.readerTimeout; d != 0 {
		self.socket.SetReadDeadline(time.Now().Add(d))
	}
	n = 0
	for {
		// once the server is quited, it will set every clients isForced = true
		if !self.isForced {
			nr, err = self.client.socket.Read(buffer)
			if err != nil {
				DldbLogger.Trace(fmt.Sprintf("client %s error : %v", self.client.socket.RemoteAddr(), err))
				return n, err
			}
			n += nr
			if n >= readSize {
				DldbLogger.Trace(fmt.Sprintf("read %d size", n))
				return n, nil
			}
		} else {
			return n, ServerQuitError("quit")
		}
	}
}*/

func (self *dldbDataServerResponse) readBinaryResponse() error {
	log.Trace("Begin read server response")
	// read the msgHeader
	self.header = new(BinaryResponse)
	responseHeaderSize := unsafe.Sizeof(*self.header)
	self.head = make([]byte, responseHeaderSize)
	if _, err := self.client.socket.Read(self.head); err != nil {
		log.Trace("error %v, client: %s read failed, client will exit", err, self.client.socket.RemoteAddr())
		return err
	}
	// cast []byte to requestHeader
	// TODO : set the binary order according to the system
	binary.Read(bytes.NewBuffer(self.head), binary.LittleEndian, self.header)

	// judge magic code
	if int(self.header.Magic) != DLDB_RESPONSR_MAGIC {
		log.Trace("invalid magic code from client %s", self.client.socket.RemoteAddr())
		return InvalidRequestError("magic code is invalid")
	}

	// read body
	self.body = make([]byte, self.header.BodyLength)
	if _, err := self.client.socket.Read(self.body); err != nil {
		log.Trace("error %v, client: %s read failed, client will exit", err, self.client.socket.RemoteAddr())
		return err
	}
	log.Trace("Read response complete")
	return nil

}
