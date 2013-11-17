package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/senarukana/dldb/log"
	"net"
	"unsafe"
)

type dldbClientRequest struct {
	client  *dldbClient
	request *BinaryRequest
}

func initClientRequest(client *dldbClient) *dldbClientRequest {
	request := new(dldbClientRequest)
	request.client = client
	request.request = new(BinaryRequest)
	return request
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

func (self *dldbClientRequest) readBinaryRequest() error {
	log.Trace("Begin read request")
	// read the msgHeader
	self.Header = new(BinaryRequestHeader)
	requestHeaderSize := unsafe.Sizeof(*self.Header)
	self.request.HeaderBytes = make([]byte, requestHeaderSize)
	if _, err := self.client.socket.Read(self.request.HeaderBytes); err != nil {
		log.Trace("error %v, client: %s read failed, client will exit", err, self.client.socket.RemoteAddr())
		return err
	}
	// cast []byte to requestHeader
	// TODO : set the binary order according to the system
	binary.Read(bytes.NewBuffer(self.request.HeaderBytes), binary.LittleEndian, self.Header)

	// judge magic code
	if int(self.Header.Magic) != DLDB_REQUEST_MAGIC {
		log.Trace("invalid magic code from client %s", self.client.socket.RemoteAddr())
		return InvalidRequestError("magic code is invalid")
	}

	// read body
	self.Body = make([]byte, self.Header.BodyLength)
	if _, err := self.client.socket.Read(self.Body); err != nil {
		log.Trace("error %v, client: %s read failed, client will exit", err, self.client.socket.RemoteAddr())
		return err
	}
	/*nbody, err := self.obstinateRead(self.header.bodyLength, self.body)
	if err != nil {
		log.Trace("error %v, client: %s read failed, client will exit", err, self.socket.RemoteAddr())
		return err
	}*/

	if err := self.isValidRequest(); err != nil {
		return err
	}
	log.Trace("Read request complete")
	return nil

}

func (self *dldbClientRequest) isValidRequest() error {
	log.Trace("Begin valid request")
	// cast []byte to int
	// we could safely tranfer byte to int
	if len(proxyCore.commandTable) < int(self.Header.OpCode) { //opcode not exist in the command table
		log.Trace("invalid opcode parse")
		return InvalidRequestError("op code is not existed")
	} else {
		command := proxyCore.commandTable[int(self.Header.OpCode)]
		// judge argc -1 means variant length ignore it
		if command.argc != -1 && command.argc != int(self.argc) {
			// command argc doesn't match client argc, invalid request
			log.Trace("invalid argc, argc = %d, want %d", self.argc, command.argc)
			return InvalidRequestError(fmt.Sprintf("command argc is invalid, current argc = %d want %d", self.argc, command.argc))
		}
		return nil
	}

}
