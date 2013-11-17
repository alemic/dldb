package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/senarukana/dldb/log"
	"github.com/senarukana/dldb/util"
	"unsafe"
)

type dldbClientRequest struct {
	client *dldbClient
	header *BinaryRequestHeader
	body   []byte
	host   string //request direct to
}

func initClientRequest(client *dldbClient) *dldbClientRequest {
	request := new(dldbClientRequest)
	request.client = client
	return request
}

func (self *dldbClientRequest) readBinaryRequest() error {
	log.Trace("Begin read request")
	// read the msgHeader
	self.header = new(BinaryRequestHeader)
	requestHeaderSize := unsafe.Sizeof(*self.header)
	headBuffer := make([]byte, requestHeaderSize)
	if _, err := self.client.socket.Read(headBuffer); err != nil {
		log.Trace("error %v, client: %s read failed, client will exit", err, self.client.socket.RemoteAddr())
		return err
	}
	// cast []byte to requestHeader
	// TODO : set the binary order according to the system
	binary.Read(bytes.NewBuffer(headBuffer), binary.LittleEndian, self.header)

	// judge magic code
	if int(self.header.Magic) != DLDB_REQUEST_MAGIC {
		log.Trace("invalid magic code from client %s", self.client.socket.RemoteAddr())
		return InvalidRequestError("magic code is invalid")
	}
	self.argc = int(self.header.Argc)

	// read body
	self.body = make([]byte, self.header.BodyLength)
	if _, err := self.client.socket.Read(self.body); err != nil {
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

	// parse body according to the opCode
	if err := self.makeKey(); err != nil {
		return err
	}
	log.Trace("Read request complete")
	return nil

}

func (self *dldbClientRequest) isValidRequest() error {
	log.Trace("Begin valid request")
	// cast []byte to int
	// we could safely tranfer byte to int
	if len(proxyCore.commandTable) < int(self.header.OpCode) { //opcode not exist in the command table
		log.Trace("invalid opcode parse")
		return InvalidRequestError("op code is not existed")
	} else {
		command := proxyCore.commandTable[int(self.header.OpCode)]
		// judge argc -1 means variant length ignore it
		if command.argc != -1 && command.argc != self.argc {
			// command argc doesn't match client argc, invalid request
			log.Trace("invalid argc, argc = %d, want %d", self.argc, command.argc)
			return InvalidRequestError(fmt.Sprintf("command argc is invalid, current argc = %d want %d", self.argc, command.argc))
		}
		return nil
	}

}
