package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/senarukana/dldb/log"
	"unsafe"
)

type dldbRequest struct {
	client        *dldbClient
	requestBuffer []byte
	header        *BinaryRequestHeader
	body          []byte
	argc          int
	argv          [][]byte
}

func initClientRequest(client *dldbClient) *dldbRequest {
	request := new(dldbRequest)
	request.client = client
	return request
}

// do read until it reads the specified buffer size
/*func (self *dldbRequest) obstinateRead(readSize int, buffer []byte) (int n, err error) {
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

func (self *dldbRequest) readBinaryRequest() error {
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
	if err := self.parseArgv(); err != nil {
		return err
	}
	log.Trace("Read request complete")
	return nil

}

func (self *dldbRequest) isValidRequest() error {
	log.Trace("Begin valid request")
	// cast []byte to int
	// we could safely tranfer byte to int
	if len(dbCore.commandTable) < int(self.header.OpCode) { //opcode not exist in the command table
		log.Trace("invalid opcode parse")
		return InvalidRequestError("op code is not existed")
	} else {
		command := dbCore.commandTable[int(self.header.OpCode)]
		// judge argc -1 means variant length ignore it
		if command.argc != -1 && command.argc != self.argc {
			// command argc doesn't match client argc, invalid request
			log.Trace("invalid argc, argc = %d, want %d", self.argc, command.argc)
			return InvalidRequestError(fmt.Sprintf("command argc is invalid, current argc = %d want %d", self.argc, command.argc))
		}
		return nil
	}

}

func (self *dldbRequest) parseArgv() error {
	log.Trace("Begin parse argv")
	var (
		argvLen uint32
		l       uint32 = 0
	)
	self.argv = make([][]byte, self.argc)
	if self.argc == 1 {
		self.argv[0] = self.body
	} else {
		for i := 0; i < self.argc; i++ {
			argvLen = binary.LittleEndian.Uint32(self.body[l : l+4])
			if argvLen > self.header.BodyLength {
				return InvalidRequestError("invalid argv length")
			}
			self.argv[i] = self.body[l+4 : l+4+argvLen]
			l += 4 + argvLen
		}
	}
	return nil
}

// hash the request key to find the place of the data
// it will change the key to the following format:
// newKey = "partition_" + key
// By doing this, we can easyly find the data that belong to a specific parition
func (self *dldbRequest) makeKey() (string, error) {
	key := self.body[:self.header.KeyLength]
	partition := util.HashFunction(key)
	keyPrefix := []byte(fmt.Sprintf("%d_", partition))
	newKey := append(keyPrefix, key...)

	// place the key
	return nil
}
