package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"unsafe"
)

type DldbClient struct {
	reader         *bufio.Reader
	socket         *net.TCPConn
	argv           [][]byte
	responseHeader *dldbResponseHeader
	responseBody   []byte
	opCode         int
}

func InitDldbClient(address string) *DldbClient {
	client := new(DldbClient)
	client.reader = bufio.NewReader(os.Stdin)
	resolveAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		panic(err)
	}
	client.socket, err = net.DialTCP("tcp", nil, resolveAddr)
	if err != nil {
		panic(err)
	}
	client.socket.SetKeepAlive(true)
	fmt.Println("connection has established!")
	return client
}

func (self *DldbClient) readUserInput() {
	var data []byte
	for {
		fmt.Printf("> ")
		line, _, _ := self.reader.ReadLine()
		data = append(data, line...)
		// "\" means continue
		lastByte := line[len(line)-1]
		if lastByte == '\\' {
			continue
		} else {
			break
		}
	}
	self.argv = bytes.Split(data, []byte{' '})
	fmt.Printf("argv length is %d\n", len(self.argv))
	for i, argv := range self.argv {
		fmt.Printf("Argument %d is : %s\n", i, argv)
	}
}

func (self *DldbClient) Execute() error {
	self.readUserInput()
	if !isValidCommand(self) {
		return errors.New("invalid command")
	}
	self.sendRequest()
	if err := self.receiveResponse(); err != nil {
		return err
	}
	if err := self.handleResponse(); err != nil {
		return err
	}
	return nil
}

func (self *DldbClient) sendRequest() {
	var request []byte
	if len(self.argv) == 1 {
		request = buildRequest(self.opCode, self.argv[0])
	} else {
		request = buildRequestMultiple(self.opCode, self.argv)
	}
	if _, err := self.socket.Write(request); err != nil {
		// give up
		panic(err)
	}
}

/*
type dldbResponseHeader struct {
	magic      byte
	statusCode byte
	argc       byte
	reserved   byte
	bodyLength [4]byte
	// body       []byte
}
*/

func (self *DldbClient) receiveResponse() error {
	fmt.Println("Begin receive response")
	self.responseHeader = new(dldbResponseHeader)
	// read header
	header := make([]byte, unsafe.Sizeof(*self.responseHeader))
	if _, err := self.socket.Read(header); err != nil {
		panic(err)
	}
	// fmt.Println("Begin receive response header complete")
	// cast []byte to response
	binary.Read(bytes.NewBuffer(header), binary.LittleEndian, self.responseHeader)
	// fmt.Printf("receive body length is %d\n", self.responseHeader.BodyLength)

	if self.responseHeader.BodyLength != 0 {
		// read body
		self.responseBody = make([]byte, self.responseHeader.BodyLength)
		if _, err := self.socket.Read(self.responseBody); err != nil {
			panic(err)
		}
		// fmt.Println("receive body complete!!")
	}
	// read complete
	// simple check
	if int(self.responseHeader.Magic) != DLDB_RESPONSR_MAGIC {
		return errors.New("invalid response")
	}
	return nil
}

func (self *DldbClient) handleResponse() error {
	switch self.responseHeader.StatusCode {
	case CODE_ERROR:
		fmt.Printf("-Error, %v\n", self.responseBody)
		return nil
	case CODE_SUCCESS:
		fmt.Println("+SUCESS")
	case CODE_OK:
		if self.responseHeader.Argc == 1 {
			fmt.Printf("+OK %s\n", self.responseBody)
			return nil
		} else {
			//multiple argv
			fmt.Printf("+OK Return %d Arguments\n", len(self.argv))
			var j uint32 = 0
			for i := 0; i < int(self.responseHeader.Argc); i++ {
				argvLen := binary.LittleEndian.Uint32(self.responseBody[j : j+4])
				if argvLen > self.responseHeader.BodyLength {
					return errors.New("invalid response")
				}
				j += 4
				self.argv[i] = self.responseBody[j : j+argvLen]
				fmt.Printf("%s", self.argv[i])
				j += argvLen
			}
			return nil
		}
	case CODE_EMPTY:
		fmt.Println("+OK nil")
		return nil
	}
	return nil
}
