package proxy

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"github.com/senarukana/dldb/balancer"
	"net"
	"unsafe"
)

/*
-----------------------Binary Protocol-----------------------------------
Request header
      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0|     Magic     |    Opcode     |            Key Length         |
        +---------------+---------------+---------------+---------------+
       4|        Options                |            Argc               |
       +---------------+----------------+---------------+---------------+
       8| Total body length                                             |
        +---------------+---------------+---------------+---------------+
        Total 12 bytes
        Max Argv Num is 2 ^ 8 -1 = 255
        Max data size is 2^32 -1 = 2MB

Response header


     Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Magic         | Status Code   |            Reserved           |
        +---------------+---------------+---------------+---------------+
       4|        Batch Num              |            Argc               |
       +---------------+----------------+---------------+---------------+
       4| Total body length                                             |
        +---------------+---------------+---------------+---------------+
        Total 8 bytes
*/

const (
	DLDB_REQUEST_MAGIC  = 99
	DLDB_RESPONSR_MAGIC = 98
)

type BinaryRequestHeader struct {
	Magic      byte
	OpCode     byte
	KeyLength  [2]byte
	Options    [2]byte
	Argc       [2]byte
	BodyLength uint32
}

type BinaryRequest struct {
	Header      *BinaryRequestHeader
	HeaderBytes []byte
	Body        []byte
}

type BinaryResponseHeader struct {
	Magic      byte
	StatusCode byte
	Reserved   [2]byte
	BatchNum   [2]byte
	Argc       [2]byte
	BodyLength uint32
	// body       []byte
}

type BinaryResponse struct {
	Header BinaryResponseHeader
	Body   []byte
}

func buildRequestHeader(opCode int, keyLength int, argc int) []byte {
	var h BinaryRequestHeader
	header := make([]byte, unsafe.Sizeof(h))
	header[0] = DLDB_REQUEST_MAGIC
	header[1] = byte(opCode % 128)
	if keyLength != 0 {
		binary.LittleEndian.PutUint16(header[2:], uint16(keyLength))
	}
	if argc != 0 {
		binary.LittleEndian.PutUint16(header[6:], uint16(argc))
	}
	return header
}

func buildRequest(opCode int, keyLength int, argv []byte) []byte {
	request := buildRequestHeader(opCode, keyLength, 1)
	binary.LittleEndian.PutUint32(request[8:], uint32(len(argv)))
	// append body
	request = append(request, argv...)
	return request
}

func buildRequestMultiple(opCode int, keyLength int, argv [][]byte) []byte {
	argc := len(argv)
	request := buildRequestHeader(opCode, keyLength, argc)
	var bodyLength uint32 = 0
	for i := 0; i < argc; i++ {
		bufInt := make([]byte, 4)
		binary.LittleEndian.PutUint32(bufInt, uint32(len(argv[i])))
		request = append(request, bufInt...)
		request = append(request, argv[i]...)
		bodyLength += 4 + uint32(len(argv[i]))
	}
	binary.LittleEndian.PutUint32(request[8:], bodyLength)
	return request
}

func buildBatchServerRequest(requests []*balancer.IdentifierRequest) []byte {
	batchRequest := buildRequestHeader(COMMAND_BATCH, 0, len(requests)) // if op is batch ,argc means batch num
	var h BinaryRequestHeader
	headerLen := unsafe.Sizeof(h)
	totalLength := 0
	for _, request := range requests {
		dataServerRequest := request.Request.(*dldbDataServerRequest)
		clientRequest := append(dataServerRequest.clientRequest.HeaderBytes, dataServerRequest.clientRequest.Body...)
		batchRequest = append(batchRequest, clientRequest...)
		totalLength += len(clientRequest)
	}
	binary.LittleEndian.PutUint32(batchRequest[8:], totalLength)
	return batchRequest
}

/*
func (self *Ring) GobEncode() ([]byte, error) {
	w := new(bytes.Buffer)
	encoder := gob.NewEncoder(w)
	err := encoder.Encode(self.part2repli2devHost)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(self.hashShift)
	if err != nil {
		return nil, err
	}
	err = encoder.Encode(self.versions)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (self *BinaryRequestHeader) GobDecode(buf []byte) error {
	r := bytes.NewBuffer(buf)
	decoder := gob.NewDecoder(r)
	err := decoder.Decode(&self.Magic)
	if err != nil {
		return err
	}
	err = decoder.Decode(&self.OpCode)
	if err != nil {
		return err
	}
	err = decoder.Decode(&self.OpCode)
	if err != nil {
		return err
	}
	err = decoder.Decode(&self.OpCode)
	if err != nil {
		return err
	}
	err = decoder.Decode(&self.OpCode)
	if err != nil {
		return err
	}
	return decoder.Decode(&self.versions)
}
*/

// used to send data to client_send_handler
type dldbClientResponse struct {
	client   *dldbClient
	response *BinaryResponse
}

func initClientResponse(client *dldbClient, response *BinaryResponse) *dldbClientResponse {
	r := new(dldbClientResponse)
	r.client = client
	r.response = response
	return r
}

//
func buildResponseHeader(argc int, batchNum int, statusCode int) []byte {
	var h BinaryResponseHeader
	response := make([]byte, unsafe.Sizeof(h))
	response[0] = DLDB_RESPONSR_MAGIC    //magic number
	response[1] = byte(statusCode % 127) //statusCode

	binary.LittleEndian.PutUint16(response[4:], batchNum) // batch num
	binary.LittleEndian.PutUint16(response[6:], argc)     //argc
	return r
}

func buildErrorResponseMessage(err error) []byte {
	response := buildResponseHeader(1, 0, 0)
	binary.LittleEndian.PutUint32(r.response[4:], uint32(len(err.Error())))
	response = append(response, []byte(err.Error())...)
	return response
	/*
		response := new(BinaryResponse)

		response.statusCode = -1
		response.argc = 1
		response.magic = DLDB_REQUEST_MAGIC
		response.bodyLength = len(err.Error()) + 4 //int32
		// cast int to []byte
		lengthBuf := bytes.NewBuffer([]bytes{})
		//TODO binary order according to the system, now it assumes the client is x86 system
		binary.Write(lengthBuf, binary.LittleEndian, int32(len(err.Error())))
		append(response.body, lengthBuf.Bytes()...)
		append(response.body, err.Error())*/
}

func buildSuccessResponseMessage() *dldbResponse {
	return buildResponseHeader(1, 0, CODE_SUCCESS)
}

func buildEmptyResponseMessage() *dldbResponse {
	return buildResponseHeader(1, 0, CODE_EMPTY)
}

func buildResponseMessage(argv []byte) *dldbResponse {
	r := buildResponseHeader(1, 0, CODE_OK)
	binary.LittleEndian.PutUint32(r.response[4:], uint32(len(argv))) //body length
	r.response = append(r.response, argv...)
	return r
}

func buildMultiResponseMessage(argc int, argv [][]byte) *dldbResponse {
	r := buildResponseHeader(argc, 0, CODE_OK)
	var bodyLength uint32 = 0
	for i := 0; i < argc; i++ {
		bufInt := make([]byte, 4)
		binary.LittleEndian.PutUint32(bufInt, uint32(len(argv[i])))
		r.response = append(r.response, bufInt...)
		r.response = append(r.response, argv[i]...)
		bodyLength += 4 + uint32(len(argv[i]))
	}
	binary.LittleEndian.PutUint32(r.response[4:], bodyLength) //body length
	return r
}

// func buildBatchResponseMessage()
