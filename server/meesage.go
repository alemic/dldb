package server

import (
	"encoding/binary"
	"unsafe"
)

/*
-----------------------Binary Protocol-----------------------------------
Message header
      Byte/     0       |       1       |       2       |       3       |
         /              |               |               |               |
        |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
        +---------------+---------------+---------------+---------------+
       0| Magic         | Opcode        |    Node id    |    Argc       |
        +---------------+---------------+---------------+---------------+
       4|        Options                |            Reserved           |
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
       0| Magic         | Status Code   |     Argc      |
        +---------------+---------------+---------------+---------------+
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
	Options    byte
	Argc       byte
	Reserved   [4]byte
	BodyLength uint32
}

type BinaryResponse struct {
	magic      byte
	statusCode byte
	argc       byte
	reserved   byte
	bodyLength [4]byte
	// body       []byte
}

type dldbResponse struct {
	client   *dldbClient
	response []byte
}

//
func buildResponseHeader(client *dldbClient, argc int, statusCode int) *dldbResponse {
	r := new(dldbResponse)
	r.client = client
	var h BinaryResponse
	r.response = make([]byte, unsafe.Sizeof(h))
	r.response[0] = DLDB_RESPONSR_MAGIC    //magic number
	r.response[1] = byte(statusCode % 127) //statusCode
	r.response[2] = byte(argc % 127)       //argc
	return r
}

func buildErrorResponseMessage(client *dldbClient, err error) *dldbResponse {
	r := buildResponseHeader(client, 1, 0)
	binary.LittleEndian.PutUint32(r.response[4:], uint32(len(err.Error())))
	r.response = append(r.response, []byte(err.Error())...)
	return r
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

func buildSuccessResponseMessage(client *dldbClient) *dldbResponse {
	return buildResponseHeader(client, 1, CODE_SUCCESS)
}

func buildEmptyResponseMessage(client *dldbClient) *dldbResponse {
	return buildResponseHeader(client, 1, CODE_EMPTY)
}

func buildResponseMessage(client *dldbClient, argv []byte) *dldbResponse {
	r := buildResponseHeader(client, 1, CODE_OK)
	binary.LittleEndian.PutUint32(r.response[4:], uint32(len(argv))) //body length
	r.response = append(r.response, argv...)
	return r
	/*response := new(BinaryResponse)
	response.statusCode = 0
	response.argc = 1
	response.magic = DLDB_REQUEST_MAGIC
	totalLen := 0
	for i := 0; i < argc; i++ {
		// cast int to []byte
		lengthBuf := bytes.NewBuffer([]byte{})
		binary.Write(lengthBuf, binary.LittleEndian, int32(len(argv[i])))
		append(response.body, lengthBuf.Bytes()...)
		append(response.body, argv[i])
		totalLen += 4 + len(argv[i]) // 4 for int32 length
	}
	response.bodyLength = totalLen
	return response*/
}

func buildMultiResponseMessage(client *dldbClient, argc int, argv [][]byte) *dldbResponse {
	r := buildResponseHeader(client, argc, CODE_OK)
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
	/*response := new(BinaryResponse)
	response.statusCode = 0
	response.argc = 1
	response.magic = DLDB_REQUEST_MAGIC
	totalLen := 0
	for i := 0; i < argc; i++ {
		// cast int to []byte
		lengthBuf := bytes.NewBuffer([]byte{})
		binary.Write(lengthBuf, binary.LittleEndian, int32(len(argv[i])))
		append(response.body, lengthBuf.Bytes()...)
		append(response.body, argv[i])
		totalLen += 4 + len(argv[i]) // 4 for int32 length
	}
	response.bodyLength = totalLen
	return response*/
}
