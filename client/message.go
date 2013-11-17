package client

import (
	"encoding/binary"
	"unsafe"
)

type dldbRequestHeader struct {
	magic      byte
	opCode     byte
	options    byte
	argc       byte
	reserved   [4]byte
	bodyLength uint32
}

type dldbResponseHeader struct {
	Magic      byte
	StatusCode byte
	Argc       byte
	Reserved   byte
	BodyLength uint32
	// body       []byte
}

const (
	DLDB_REQUEST_MAGIC  = 99
	DLDB_RESPONSR_MAGIC = 98
)

func buildRequestHeader(opCode int, argc int) []byte {
	var h dldbRequestHeader
	header := make([]byte, unsafe.Sizeof(h))
	header[0] = DLDB_REQUEST_MAGIC
	header[1] = byte(opCode % 128)
	header[3] = byte(argc % 128)
	return header
}

func buildRequest(opCode int, argv []byte) []byte {
	request := buildRequestHeader(opCode, 1)
	binary.LittleEndian.PutUint32(request[8:], uint32(len(argv)))
	// append body
	request = append(request, argv...)
	return request
}

func buildRequestMultiple(opCode int, argv [][]byte) []byte {
	argc := len(argv)
	request := buildRequestHeader(opCode, argc)
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
