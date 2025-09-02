package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type Request struct {
	MessageSize int32
	RequestApiKey int16
	RequestApiVersion int16
	CorrelationId int32
	ClientId string // nullable string
}

func ParseBuffer(buffer []byte) Request {
	var r Request
	r.MessageSize = int32(binary.BigEndian.Uint32(buffer[0:4]))
	r.RequestApiKey = int16(binary.BigEndian.Uint16(buffer[4:6]))
	r.RequestApiVersion = int16(binary.BigEndian.Uint16(buffer[6:8]))
	r.CorrelationId = int32(binary.BigEndian.Uint32(buffer[8:12]))

	nullableStringLength := int16(binary.BigEndian.Uint16(buffer[12:14]))

	if nullableStringLength == -1 {
		r.ClientId = ""
	} else {
		r.ClientId = string(buffer[14 : 14+nullableStringLength])
	}

	return r
}

type Response struct {
	MessageSize int32
	CorrelationId int32
}

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	connection, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer connection.Close()

	buffer := make([]byte, 1024)

	for {
		numberOfBytesRead, err := connection.Read(buffer)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			break
		}

		if numberOfBytesRead == 0 {
			break
		}

		request := ParseBuffer(buffer[:numberOfBytesRead])
		response := processRequest(&request)

		_, err = connection.Write(response)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			break
		}
	}
}


func processRequest(request *Request) []byte {
	messageSize := createByteSliceFromInt(0)
	correlationId := createByteSliceFromInt(int(request.CorrelationId))

	return append(messageSize, correlationId...)
}

func createByteSliceFromInt(integer int) []byte {
	arr := make([]byte, 4)
	binary.BigEndian.PutUint32(arr, uint32(integer))
	return arr
}