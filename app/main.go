package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type RequestParseError struct {
	Code    int
	Message string
}

func (e *RequestParseError) Error() string {
    return e.Message
}

type Request struct {
	MessageSize int32
	RequestApiKey int16
	RequestApiVersion int16
	CorrelationId int32
	ClientId string // nullable string
}

func ParseBuffer(buffer []byte) (Request, error) {
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

	if r.RequestApiVersion < 0 || r.RequestApiVersion > 4 {
        return r, &RequestParseError{Code: 35, Message: "UNSUPPORTED_VERSION"}
    }

    return r, nil
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

		request, err := ParseBuffer(buffer[:numberOfBytesRead])
		if err != nil {
			fmt.Println("Error parsing request: ", err.Error())
		}

		response := processRequest(&request, err)

		_, err = connection.Write(response)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			continue
		}
	}
}


func processRequest(request *Request, err error) []byte {
	buffer := make([]byte, 10)

	binary.BigEndian.PutUint32(buffer[0:4], uint32(0))
	binary.BigEndian.PutUint32(buffer[4:8], uint32(request.CorrelationId))

    if err != nil {
		var code int

        if parseErr, ok := err.(*RequestParseError); ok {
            code = parseErr.Code
        } else {
            code = -1 // unknown error
        }

		fmt.Printf("Error Code: %d\n", code)
		binary.BigEndian.PutUint16(buffer[8:10], uint16(code))
    } else {
		binary.BigEndian.PutUint16(buffer[8:10], uint16(0))
	}

	return buffer
}