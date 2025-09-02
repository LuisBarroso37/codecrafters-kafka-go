package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

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

		response := processMessage(buffer[:numberOfBytesRead])
		connection.Write(response)

		fmt.Println("Received message: ", string(buffer))
	}
}


func processMessage(message []byte) []byte {
	messageSize := createByteSliceFromInt(0)
	correlationId := createByteSliceFromInt(7)
	fmt.Printf("Processing message: %s\n", string(message))
	return append(messageSize, correlationId...)
}

func createByteSliceFromInt(integer int) []byte {
	arr := make([]byte, 4)
	binary.BigEndian.PutUint32(arr, uint32(integer))
	return arr
}