package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
)

func main() {
	broker := request.NewKafkaBroker()

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
	defer func() {
		if err := connection.Close(); err != nil {
			fmt.Println("Error closing connection:", err)
		}
	}()

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

		response, err := broker.ProcessRequest(buffer[:numberOfBytesRead])
		if err != nil {
			fmt.Println("Error processing request: ", err.Error())

			_, err = connection.Write([]byte(err.Error()))
			if err != nil {
				fmt.Println("Error writing to connection: ", err.Error())
				continue
			}
		}

		_, err = connection.Write(response)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			continue
		}
	}
}
