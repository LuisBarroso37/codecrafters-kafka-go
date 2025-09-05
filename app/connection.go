package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
)

func listenForConnections(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(connection)
	}
}

func handleConnection(connection net.Conn) {
	buffer := make([]byte, 1024)
	broker := request.NewKafkaBroker()

	defer func() {
		if err := connection.Close(); err != nil {
			fmt.Println("Error closing connection:", err)
		}
	}()

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
