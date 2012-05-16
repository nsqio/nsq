package main

import (
	"log"
	"net"
)

func tcpServer(address string, port string) {
	fqAddress := address + ":" + port
	listener, err := net.Listen("tcp", fqAddress)
	if err != nil {
		panic("listen (" + fqAddress + ") failed: " + err.Error())
	}
	log.Printf("listening for clients on %s", fqAddress)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			panic("accept failed: " + err.Error())
		}
		client := NewClient(clientConn)
		log.Printf("NSQ: new client(%s)", client.String())
		go client.Handle()
	}
}
