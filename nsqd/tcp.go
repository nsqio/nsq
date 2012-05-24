package main

import (
	"../nsq"
	"log"
	"net"
)

var Protocols = map[int32]nsq.Protocol{}

func TcpServer(address string, port string) {
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
		client := nsq.NewServerClient(clientConn, clientConn.RemoteAddr().String())
		log.Printf("NSQ: new client(%s)", client.String())
		go client.Handle(Protocols)
	}
}
