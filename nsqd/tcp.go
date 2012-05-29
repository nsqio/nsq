package main

import (
	"../nsq"
	"log"
	"net"
)

var Protocols = map[int32]nsq.Protocol{}

func TcpServer(tcpAddr *net.TCPAddr) {
	listener, err := net.Listen("tcp", tcpAddr.String())
	if err != nil {
		panic("listen (" + tcpAddr.String() + ") failed: " + err.Error())
	}
	log.Printf("TCP: listening on %s", tcpAddr.String())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			panic("accept failed: " + err.Error())
		}
		client := nsq.NewServerClient(clientConn, clientConn.RemoteAddr().String())
		log.Printf("TCP: new client(%s)", client.String())
		go client.Handle(Protocols)
	}
}
