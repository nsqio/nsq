package main

import (
	"../nsq"
	"log"
	"net"
)

var Protocols = map[int32]nsq.Protocol{}

func tcpClientHandler(clientConn net.Conn) {
	client := nsq.NewServerClient(clientConn)
	log.Printf("TCP: new client(%s)", client.String())
	client.Handle(Protocols)
}
