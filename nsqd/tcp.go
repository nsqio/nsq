package main

import (
	"../nsq"
	"log"
	"net"
)

type ClientInterface interface {
	TimedOutMessage()
	Stats() ClientStats
}

var Protocols = map[int32]nsq.Protocol{}

func tcpClientHandler(clientConn net.Conn) {
	client := nsq.NewServerClient(clientConn)
	log.Printf("TCP: new client(%s)", client.String())
	client.Handle(Protocols)
}
