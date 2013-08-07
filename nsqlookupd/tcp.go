package main

import (
	"github.com/bitly/nsq/util"
	"io"
	"log"
	"net"
)

type tcpServer struct {
	context *Context
}

func (p *tcpServer) Handle(clientConn net.Conn) {
	log.Printf("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		log.Printf("ERROR: failed to read protocol version - %s", err.Error())
		return
	}
	protocolMagic := string(buf)

	log.Printf("CLIENT(%s): desired protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)

	var prot util.Protocol
	switch protocolMagic {
	case "  V1":
		prot = &LookupProtocolV1{context: p.context}
	default:
		util.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		log.Printf("ERROR: client(%s) bad protocol magic '%s'", clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err.Error())
		return
	}
}
