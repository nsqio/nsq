package util

import (
	"log"
	"net"
)

func TcpServer(listener net.Listener, handler func(net.Conn) error) {
	log.Printf("TCP: listening on %s", listener.Addr().String())

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				log.Printf("NOTICE: temporary Accept() failure - %s", err.Error())
				continue
			}
			log.Printf("ERROR: listener.Accept() - %s", err.Error())
			break
		}
		go handler(clientConn)
	}
}
