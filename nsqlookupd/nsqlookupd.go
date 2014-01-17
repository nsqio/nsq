package nsqlookupd

import (
	"github.com/bitly/nsq/util"
	"log"
	"net"
)

type NSQLookupd struct {
	options      *nsqlookupdOptions
	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

func NewNSQLookupd(options *nsqlookupdOptions) *NSQLookupd {
	tcpAddr, err := net.ResolveTCPAddr("tcp", options.TCPAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	return &NSQLookupd{
		options:  options,
		tcpAddr:  tcpAddr,
		httpAddr: httpAddr,
		DB:       NewRegistrationDB(),
	}
}

func (l *NSQLookupd) Main() {
	context := &Context{l}

	tcpListener, err := net.Listen("tcp", l.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", l.tcpAddr, err.Error())
	}
	l.tcpListener = tcpListener
	tcpServer := &tcpServer{context: context}
	l.waitGroup.Wrap(func() { util.TCPServer(tcpListener, tcpServer) })

	httpListener, err := net.Listen("tcp", l.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", l.httpAddr, err.Error())
	}
	l.httpListener = httpListener
	httpServer := &httpServer{context: context}
	l.waitGroup.Wrap(func() { util.HTTPServer(httpListener, httpServer) })
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
