package nsqlookupd

import (
	"log"
	"net"

	"github.com/bitly/nsq/util"
)

type NSQLookupd struct {
	opts         *nsqlookupdOptions
	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

func NewNSQLookupd(opts *nsqlookupdOptions) *NSQLookupd {
	tcpAddr, err := net.ResolveTCPAddr("tcp", opts.TCPAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	n := &NSQLookupd{
		opts:     opts,
		tcpAddr:  tcpAddr,
		httpAddr: httpAddr,
		DB:       NewRegistrationDB(),
	}

	n.opts.Logger.Output(2, util.Version("nsqlookupd"))

	return n
}

func (l *NSQLookupd) Main() {
	ctx := &Context{l}

	tcpListener, err := net.Listen("tcp", l.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", l.tcpAddr, err.Error())
	}
	l.tcpListener = tcpListener
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		util.TCPServer(tcpListener, tcpServer, l.opts.Logger)
	})

	httpListener, err := net.Listen("tcp", l.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", l.httpAddr, err.Error())
	}
	l.httpListener = httpListener
	httpServer := &httpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		util.HTTPServer(httpListener, httpServer, l.opts.Logger, "HTTP")
	})
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
