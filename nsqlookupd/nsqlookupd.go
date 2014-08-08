package nsqlookupd

import (
	"fmt"
	"net"
	"os"

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
	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", opts.TCPAddress)
	if err != nil {
		n.logf("FATAL: failed to resolve TCP address (%s) - %s", opts.TCPAddress, err)
		os.Exit(1)
	}
	n.tcpAddr = tcpAddr

	httpAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPAddress)
	if err != nil {
		n.logf("FATAL: failed to resolve HTTP address (%s) - %s", opts.HTTPAddress, err)
		os.Exit(1)
	}
	n.httpAddr = httpAddr

	n.logf(util.Version("nsqlookupd"))

	return n
}

func (n *NSQLookupd) logf(f string, args ...interface{}) {
	if n.opts.Logger == nil {
		return
	}
	n.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (l *NSQLookupd) Main() {
	ctx := &Context{l}

	tcpListener, err := net.Listen("tcp", l.tcpAddr.String())
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.tcpAddr, err)
		os.Exit(1)
	}
	l.tcpListener = tcpListener
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		util.TCPServer(tcpListener, tcpServer, l.opts.Logger)
	})

	httpListener, err := net.Listen("tcp", l.httpAddr.String())
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.httpAddr, err)
		os.Exit(1)
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
