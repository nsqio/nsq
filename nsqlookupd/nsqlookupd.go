package nsqlookupd

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

func New(opts *Options) (*NSQLookupd, error) {
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}

	l.logf(LOG_INFO, version.String("nsqlookupd"))

	return l, nil
}

// Main starts an instance of nsqlookupd and returns an
// error if there was a problem starting up.
func (l *NSQLookupd) Main() error {
	var err error
	ctx := &Context{l}

	l.tcpListener, err = net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		return fmt.Errorf("listen (%s) failed - %s", l.opts.TCPAddress, err)
	}
	l.httpListener, err = net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		return fmt.Errorf("listen (%s) failed - %s", l.opts.TCPAddress, err)
	}

	exitCh := make(chan error)
	var once sync.Once
	exitFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				l.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		exitFunc(protocol.TCPServer(l.tcpListener, tcpServer, l.logf))
	})
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		exitFunc(http_api.Serve(l.httpListener, httpServer, "HTTP", l.logf))
	})

	err = <-exitCh
	return err
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
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
