package nsqlookupd

import (
	"github.com/absolute8511/nsq/consistence"
	"net"
	"os"
	"sync"

	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/util"
	"github.com/absolute8511/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
	coordinator  *consistence.NsqLookupCoordinator
}

func New(opts *Options) *NSQLookupd {
	nsqlookupLog.Logger = opts.Logger
	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	nsqlookupLog.Logf(version.String("nsqlookupd"))
	return n
}

func (l *NSQLookupd) Main() {
	ctx := &Context{l}

	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		nsqlookupLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.tcpListener = tcpListener
	l.Unlock()
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)
	})

	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		nsqlookupLog.LogErrorf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
		os.Exit(1)
	}

	var node consistence.NsqLookupdNodeInfo
	node.NodeIp, node.HttpPort, _ = net.SplitHostPort(l.opts.HTTPAddress)
	if l.opts.RPCAddress != "" {
		_, node.RpcPort, _ = net.SplitHostPort(l.opts.RPCAddress)
		node.ID = net.JoinHostPort(l.opts.BroadcastAddress, node.RpcPort)

		l.coordinator = consistence.NewNsqLookupCoordinator("cluster-id", &node)
		// set etcd leader manager here
		// l.coordinator.SetLeadershipMgr(nil)
		err = l.coordinator.Start()
		if err != nil {
			nsqlookupLog.LogErrorf("FATAL: start coordinator failed - %s", err)
			os.Exit(1)
		}
	} else {
		nsqlookupLog.Logf("lookup start without the coordinator enabled.")
		l.coordinator = nil
	}
	l.Lock()
	l.httpListener = httpListener
	l.Unlock()
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	})
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
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
	if l.coordinator != nil {
		l.coordinator.Stop()
	}
}
