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
	nsqlookupLog.SetLevel(opts.LogLevel)
	consistence.SetCoordLogger(opts.Logger, opts.LogLevel)

	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	nsqlookupLog.Logf(version.String("nsqlookupd"))
	return n
}

func getIPv4ForInterfaceName(ifname string) string {
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		nsqlookupLog.Logf("found interface: %s", inter.Name)
		if inter.Name == ifname {
			if addrs, err := inter.Addrs(); err == nil {
				for _, addr := range addrs {
					switch ip := addr.(type) {
					case *net.IPNet:
						if ip.IP.DefaultMask() != nil {
							return ip.IP.String()
						}
					}
				}
			}
		}
	}
	return ""
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
	node.NodeIp, node.TcpPort, _ = net.SplitHostPort(l.opts.TCPAddress)
	if l.opts.RPCPort != "" {
		nsqlookupLog.Logf("broadcast option: %s, %s", l.opts.BroadcastAddress, l.opts.BroadcastInterface)
		if l.opts.BroadcastInterface != "" {
			node.NodeIp = getIPv4ForInterfaceName(l.opts.BroadcastInterface)
		}
		if node.NodeIp == "" {
			node.NodeIp = l.opts.BroadcastAddress
		} else {
			l.opts.BroadcastAddress = node.NodeIp
		}
		if node.NodeIp == "0.0.0.0" || node.NodeIp == "" {
			nsqlookupLog.LogErrorf("can not decide the broadcast ip: %v", node.NodeIp)
			os.Exit(1)
		}
		nsqlookupLog.Logf("Start with broadcast ip:%s", node.NodeIp)
		node.RpcPort = l.opts.RPCPort
		node.ID = consistence.GenNsqLookupNodeID(&node, "nsqlookup")

		l.coordinator = consistence.NewNsqLookupCoordinator(l.opts.ClusterID, &node)
		// set etcd leader manager here
		leadership := consistence.NewNsqLookupdEtcdMgr(l.opts.ClusterLeadershipAddresses)
		l.coordinator.SetLeadershipMgr(leadership)
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
