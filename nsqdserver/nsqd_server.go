package nsqdserver

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/nsqd"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/util"
	"github.com/absolute8511/nsq/internal/version"
)

type NsqdServer struct {
	ctx           *context
	lookupPeers   atomic.Value
	waitGroup     util.WaitGroupWrapper
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	exitChan      chan int
}

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

func buildTLSConfig(opts *nsqd.Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func getIPv4ForInterfaceName(ifname string) string {
	interfaces, _ := net.Interfaces()
	for _, inter := range interfaces {
		nsqd.NsqLogger().Logf("found interface: %s", inter.Name)
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

func NewNsqdServer(nsqdInstance *nsqd.NSQD, opts *nsqd.Options) *NsqdServer {
	s := &NsqdServer{}
	ctx := &context{}
	ctx.nsqd = nsqdInstance
	ip, port, _ := net.SplitHostPort(opts.TCPAddress)
	rpcport := opts.RPCPort
	if rpcport != "" {
		nsqd.NsqLogger().Logf("broadcast option: %s, %s", opts.BroadcastAddress, opts.BroadcastInterface)
		if opts.BroadcastInterface != "" {
			ip = getIPv4ForInterfaceName(opts.BroadcastInterface)
		}
		if ip == "" {
			ip = opts.BroadcastAddress
		} else {
			opts.BroadcastAddress = ip
		}
		if ip == "0.0.0.0" || ip == "" {
			nsqd.NsqLogger().LogErrorf("can not decide the broadcast ip: %v", ip)
			os.Exit(1)
		}
		nsqd.NsqLogger().Logf("Start with broadcast: %s", ip)

		coord := consistence.NewNsqdCoordinator(opts.ClusterID, ip, port, rpcport, strconv.FormatInt(opts.ID, 10), opts.DataPath, nsqdInstance)
		l := consistence.NewNsqdEtcdMgr(opts.ClusterLeadershipAddresses)
		coord.SetLeadershipMgr(l)
		consistence.SetCoordLogger(opts.Logger, opts.LogLevel)
		ctx.nsqdCoord = coord
	} else {
		nsqd.NsqLogger().LogWarningf("Start without nsqd coordinator enabled")
		ctx.nsqdCoord = nil
	}

	s.ctx = ctx

	s.exitChan = make(chan int)

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("FATAL: failed to build TLS config - %s", err)
		os.Exit(1)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		nsqd.NsqLogger().LogErrorf("FATAL: cannot require TLS client connections without TLS key and cert")
		os.Exit(1)
	}
	s.ctx.tlsConfig = tlsConfig

	nsqd.NsqLogger().Logf(version.String("nsqd"))
	nsqd.NsqLogger().Logf("ID: %d", opts.ID)

	return s
}

func (s *NsqdServer) GetNsqdInstance() *nsqd.NSQD {
	return s.ctx.nsqd
}

func (s *NsqdServer) Exit() {
	nsqd.NsqLogger().Logf("nsqd server stopping.")
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}
	if s.httpListener != nil {
		s.httpListener.Close()
	}
	if s.httpsListener != nil {
		s.httpsListener.Close()
	}

	if s.ctx.nsqdCoord != nil {
		s.ctx.nsqdCoord.Stop()
	}

	if s.ctx.nsqd != nil {
		s.ctx.nsqd.Exit()
	}

	close(s.exitChan)
	s.waitGroup.Wait()
	nsqd.NsqLogger().Logf("nsqd server stopped.")
}

func (s *NsqdServer) Main() {
	var httpListener net.Listener
	var httpsListener net.Listener

	if s.ctx.nsqdCoord != nil {
		err := s.ctx.nsqdCoord.Start()
		if err != nil {
			nsqd.NsqLogger().LogErrorf("FATAL: start coordinator failed - %v", err)
			os.Exit(1)
		}
	}

	opts := s.ctx.getOpts()
	tcpListener, err := net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("FATAL: listen (%s) failed - %s", opts.TCPAddress, err)
		os.Exit(1)
	}
	s.tcpListener = tcpListener
	s.ctx.tcpAddr = tcpListener.Addr().(*net.TCPAddr)

	tcpServer := &tcpServer{ctx: s.ctx}
	s.waitGroup.Wrap(func() {
		protocol.TCPServer(s.tcpListener, tcpServer, opts.Logger)
	})

	if s.ctx.GetTlsConfig() != nil && opts.HTTPSAddress != "" {
		httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, s.ctx.GetTlsConfig())
		if err != nil {
			nsqd.NsqLogger().LogErrorf("FATAL: listen (%s) failed - %s", opts.HTTPSAddress, err)
			os.Exit(1)
		}
		s.httpsListener = httpsListener
		httpsServer := newHTTPServer(s.ctx, true, true)
		s.waitGroup.Wrap(func() {
			http_api.Serve(s.httpsListener, httpsServer, "HTTPS", opts.Logger)
		})
	}
	httpListener, err = net.Listen("tcp", opts.HTTPAddress)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("FATAL: listen (%s) failed - %s", opts.HTTPAddress, err)
		os.Exit(1)
	}
	s.httpListener = httpListener
	s.ctx.httpAddr = httpListener.Addr().(*net.TCPAddr)

	httpServer := newHTTPServer(s.ctx, false, opts.TLSRequired == TLSRequired)
	s.waitGroup.Wrap(func() {
		http_api.Serve(s.httpListener, httpServer, "HTTP", opts.Logger)
	})

	s.ctx.nsqd.Start()

	s.waitGroup.Wrap(func() {
		s.lookupLoop(s.ctx.nsqd.MetaNotifyChan, s.ctx.nsqd.OptsNotificationChan, s.exitChan)
	})

	if opts.StatsdAddress != "" {
		s.waitGroup.Wrap(s.statsdLoop)
	}
}
