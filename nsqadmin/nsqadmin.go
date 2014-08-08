package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/bitly/nsq/util"
)

type NSQAdmin struct {
	opts          *nsqadminOptions
	httpAddr      *net.TCPAddr
	httpListener  net.Listener
	waitGroup     util.WaitGroupWrapper
	notifications chan *AdminAction
	graphiteURL   *url.URL
}

func NewNSQAdmin(opts *nsqadminOptions) *NSQAdmin {
	n := &NSQAdmin{
		opts:          opts,
		notifications: make(chan *AdminAction),
	}

	if len(opts.NSQDHTTPAddresses) == 0 && len(opts.NSQLookupdHTTPAddresses) == 0 {
		n.logf("--nsqd-http-address or --lookupd-http-address required.")
		os.Exit(1)
	}

	if len(opts.NSQDHTTPAddresses) != 0 && len(opts.NSQLookupdHTTPAddresses) != 0 {
		n.logf("use --nsqd-http-address or --lookupd-http-address not both")
		os.Exit(1)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPAddress)
	if err != nil {
		n.logf("FATAL: failed to resolve HTTP address (%s) - %s", opts.HTTPAddress, err)
		os.Exit(1)
	}
	n.httpAddr = httpAddr

	if opts.ProxyGraphite {
		url, err := url.Parse(opts.GraphiteURL)
		if err != nil {
			n.logf("FATAL: failed to parse --graphite-url='%s' - %s", opts.GraphiteURL, err)
			os.Exit(1)
		}
		n.graphiteURL = url
	}

	n.logf(util.Version("nsqlookupd"))

	return n
}

func (n *NSQAdmin) logf(f string, args ...interface{}) {
	if n.opts.Logger == nil {
		return
	}
	n.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (n *NSQAdmin) handleAdminActions() {
	for action := range n.notifications {
		content, err := json.Marshal(action)
		if err != nil {
			n.logf("ERROR: failed to serialize admin action - %s", err)
		}
		httpclient := &http.Client{Transport: util.NewDeadlineTransport(10 * time.Second)}
		n.logf("POSTing notification to %s", *notificationHTTPEndpoint)
		_, err = httpclient.Post(*notificationHTTPEndpoint, "application/json", bytes.NewBuffer(content))
		if err != nil {
			n.logf("ERROR: failed to POST notification - %s", err)
		}
	}
}

func (n *NSQAdmin) Main() {
	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.httpAddr, err)
		os.Exit(1)
	}
	n.httpListener = httpListener
	httpServer := NewHTTPServer(&Context{n})
	n.waitGroup.Wrap(func() {
		util.HTTPServer(n.httpListener, httpServer, n.opts.Logger, "HTTP")
	})
	n.waitGroup.Wrap(func() { n.handleAdminActions() })
}

func (n *NSQAdmin) Exit() {
	n.httpListener.Close()
	close(n.notifications)
	n.waitGroup.Wait()
}
