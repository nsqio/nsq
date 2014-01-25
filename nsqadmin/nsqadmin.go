package main

import (
	"bytes"
	"encoding/json"
	"github.com/bitly/nsq/util"
	"log"
	"net"
	"net/http"
	"time"
)

type NSQAdmin struct {
	options       *nsqadminOptions
	httpAddr      *net.TCPAddr
	httpListener  net.Listener
	waitGroup     util.WaitGroupWrapper
	notifications chan *AdminAction
}

func NewNSQAdmin(options *nsqadminOptions) *NSQAdmin {
	if len(options.NSQDHTTPAddresses) == 0 && len(options.NSQLookupdHTTPAddresses) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required.")
	}

	if len(options.NSQDHTTPAddresses) != 0 && len(options.NSQLookupdHTTPAddresses) != 0 {
		log.Fatalf("use --nsqd-http-address or --lookupd-http-address not both")
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	return &NSQAdmin{
		options:       options,
		httpAddr:      httpAddr,
		notifications: make(chan *AdminAction),
	}
}

func (n *NSQAdmin) handleAdminActions() {
	for action := range n.notifications {
		content, err := json.Marshal(action)
		if err != nil {
			log.Printf("Error serializing admin action! %s", err)
		}
		httpclient := &http.Client{Transport: util.NewDeadlineTransport(10 * time.Second)}
		log.Printf("Posting notification to %s", *notificationHTTPEndpoint)
		_, err = httpclient.Post(*notificationHTTPEndpoint, "application/json", bytes.NewBuffer(content))
		if err != nil {
			log.Printf("Error posting notification: %s", err)
		}
	}
}

func (n *NSQAdmin) Main() {
	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	httpServer := NewHTTPServer(&Context{n})
	n.waitGroup.Wrap(func() { util.HTTPServer(n.httpListener, httpServer) })
	n.waitGroup.Wrap(func() { n.handleAdminActions() })
}

func (n *NSQAdmin) Exit() {
	n.httpListener.Close()
	close(n.notifications)
	n.waitGroup.Wait()
}
