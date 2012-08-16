// This is a client that writes out to a http endpoint

package main

import (
	"../../nsq"
	"../../util"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

const (
	ModeAll = iota
	ModeRoundRobin
)

var (
	topic            = flag.String("topic-name", "", "nsq topic")
	channel          = flag.String("channel-name", "nsq_to_file", "nsq channel")
	buffer           = flag.Int("buffer", 200, "number of messages to allow in flight")
	verbose          = flag.Bool("verbose", false, "enable verbose logging")
	numPublishers    = flag.Int("n", 100, "number of concurrent publishers")
	roundRobin       = flag.Bool("round-robin", false, "enable round robin mode")
	getAddresses     = util.StringArray{}
	postAddresses    = util.StringArray{}
	nsqAddresses     = util.StringArray{}
	lookupdAddresses = util.StringArray{}
)

func init() {
	flag.Var(&postAddresses, "post", "HTTP address to make a POST request to.  data will be in the body (may be given multiple times)")
	flag.Var(&getAddresses, "get", "HTTP address to make a GET request to. '%s' will be printf replaced with data (may be given multiple times)")
	flag.Var(&nsqAddresses, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type Publisher interface {
	Publish(string, []byte) error
}

type PublishHandler struct {
	Publisher
	addresses util.StringArray
	counter   uint64
	mode      int
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	switch ph.mode {
	case ModeAll:
		for _, addr := range ph.addresses {
			err := ph.Publish(addr, m.Body)
			if err != nil {
				return err
			}
		}
	case ModeRoundRobin:
		idx := ph.counter % uint64(len(ph.addresses))
		err := ph.Publish(ph.addresses[idx], m.Body)
		if err != nil {
			return err
		}
		ph.counter++
	}

	return nil
}

type PostPublisher struct{}

func (p *PostPublisher) Publish(addr string, msg []byte) error {
	reader := bytes.NewReader(msg)
	resp, err := http.Post(addr, "application/octet-stream", reader)
	resp.Body.Close()
	return err
}

type GetPublisher struct{}

func (p *GetPublisher) Publish(addr string, msg []byte) error {
	endpoint := fmt.Sprintf(addr, url.QueryEscape(string(msg)))
	resp, err := http.Get(endpoint)
	resp.Body.Close()
	return err
}

func main() {
	var publisher Publisher
	var addresses util.StringArray
	var mode int

	flag.Parse()

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic-name and --channel-name are required")
	}

	if *buffer < 0 {
		log.Fatalf("--buffer must be > 0")
	}

	if len(nsqAddresses) == 0 && len(lookupdAddresses) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqAddresses) > 0 && len(lookupdAddresses) > 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(getAddresses) == 0 && len(postAddresses) == 0 {
		log.Fatalf("--get or --post required")
	}
	if len(getAddresses) > 0 && len(postAddresses) > 0 {
		log.Fatalf("use --get or --post not both")
	}
	if len(getAddresses) > 0 {
		for _, get := range getAddresses {
			if strings.Count(get, "%s") != 1 {
				log.Fatal("invalid GET address - must be a printf string")
			}
		}
	}

	if *roundRobin {
		mode = ModeRoundRobin
	}

	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	if len(postAddresses) > 0 {
		publisher = &PostPublisher{}
		addresses = postAddresses
	} else {
		publisher = &GetPublisher{}
		addresses = getAddresses
	}

	r, _ := nsq.NewReader(*topic, *channel)
	r.BufferSize = *buffer
	r.VerboseLogging = *verbose

	for i := 0; i < *numPublishers; i++ {
		handler := &PublishHandler{
			Publisher: publisher,
			addresses: addresses,
			mode:      mode,
		}
		r.AddHandler(handler)
	}

	for _, addrString := range nsqAddresses {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdAddresses {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	select {
	case <-r.ExitChan:
	case <-hupChan:
		r.Stop()
	case <-termChan:
		r.Stop()
	}
}
