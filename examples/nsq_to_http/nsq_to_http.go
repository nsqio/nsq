// This is a client that writes out to a http endpoint

package main

import (
	"../../nsq"
	"../../util"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"bytes"
	"net/http"
	"net/url"
)

var (
	topic            = flag.String("topic-name", "", "nsq topic")
	channel          = flag.String("channel-name", "nsq_to_file", "nsq channel")
	buffer           = flag.Int("buffer", 200, "number of messages to buffer in channel and disk before sync/ack")
	verbose          = flag.Bool("verbose", false, "verbose logging")
	post             = flag.String("post", "", "Address to make a POST request to. Data will be in the body")
	get              = flag.String("get", "", `Address to make a GET request to.
     Address should be a format string where data can be subbed in`)
	numPublishers    = flag.Int("n", 100, "Number of concurrent publishers")
	nsqAddresses     = util.StringArray{}
	lookupdAddresses = util.StringArray{}
)

func init() {
	flag.Var(&nsqAddresses, "nsq-address", "nsq address (may be given multiple times)")
	flag.Var(&lookupdAddresses, "lookupd-address", "lookupd address (may be given multiple times)")
}

type Publisher interface {
	Publish(string) error
}

type PublisherInfo struct {
	addr string
}

type PublishHandler struct {
	Publisher
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) (err error) {
	return ph.Publish(string(m.Body))
}

// ---------- Post -------------------------

type PostPublisher struct {
	PublisherInfo
}

func (p *PostPublisher) Publish(msg string) error {
	var buffer bytes.Buffer
	buffer.Write([]byte(msg))
	resp, err := http.Post(p.addr, "application/octet-stream", &buffer)
	defer resp.Body.Close()
	defer buffer.Reset()
	return err
}


// ----------- Get ---------------------------

type GetPublisher struct {
	PublisherInfo
}

func (p *GetPublisher) Publish(msg string) error {
	endpoint := fmt.Sprintf(p.addr, url.QueryEscape(msg))
	resp, err := http.Get(endpoint)
	defer resp.Body.Close()
	return err
}


func main() {
	flag.Parse()

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic-name and --channel-name are required")
	}

	if *buffer < 0 {
		log.Fatalf("--buffer must be > 0")
	}

	if len(nsqAddresses) == 0 && len(lookupdAddresses) == 0 {
		log.Fatalf("--nsq-address or --lookupd-address required.")
	}
	if len(nsqAddresses) != 0 && len(lookupdAddresses) != 0 {
		log.Fatalf("use --nsq-address or --lookupd-address not both")
	}

	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	var publisher Publisher
	if len(*post) > 0 {
		publisher = &PostPublisher{PublisherInfo{*post}}
	} else if len(*get) > 0 {
		if strings.Count(*get, "%s") != 1 {
			log.Fatal("Invalid get address - must be a format string")
		}
		publisher = &GetPublisher{PublisherInfo{*get}}
	} else {
		log.Fatal("Need get or post address!")
	}

	r, _ := nsq.NewReader(*topic, *channel)
	r.BufferSize = *buffer
	r.VerboseLogging = *verbose

	for i := 0; i < *numPublishers; i++ {
		r.AddHandler(&PublishHandler{publisher})
	}

	for _, addrString := range nsqAddresses {
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToNSQ(addr)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdAddresses {
		log.Printf("lookupd addr %s", addrString)
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	select {
	case <-r.ExitChan:
		break
	case <-hupChan:
		r.Stop()
		break
	case <-termChan:
		r.Stop()
		break
	}

}
