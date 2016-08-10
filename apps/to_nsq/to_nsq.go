// This is an NSQ client that publishes incoming messages from
// stdin to the specified topic.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/app"
	"github.com/absolute8511/nsq/internal/version"
)

var (
	topic         = flag.String("topic", "", "NSQ topic to publish to")
	delimiter     = flag.String("delimiter", "\n", "character to split input from stdin (defaults to '\n')")
	lookupAddress = flag.String("lookupd-http-address", "", "<addr>:<port> to connect to nsqlookupd")

	destNsqdTCPAddrs = app.StringArray{}
)

func init() {
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

func main() {
	cfg := nsq.NewConfig()
	flag.Var(&nsq.ConfigFlag{cfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/absolute8511/go-nsq#Config)")

	flag.Parse()

	if len(*topic) == 0 {
		log.Fatal("--topic required")
	}

	if len(*delimiter) != 1 {
		log.Fatal("--delimiter must be a single byte")
	}

	stopChan := make(chan bool)
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	cfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	// make the producers
	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, cfg)
		if err != nil {
			log.Fatalf("failed to create nsq.Producer - %s", err)
		}
		producers[addr] = producer
	}

	var lookupProducer *nsq.TopicProducerMgr
	if *lookupAddress != "" {
		topics := make([]string, 0)
		topics = append(topics, *topic)
		lookupProducer, _ = nsq.NewTopicProducerMgr(topics, nsq.PubRR, cfg)
		lookupProducer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelDebug)
		err := lookupProducer.ConnectToNSQLookupd(*lookupAddress)
		if err != nil {
			log.Printf("lookup connect error: %v", err)
			return
		}
	}
	if len(producers) == 0 && lookupProducer == nil {
		log.Fatal("--nsqd-tcp-address or --lookup-address required")
	}

	r := bufio.NewReader(os.Stdin)
	delim := (*delimiter)[0]
	go func() {
		for {
			err := readAndPublish(r, delim, producers, lookupProducer)
			if err != nil {
				if err != io.EOF {
					log.Fatal(err)
				}
				close(stopChan)
				break
			}
		}
	}()

	select {
	case <-termChan:
	case <-stopChan:
	}

	for _, producer := range producers {
		producer.Stop()
	}
}

// readAndPublish reads to the delim from r and publishes the bytes
// to the map of producers.
func readAndPublish(r *bufio.Reader, delim byte, producers map[string]*nsq.Producer, lookupProducer *nsq.TopicProducerMgr) error {
	line, readErr := r.ReadBytes(delim)

	if len(line) > 0 {
		// trim the delimiter
		line = line[:len(line)-1]
	}

	if len(line) == 0 {
		return readErr
	}

	if lookupProducer != nil {
		err := lookupProducer.Publish(*topic, line)
		if err != nil {
			return err
		}
	} else {
		for _, producer := range producers {
			err := producer.Publish(*topic, line)
			if err != nil {
				return err
			}
		}
	}

	return readErr
}
