package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	channel       = flag.String("channel", "", "NSQ channel")
	maxInFlight   = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	totalMessages = flag.Int("n", 0, "total messages to show (will wait if starved)")
	printTopic    = flag.Bool("print-topic", false, "print topic name where message was received")
	writeToFiles  = flag.Bool("write-to-files", false, "prints messages to file - one file per message")
	filePrefix    = flag.String("file-prefix", "message", "filename prefix when printing to file")

	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
	topics           = app.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "NSQ topic (may be given multiple times)")
}

type TailHandler struct {
	topicName     string
	totalMessages int
	messagesShown int
}

func (th *TailHandler) writeMessageToFile(message []byte) error {
	f, err := os.OpenFile(fmt.Sprintf("%s-%d.bin", *filePrefix, th.messagesShown), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("ERROR: failed to open file to write message to - %s", err)
	}

	defer f.Close()

	_, err = f.Write(message)
	if err != nil {
		log.Fatalf("ERROR: failed to write to file - %s", err)
	}
	return nil
}

func (th *TailHandler) HandleMessage(m *nsq.Message) error {
	th.messagesShown++

	if *writeToFiles {
		err := th.writeMessageToFile(m.Body)
		if err != nil {
			log.Fatalf("ERROR: failed to write message to file - %s", err)
		}
		if th.totalMessages > 0 && th.messagesShown >= th.totalMessages {
			os.Exit(0)
		}
		return nil
	}

	if *printTopic {
		_, err := os.Stdout.WriteString(th.topicName)
		if err != nil {
			log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
		}
		_, err = os.Stdout.WriteString(" | ")
		if err != nil {
			log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
		}
	}

	_, err := os.Stdout.Write(m.Body)
	if err != nil {
		log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
	}
	_, err = os.Stdout.WriteString("\n")
	if err != nil {
		log.Fatalf("ERROR: failed to write to os.Stdout - %s", err)
	}
	if th.totalMessages > 0 && th.messagesShown >= th.totalMessages {
		os.Exit(0)
	}
	return nil
}

func main() {
	cfg := nsq.NewConfig()

	flag.Var(&nsq.ConfigFlag{cfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Parse()

	if *writeToFiles && *totalMessages == 0 {
		log.Println("WARNING: writing messages to file but NO limit on message count to process")
	}

	if *showVersion {
		fmt.Printf("nsq_tail v%s\n", version.Binary)
		return
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	if len(topics) == 0 {
		log.Fatal("--topic required")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Don't ask for more messages than we want
	if *totalMessages > 0 && *totalMessages < *maxInFlight {
		*maxInFlight = *totalMessages
	}

	cfg.UserAgent = fmt.Sprintf("nsq_tail/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	consumers := []*nsq.Consumer{}
	for i := 0; i < len(topics); i += 1 {
		log.Printf("Adding consumer for topic: %s\n", topics[i])

		consumer, err := nsq.NewConsumer(topics[i], *channel, cfg)
		if err != nil {
			log.Fatal(err)
		}

		consumer.AddHandler(&TailHandler{topicName: topics[i], totalMessages: *totalMessages})

		err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
		if err != nil {
			log.Fatal(err)
		}

		err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
		if err != nil {
			log.Fatal(err)
		}

		consumers = append(consumers, consumer)
	}

	<-sigChan

	for _, consumer := range consumers {
		consumer.Stop()
	}
	for _, consumer := range consumers {
		<-consumer.StopChan
	}
}
