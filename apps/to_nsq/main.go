// This is an NSQ client that Publishes incoming messages from
// stdin to the specified topic and channel.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
)

var (
	topic            = flag.String("topic", "", "nsq topic")
	scan             = flag.String("scan", "lines", "what from stdin to publish (words,lines,bytes or runes)")
	maxInFlight      = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
	destNsqdTCPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

func main() {

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	flag.Parse()

	if len(*topic) == 0 {
		fatal("Must specify a valid topic")
	}

	wcfg := nsq.NewConfig()
	wcfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", util.BINARY_VERSION, nsq.VERSION)
	wcfg.MaxInFlight = *maxInFlight

	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, wcfg)
		if err != nil {
			fatal("failed creating producer", err)
		}
		producers[addr] = producer
		defer producer.Stop()
	}

	if len(producers) == 0 {
		fatal("Must specify at least one nsqd-tcp-address")
	}

	// scan the input
	s := bufio.NewScanner(os.Stdin)
	s.Split(splitFunc())
	go func() {
		for s.Scan() {
			for _, producer := range producers {
				if err := producer.Publish(*topic, s.Bytes()); err != nil {
					log.Fatalln(err)
				}
			}
		}
	}()

	<-termChan
}

// splitFunc gets the split function to use when scanning
// from the input.
func splitFunc() bufio.SplitFunc {
	switch *scan {
	case "words":
		return bufio.ScanWords
	case "lines":
		return bufio.ScanLines
	case "runes":
		return bufio.ScanRunes
	case "bytes":
		return bufio.ScanBytes
	default:
		fatal("scan must be words, lines, bytes or runes")
	}
	return nil
}

// fatal writes an error and exits
func fatal(args ...interface{}) {
	os.Stderr.WriteString(fmt.Sprint(args...))
	os.Stderr.WriteString("\n")
	flag.PrintDefaults()
	os.Exit(1)
}
