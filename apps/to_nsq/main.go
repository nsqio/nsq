// This is an NSQ client that Publishes incoming messages from
// stdin to the specified topic and channel.

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

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
)

var (
	topic            = flag.String("topic", "", "nsq topic")
	delimiter        = flag.String("delimiter", "\n", "what from stdin to split on")
	maxInFlight      = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	destNsqdTCPAddrs = util.StringArray{}
	readerOpts       = util.StringArray{}
)

func init() {
	flag.Var(&readerOpts, "reader-opt", "option to passthrough to nsq.Consumer (may be given multiple times)")
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

func main() {

	// handle stopping
	stopChan := make(chan bool)
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// parse flags
	flag.Parse()
	if len(*topic) == 0 {
		fatal("Must specify a valid topic")
	}
	if len(*delimiter) != 1 {
		fatal("Delimiter must be a single byte")
	}

	// prepare the configuration
	wcfg := nsq.NewConfig()
	wcfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", util.BINARY_VERSION, nsq.VERSION)
	if err := util.ParseReaderOpts(wcfg, readerOpts); err != nil {
		fatal(err)
	}
	wcfg.MaxInFlight = *maxInFlight

	// make the producers
	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, wcfg)
		if err != nil {
			fatal("failed creating producer", err)
		}
		producer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
		producers[addr] = producer
		defer producer.Stop()
	}
	if len(producers) == 0 {
		fatal("Must specify at least one nsqd-tcp-address")
	}

	// scan the input
	r := bufio.NewReader(os.Stdin)
	delim := []byte(*delimiter)[0]
	go func() {
		var readErr error = nil
		var line []byte
		for readErr == nil {

			line, readErr = r.ReadBytes(delim)
			if readErr == nil || readErr == io.EOF {
				if len(line) > 0 {
					line = line[:len(line)-1]
				}
				if len(line) > 0 {
					for _, producer := range producers {
						fmt.Println(string(line))
						if err := producer.Publish(*topic, line); err != nil {
							log.Fatalln(err)
						}
					}
				}
				if readErr == io.EOF {
					stopChan <- true
				}
			} else {
				// real error
				stopChan <- true
				fatal(readErr)
			}

		}
	}()

	// wait for things to finish
	select {
	case <-termChan:
	case <-stopChan:
	}

}

// fatal writes an error and exits
func fatal(args ...interface{}) {
	os.Stderr.WriteString(fmt.Sprint(args...))
	os.Stderr.WriteString("\n")
	flag.PrintDefaults()
	os.Exit(1)
}
