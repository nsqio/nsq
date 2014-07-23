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

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
)

var (
	topic            = flag.String("topic", "", "nsq topic")
	delimiter        = flag.String("delimiter", "\n", "what from stdin to split on (lines by default)")
	destNsqdTCPAddrs = util.StringArray{}
	readerOpts       = util.StringArray{}
)

func init() {
	flag.Var(&readerOpts, "reader-opt", "option to passthrough to nsq.Consumer (may be given multiple times)")
	flag.Var(&destNsqdTCPAddrs, "nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
}

var logger *log.Logger

func main() {
	logger = log.New(os.Stderr, "", log.LstdFlags)

	// handle stopping
	stopChan := make(chan bool)
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// parse flags
	flag.Parse()
	if len(*topic) == 0 {
		fatal(true, "Must specify a valid topic")
	}
	if len(*delimiter) != 1 {
		fatal(true, "Delimiter must be a single byte")
	}

	// prepare the configuration
	wcfg := nsq.NewConfig()
	wcfg.UserAgent = fmt.Sprintf("to_nsq/%s go-nsq/%s", util.BINARY_VERSION, nsq.VERSION)
	if err := util.ParseReaderOpts(wcfg, readerOpts); err != nil {
		fatal(true, err)
	}

	// make the producers
	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, wcfg)
		if err != nil {
			fatal(true, "failed creating producer", err)
		}
		producer.SetLogger(logger, nsq.LogLevelInfo)
		producers[addr] = producer
		defer producer.Stop()
	}
	if len(producers) == 0 {
		fatal(true, "Must specify at least one nsqd-tcp-address")
	}

	var fatalErr error

	// scan the input
	r := bufio.NewReader(os.Stdin)
	delim := (*delimiter)[0]
	go func() {
		for {
			if err := readAndPublish(r, delim, producers); err != nil {
				if err != io.EOF {
					fatalErr = err
				}
				stopChan <- true // stop
				break
			}
		}
	}()

	// wait for things to finish
	select {
	case <-termChan:
	case <-stopChan:
	}

	// if a fatal error occurred - report it
	if fatalErr != nil {
		fatal(false, fatalErr)
	}

}

// readAndPublish reads to the delim from r and publishes the bytes
// to the map of producers.
func readAndPublish(r *bufio.Reader, delim byte, producers map[string]*nsq.Producer) error {

	line, readErr := r.ReadBytes(delim)

	if len(line) > 0 { // trim the delimiter
		line = line[:len(line)-1]
	}
	if len(line) > 0 {
		for _, producer := range producers {
			if err := producer.Publish(*topic, line); err != nil {
				return err
			}
		}
	}

	return readErr

}

// fatal writes an error and exits
func fatal(usage bool, args ...interface{}) {
	logger.Println(args...)
	if usage {
		flag.PrintDefaults()
	}
	os.Exit(1)
}
