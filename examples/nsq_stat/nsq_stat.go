// This is an NSQ client that polls /stats for all the producers
// of the specified topic/channel and displays aggregate stats

package main

import (
	"flag"
	"fmt"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/lookupd"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	showVersion      = flag.Bool("version", false, "print version")
	topic            = flag.String("topic", "", "NSQ topic")
	channel          = flag.String("channel", "", "NSQ channel")
	statusEvery      = flag.Duration("status-every", 2*time.Second, "duration of time between polling/printing output")
	nsqdHTTPAddrs    = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&nsqdHTTPAddrs, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

func statLoop(interval time.Duration, topic string, channel string,
	nsqdTCPAddrs []string, lookupdHTTPAddrs []string) {
	i := 0
	for {
		log.SetOutput(ioutil.Discard)
		var producers []string
		if len(lookupdHTTPAddrs) != 0 {
			producers, _ = lookupd.GetLookupdTopicProducers(topic, lookupdHTTPAddrs)
		} else {
			producers, _ = lookupd.GetNSQDTopicProducers(topic, nsqdHTTPAddrs)
		}
		_, allChannelStats, _ := lookupd.GetNSQDStats(producers, topic)
		c := allChannelStats[channel]
		log.SetOutput(os.Stdout)

		if i%25 == 0 {
			fmt.Printf("-----------depth------------+--------------metadata---------------\n")
			fmt.Printf("%7s %7s %5s %5s | %7s %7s %12s %7s\n", "mem", "disk", "inflt", "def", "req", "t-o", "msgs", "clients")
		}

		// TODO: paused
		fmt.Printf("%7d %7d %5d %5d | %7d %7d %12d %7d\n",
			c.Depth,
			c.BackendDepth,
			c.InFlightCount,
			c.DeferredCount,
			c.RequeueCount,
			c.TimeoutCount,
			c.MessageCount,
			c.ClientCount)

		time.Sleep(interval)

		i++
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_stat v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if len(nsqdHTTPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required")
	}
	if len(nsqdHTTPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatalf("use --nsqd-http-address or --lookupd-http-address not both")
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go statLoop(*statusEvery, *topic, *channel, nsqdHTTPAddrs, lookupdHTTPAddrs)

	<-termChan
}
