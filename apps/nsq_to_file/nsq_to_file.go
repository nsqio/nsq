// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"flag"
	"fmt"
	"log"
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

	channel     = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	outputDir      = flag.String("output-dir", "/tmp", "directory to write output files to")
	datetimeFormat = flag.String("datetime-format", "%Y-%m-%d_%H", "strftime compatible format for <DATETIME> in filename format")
	filenameFormat = flag.String("filename-format", "<TOPIC>.<HOST><REV>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <PID>, <DATETIME>, <REV> are replaced. <REV> is increased when file already exists)")
	hostIdentifier = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	gzipLevel      = flag.Int("gzip-level", 6, "gzip compression level (1-9, 1=BestSpeed, 9=BestCompression)")
	gzipEnabled    = flag.Bool("gzip", false, "gzip output files.")
	skipEmptyFiles = flag.Bool("skip-empty-files", false, "Skip writing empty files")
	topicPollRate  = flag.Duration("topic-refresh", time.Minute, "how frequently the topic list should be refreshed")
	topicPattern   = flag.String("topic-pattern", "", "Only log topics matching the following pattern")

	rotateSize     = flag.Int64("rotate-size", 0, "rotate the file when it grows bigger than `rotate-size` bytes")
	rotateInterval = flag.Duration("rotate-interval", 0*time.Second, "rotate the file every duration")

	httpConnectTimeout = flag.Duration("http-client-connect-timeout", 2*time.Second, "timeout for HTTP connect")
	httpRequestTimeout = flag.Duration("http-client-request-timeout", 5*time.Second, "timeout for HTTP request")

	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
	topics           = app.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
}

func hasArg(s string) bool {
	argExist := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == s {
			argExist = true
		}
	})
	return argExist
}

func main() {
	cfg := nsq.NewConfig()

	flag.Var(&nsq.ConfigFlag{cfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", version.Binary)
		return
	}

	if *channel == "" {
		log.Fatal("--channel is required")
	}

	if *httpConnectTimeout <= 0 {
		log.Fatal("--http-client-connect-timeout should be positive")
	}

	if *httpRequestTimeout <= 0 {
		log.Fatal("--http-client-request-timeout should be positive")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if *gzipLevel < 1 || *gzipLevel > 9 {
		log.Fatalf("invalid --gzip-level value (%d), should be 1-9", *gzipLevel)
	}

	if len(topics) == 0 && len(*topicPattern) == 0 {
		log.Fatal("--topic or --topic-pattern required")
	}

	if len(topics) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--lookupd-http-address must be specified when no --topic specified")
	}

	cfg.UserAgent = fmt.Sprintf("nsq_to_file/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	hupChan := make(chan os.Signal)
	termChan := make(chan os.Signal)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	discoverer := newTopicDiscoverer(cfg, hupChan, termChan, *httpConnectTimeout, *httpRequestTimeout)
	discoverer.updateTopics(topics, *topicPattern)
	discoverer.poller(lookupdHTTPAddrs, len(topics) == 0, *topicPattern)
}
