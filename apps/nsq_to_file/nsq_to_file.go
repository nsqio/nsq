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

	"github.com/mreiferson/go-options"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

func hasArg(s string) bool {
	argExist := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == s {
			argExist = true
		}
	})
	return argExist
}

func flagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("nsqd", flag.ExitOnError)

	fs.Bool("version", false, "print version string")

	fs.String("channel", "nsq_to_file", "nsq channel")
	fs.Int("max-in-flight", 200, "max number of messages to allow in flight")

	fs.String("output-dir", "/tmp", "directory to write output files to")
	fs.String("work-dir", "", "directory for in-progress files before moving to output-dir")
	fs.String("datetime-format", "%Y-%m-%d_%H", "strftime compatible format for <DATETIME> in filename format")
	fs.String("filename-format", "<TOPIC>.<HOST><REV>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <PID>, <DATETIME>, <REV> are replaced. <REV> is increased when file already exists)")
	fs.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	fs.Int("gzip-level", 6, "gzip compression level (1-9, 1=BestSpeed, 9=BestCompression)")
	fs.Bool("gzip", false, "gzip output files.")
	fs.Bool("skip-empty-files", false, "skip writing empty files")
	fs.Duration("topic-refresh", time.Minute, "how frequently the topic list should be refreshed")
	fs.String("topic-pattern", "", "only log topics matching the following pattern")

	fs.Int64("rotate-size", 0, "rotate the file when it grows bigger than `rotate-size` bytes")
	fs.Duration("rotate-interval", 0, "rotate the file every duration")
	fs.Duration("sync-interval", 30*time.Second, "sync file to disk every duration")

	fs.Duration("http-client-connect-timeout", 2*time.Second, "timeout for HTTP connect")
	fs.Duration("http-client-request-timeout", 5*time.Second, "timeout for HTTP request")

	nsqdTCPAddrs := app.StringArray{}
	lookupdHTTPAddrs := app.StringArray{}
	topics := app.StringArray{}
	consumerOpts := app.StringArray{}
	fs.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	fs.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	fs.Var(&topics, "topic", "nsq topic (may be given multiple times)")
	fs.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")

	return fs
}

func main() {
	fs := flagSet()
	fs.Parse(os.Args[1:])

	opts := NewOptions()
	options.Resolve(opts, fs, nil)

	if fs.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Printf("nsq_to_file v%s\n", version.Binary)
		return
	}

	if opts.Channel == "" {
		log.Fatal("--channel is required")
	}

	if opts.HTTPClientConnectTimeout <= 0 {
		log.Fatal("--http-client-connect-timeout should be positive")
	}

	if opts.HTTPClientRequestTimeout <= 0 {
		log.Fatal("--http-client-request-timeout should be positive")
	}

	if len(opts.NSQDTCPAddrs) == 0 && len(opts.NSQLookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(opts.NSQDTCPAddrs) != 0 && len(opts.NSQLookupdHTTPAddrs) != 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if opts.GZIPLevel < 1 || opts.GZIPLevel > 9 {
		log.Fatalf("invalid --gzip-level value (%d), should be 1-9", opts.GZIPLevel)
	}

	if len(opts.Topics) == 0 && len(opts.TopicPattern) == 0 {
		log.Fatal("--topic or --topic-pattern required")
	}

	if len(opts.Topics) == 0 && len(opts.NSQLookupdHTTPAddrs) == 0 {
		log.Fatal("--lookupd-http-address must be specified when no --topic specified")
	}

	if opts.WorkDir == "" {
		opts.WorkDir = opts.OutputDir
	}

	cfg := nsq.NewConfig()
	cfgFlag := nsq.ConfigFlag{cfg}
	for _, opt := range opts.ConsumerOpts {
		cfgFlag.Set(opt)
	}
	cfg.UserAgent = fmt.Sprintf("nsq_to_file/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = opts.MaxInFlight

	hupChan := make(chan os.Signal)
	termChan := make(chan os.Signal)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	discoverer := newTopicDiscoverer(opts, cfg, hupChan, termChan)
	discoverer.run()
}
