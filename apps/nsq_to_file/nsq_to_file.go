// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"encoding/json"
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
	"github.com/nsqio/nsq/internal/lg"
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
	fs.String("log-level", "info", "set log verbosity: debug, info, warn, error, or fatal")
	fs.String("log-prefix", "[nsq_to_file] ", "log message prefix")

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

// FileLoggerStats struct definition
type FileLoggerStats struct {
	Rotations         int64  `json:"rotations"`
	SkippedEmptyFiles int64  `json:"skipped_empty_files"`
	FilesWritten      int64  `json:"files_written"`
	Uptime            int64  `json:"uptime"`
	TopicPattern      string `json:"topic_pattern"`
	IsGzipped         bool   `json:"is_gzipped"`
}

var serviceStats *FileLoggerStats

// InitStats function initialises stats for service
func (f *FileLoggerStats) InitStats() {

	// Update uptime
	go func() {
		for {
			f.Uptime += 5
			b, err := json.Marshal(f)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(b))
			time.Sleep(5 * time.Second)
		}
	}()

	// // Create new router
	// muxRouter := mux.NewRouter()
	// // Handle /stats
	// muxRouter.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
	// 	b, err := json.Marshal(f)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	w.Write(b)
	// })

	// // Listens in the background
	// go func() {
	// 	// Log Fatal to crash in case of non available port
	// 	if err := http.ListenAndServe(":4949", muxRouter); err != nil {

	// 	}
	// 	log.Fatal()
	// }()

}

type logger struct{}

func (lo *logger) Output(calldepth int, s string) error {

	b, err := json.Marshal(map[string]interface{}{
		"service":  "nsq_to_file",
		"error":    s,
		"hostname": os.Getenv("HOSTNAME"),
	})

	if err != nil {
		return err
	}

	fmt.Println(b)

	return nil
}

func main() {
	fs := flagSet()
	fs.Parse(os.Args[1:])

	if args := fs.Args(); len(args) > 0 {
		log.Fatalf("unknown arguments: %s", args)
	}

	opts := NewOptions()
	options.Resolve(opts, fs, nil)

	serviceStats = new(FileLoggerStats)
	serviceStats.IsGzipped = opts.GZIP
	serviceStats.TopicPattern = opts.TopicPattern
	serviceStats.InitStats()

	logger := log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	logLevel, err := lg.ParseLogLevel(opts.LogLevel)
	if err != nil {
		log.Fatal("--log-level is invalid")
	}
	logf := func(lvl lg.LogLevel, f string, args ...interface{}) {
		lg.Logf(logger, logLevel, lvl, f, args...)
	}

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

	discoverer := newTopicDiscoverer(logf, opts, cfg, hupChan, termChan)
	discoverer.run()
}
