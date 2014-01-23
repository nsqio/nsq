package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/bitly/nsq/util"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	flagSet = flag.NewFlagSet("nsqd", flag.ExitOnError)

	// basic options
	config           = flagSet.String("config", "", "path to config file")
	showVersion      = flagSet.Bool("version", false, "print version string")
	verbose          = flagSet.Bool("verbose", false, "enable verbose logging")
	workerId         = flagSet.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:4151", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress       = flagSet.String("tcp-address", "0.0.0.0:4150", "<addr>:<port> to listen on for TCP clients")
	broadcastAddress = flagSet.String("broadcast-address", "", "address that will be registered with lookupd (defaults to the OS hostname)")
	lookupdTCPAddrs  = util.StringArray{}

	// diskqueue options
	dataPath        = flagSet.String("data-path", "", "path to store disk-backed messages")
	memQueueSize    = flagSet.Int64("mem-queue-size", 10000, "number of messages to keep in memory (per topic/channel)")
	maxBytesPerFile = flagSet.Int64("max-bytes-per-file", 104857600, "number of bytes per diskqueue file before rolling")
	syncEvery       = flagSet.Int64("sync-every", 2500, "number of messages per diskqueue fsync")
	syncTimeout     = flagSet.Duration("sync-timeout", 2*time.Second, "duration of time per diskqueue fsync")

	// msg and command options
	msgTimeout     = flagSet.String("msg-timeout", "60s", "duration to wait before auto-requeing a message")
	maxMsgTimeout  = flagSet.Duration("max-msg-timeout", 15*time.Minute, "maximum duration before a message will timeout")
	maxMessageSize = flagSet.Int64("max-message-size", 1024768, "maximum size of a single message in bytes")
	maxBodySize    = flagSet.Int64("max-body-size", 5*1024768, "maximum size of a single command body")

	// client overridable configuration options
	maxHeartbeatInterval   = flagSet.Duration("max-heartbeat-interval", 60*time.Second, "maximum client configurable duration of time between client heartbeats")
	maxRdyCount            = flagSet.Int64("max-rdy-count", 2500, "maximum RDY count for a client")
	maxOutputBufferSize    = flagSet.Int64("max-output-buffer-size", 64*1024, "maximum client configurable size (in bytes) for a client output buffer")
	maxOutputBufferTimeout = flagSet.Duration("max-output-buffer-timeout", 1*time.Second, "maximum client configurable duration of time between flushing to a client")

	// statsd integration options
	statsdAddress  = flagSet.String("statsd-address", "", "UDP <addr>:<port> of a statsd daemon for pushing stats")
	statsdInterval = flagSet.String("statsd-interval", "60s", "duration between pushing to statsd")
	statsdMemStats = flagSet.Bool("statsd-mem-stats", true, "toggle sending memory and GC stats to statsd")
	statsdPrefix   = flagSet.String("statsd-prefix", "nsq.%s", "prefix used for keys sent to statsd (%s for host replacement)")

	// End to end percentile flags
	e2eProcessingLatencyPercentiles = util.FloatArray{}
	e2eProcessingLatencyWindowTime  = flagSet.Duration("e2e-processing-latency-window-time", 10*time.Minute, "calculate end to end latency quantiles for this duration of time (ie: 60s would only show quantile calculations from the past 60 seconds)")

	// TLS config
	tlsCert = flagSet.String("tls-cert", "", "path to certificate file")
	tlsKey  = flagSet.String("tls-key", "", "path to private key file")

	// compression
	deflateEnabled  = flagSet.Bool("deflate", true, "enable deflate feature negotiation (client compression)")
	maxDeflateLevel = flagSet.Int("max-deflate-level", 6, "max deflate compression level a client can negotiate (> values == > nsqd CPU usage)")
	snappyEnabled   = flagSet.Bool("snappy", true, "enable snappy feature negotiation (client compression)")
)

func init() {
	flagSet.Var(&lookupdTCPAddrs, "lookupd-tcp-address", "lookupd TCP address (may be given multiple times)")
	flagSet.Var(&e2eProcessingLatencyPercentiles, "e2e-processing-latency-percentile", "message processing time percentiles to keep track of (can be specified multiple times or comma separated, default none)")
}

func main() {
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	if *showVersion {
		fmt.Println(util.Version("nsqd"))
		return
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	options := NewNSQDOptions()
	util.ResolveOptions(options, flagSet, cfg)
	nsqd := NewNSQD(options)

	log.Println(util.Version("nsqd"))
	log.Printf("worker id %d", options.ID)

	nsqd.LoadMetadata()
	err := nsqd.PersistMetadata()
	if err != nil {
		log.Fatalf("ERROR: failed to persist metadata - %s", err.Error())
	}
	nsqd.Main()
	<-exitChan
	nsqd.Exit()
}
