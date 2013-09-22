package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/bitly/nsq/util"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var (
	// basic options
	showVersion      = flag.Bool("version", false, "print version string")
	verbose          = flag.Bool("verbose", false, "enable verbose logging")
	workerId         = flag.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	httpAddress      = flag.String("http-address", "0.0.0.0:4151", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress       = flag.String("tcp-address", "0.0.0.0:4150", "<addr>:<port> to listen on for TCP clients")
	broadcastAddress = flag.String("broadcast-address", "", "address that will be registered with lookupd (defaults to the OS hostname)")
	lookupdTCPAddrs  = util.StringArray{}

	// diskqueue options
	dataPath        = flag.String("data-path", "", "path to store disk-backed messages")
	memQueueSize    = flag.Int64("mem-queue-size", 10000, "number of messages to keep in memory (per topic/channel)")
	maxBytesPerFile = flag.Int64("max-bytes-per-file", 104857600, "number of bytes per diskqueue file before rolling")
	syncEvery       = flag.Int64("sync-every", 2500, "number of messages per diskqueue fsync")
	syncTimeout     = flag.Duration("sync-timeout", 2*time.Second, "duration of time per diskqueue fsync")

	// msg and command options
	msgTimeout     = flag.String("msg-timeout", "60s", "duration to wait before auto-requeing a message")
	maxMsgTimeout  = flag.Duration("max-msg-timeout", 15*time.Minute, "maximum duration before a message will timeout")
	maxMessageSize = flag.Int64("max-message-size", 1024768, "maximum size of a single message in bytes")
	maxBodySize    = flag.Int64("max-body-size", 5*1024768, "maximum size of a single command body")

	// client overridable configuration options
	maxHeartbeatInterval   = flag.Duration("max-heartbeat-interval", 60*time.Second, "maximum client configurable duration of time between client heartbeats")
	maxRdyCount            = flag.Int64("max-rdy-count", 2500, "maximum RDY count for a client")
	maxOutputBufferSize    = flag.Int64("max-output-buffer-size", 64*1024, "maximum client configurable size (in bytes) for a client output buffer")
	maxOutputBufferTimeout = flag.Duration("max-output-buffer-timeout", 1*time.Second, "maximum client configurable duration of time between flushing to a client")

	// statsd integration options
	statsdAddress  = flag.String("statsd-address", "", "UDP <addr>:<port> of a statsd daemon for pushing stats")
	statsdInterval = flag.String("statsd-interval", "60s", "duration between pushing to statsd")
	statsdMemStats = flag.Bool("statsd-mem-stats", true, "toggle sending memory and GC stats to statsd")
	statsdPrefix   = flag.String("statsd-prefix", "nsq.%s", "prefix used for keys sent to statsd (%s for host replacement)")

	// TLS config
	tlsCert = flag.String("tls-cert", "", "path to certificate file")
	tlsKey  = flag.String("tls-key", "", "path to private key file")

	// compression
	deflateEnabled  = flag.Bool("deflate", true, "enable deflate feature negotiation (client compression)")
	maxDeflateLevel = flag.Int("max-deflate-level", 6, "max deflate compression level a client can negotiate (> values == > nsqd CPU usage)")
	snappyEnabled   = flag.Bool("snappy", true, "enable snappy feature negotiation (client compression)")
)

func init() {
	flag.Var(&lookupdTCPAddrs, "lookupd-tcp-address", "lookupd TCP address (may be given multiple times)")
}

func main() {
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	if *showVersion {
		fmt.Println(util.Version("nsqd"))
		return
	}

	if *workerId == 0 {
		h := md5.New()
		io.WriteString(h, hostname)
		*workerId = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	if *broadcastAddress == "" {
		*broadcastAddress = hostname
	}

	log.Println(util.Version("nsqd"))
	log.Printf("worker id %d", *workerId)

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// flagToDuration will fatally error if it is invalid
	msgTimeoutDuration := flagToDuration(*msgTimeout, time.Millisecond, "--msg-timeout")

	if *maxDeflateLevel < 1 || *maxDeflateLevel > 9 {
		log.Fatalf("--max-deflate-level must be [1,9]")
	}

	options := NewNsqdOptions()
	options.maxRdyCount = *maxRdyCount
	options.maxMessageSize = *maxMessageSize
	options.maxBodySize = *maxBodySize
	options.memQueueSize = *memQueueSize
	options.dataPath = *dataPath
	options.maxBytesPerFile = *maxBytesPerFile
	options.syncEvery = *syncEvery
	options.syncTimeout = *syncTimeout
	options.msgTimeout = msgTimeoutDuration
	options.maxMsgTimeout = *maxMsgTimeout
	options.broadcastAddress = *broadcastAddress
	options.maxHeartbeatInterval = *maxHeartbeatInterval
	options.maxOutputBufferSize = *maxOutputBufferSize
	options.maxOutputBufferTimeout = *maxOutputBufferTimeout
	options.tlsCert = *tlsCert
	options.tlsKey = *tlsKey
	options.deflateEnabled = *deflateEnabled
	options.maxDeflateLevel = *maxDeflateLevel
	options.snappyEnabled = *snappyEnabled

	if *statsdAddress != "" {
		// flagToDuration will fatally error if it is invalid
		options.statsdInterval = flagToDuration(*statsdInterval, time.Second, "--statsd-interval")
		statsdHostKey := util.StatsdHostKey(net.JoinHostPort(*broadcastAddress, strconv.Itoa(httpAddr.Port)))
		prefixWithHost := strings.Replace(*statsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		options.statsdPrefix = prefixWithHost
		options.statsdAddress = *statsdAddress
	}

	nsqd := NewNSQd(*workerId, options)
	nsqd.tcpAddr = tcpAddr
	nsqd.httpAddr = httpAddr
	nsqd.lookupdTCPAddrs = lookupdTCPAddrs

	nsqd.LoadMetadata()
	err = nsqd.PersistMetadata()
	if err != nil {
		log.Fatalf("ERROR: failed to persist metadata - %s", err.Error())
	}
	nsqd.Main()
	<-exitChan
	nsqd.Exit()
}

// this is a helper to maintain backwards compatibility for flags which
// were originally Int before we realized there was a Duration flag :)
func flagToDuration(val string, mult time.Duration, flag string) time.Duration {
	if regexp.MustCompile(`^[0-9]+$`).MatchString(val) {
		intVal, err := strconv.Atoi(val)
		if err != nil {
			log.Fatalf("ERROR: failed to Atoi %s=%s - %s", flag, val, err.Error())
		}
		return time.Duration(intVal) * mult
	}

	duration, err := time.ParseDuration(val)
	if err != nil {
		log.Fatalf("ERROR: failed to ParseDuration %s=%s - %s", flag, val, err.Error())
	}
	return duration
}
