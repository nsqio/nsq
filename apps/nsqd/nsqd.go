package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bitly/nsq/internal/app"
	"github.com/bitly/nsq/internal/version"
	"github.com/bitly/nsq/nsqd"
	"github.com/mreiferson/go-options"
)

type tlsRequiredOption int

func (t *tlsRequiredOption) Set(s string) error {
	s = strings.ToLower(s)
	if s == "tcp-https" {
		*t = nsqd.TLSRequiredExceptHTTP
		return nil
	}
	required, err := strconv.ParseBool(s)
	if required {
		*t = nsqd.TLSRequired
	} else {
		*t = nsqd.TLSNotRequired
	}
	return err
}

func (t *tlsRequiredOption) Get() interface{} { return *t }

func (t *tlsRequiredOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

func (t *tlsRequiredOption) IsBoolFlag() bool { return true }

type tlsVersionOption uint16

func (t *tlsVersionOption) Set(s string) error {
	s = strings.ToLower(s)
	switch s {
	case "":
		return nil
	case "ssl3.0":
		*t = tls.VersionSSL30
	case "tls1.0":
		*t = tls.VersionTLS10
	case "tls1.1":
		*t = tls.VersionTLS11
	case "tls1.2":
		*t = tls.VersionTLS12
	default:
		return fmt.Errorf("unknown tlsVersionOption %q", s)
	}
	return nil
}

func (t *tlsVersionOption) Get() interface{} {
	return *t
}

func (t *tlsVersionOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

func nsqFlagset() *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqd", flag.ExitOnError)

	// basic options
	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")
	flagSet.Bool("verbose", false, "enable verbose logging")
	flagSet.Int64("worker-id", 0, "unique seed for message ID generation (int) in range [0,4096) (will default to a hash of hostname)")
	flagSet.String("https-address", "", "<addr>:<port> to listen on for HTTPS clients")
	flagSet.String("http-address", "0.0.0.0:4151", "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("tcp-address", "0.0.0.0:4150", "<addr>:<port> to listen on for TCP clients")

	authHTTPAddresses := app.StringArray{}
	flagSet.Var(&authHTTPAddresses, "auth-http-address", "<addr>:<port> to query auth server (may be given multiple times)")

	flagSet.String("broadcast-address", "", "address that will be registered with lookupd (defaults to the OS hostname)")
	lookupdTCPAddrs := app.StringArray{}
	flagSet.Var(&lookupdTCPAddrs, "lookupd-tcp-address", "lookupd TCP address (may be given multiple times)")

	// diskqueue options
	flagSet.String("data-path", "", "path to store disk-backed messages")
	flagSet.Int64("mem-queue-size", 10000, "number of messages to keep in memory (per topic/channel)")
	flagSet.Int64("max-bytes-per-file", 104857600, "number of bytes per diskqueue file before rolling")
	flagSet.Int64("sync-every", 2500, "number of messages per diskqueue fsync")
	flagSet.Duration("sync-timeout", 2*time.Second, "duration of time per diskqueue fsync")

	// msg and command options
	flagSet.String("msg-timeout", "60s", "duration to wait before auto-requeing a message")
	flagSet.Duration("max-msg-timeout", 15*time.Minute, "maximum duration before a message will timeout")
	flagSet.Int64("max-msg-size", 1024768, "maximum size of a single message in bytes")
	flagSet.Duration("max-req-timeout", 1*time.Hour, "maximum requeuing timeout for a message")
	// remove, deprecated
	flagSet.Int64("max-message-size", 1024768, "(deprecated use --max-msg-size) maximum size of a single message in bytes")
	flagSet.Int64("max-body-size", 5*1024768, "maximum size of a single command body")

	// client overridable configuration options
	flagSet.Duration("max-heartbeat-interval", 60*time.Second, "maximum client configurable duration of time between client heartbeats")
	flagSet.Int64("max-rdy-count", 2500, "maximum RDY count for a client")
	flagSet.Int64("max-output-buffer-size", 64*1024, "maximum client configurable size (in bytes) for a client output buffer")
	flagSet.Duration("max-output-buffer-timeout", 1*time.Second, "maximum client configurable duration of time between flushing to a client")

	// statsd integration options
	flagSet.String("statsd-address", "", "UDP <addr>:<port> of a statsd daemon for pushing stats")
	flagSet.String("statsd-interval", "60s", "duration between pushing to statsd")
	flagSet.Bool("statsd-mem-stats", true, "toggle sending memory and GC stats to statsd")
	flagSet.String("statsd-prefix", "nsq.%s", "prefix used for keys sent to statsd (%s for host replacement)")

	// End to end percentile flags
	e2eProcessingLatencyPercentiles := app.FloatArray{}
	flagSet.Var(&e2eProcessingLatencyPercentiles, "e2e-processing-latency-percentile", "message processing time percentiles to keep track of (can be specified multiple times or comma separated, default none)")
	flagSet.Duration("e2e-processing-latency-window-time", 10*time.Minute, "calculate end to end latency quantiles for this duration of time (ie: 60s would only show quantile calculations from the past 60 seconds)")

	// TLS config
	flagSet.String("tls-cert", "", "path to certificate file")
	flagSet.String("tls-key", "", "path to private key file")
	flagSet.String("tls-client-auth-policy", "", "client certificate auth policy ('require' or 'require-verify')")
	flagSet.String("tls-root-ca-file", "", "path to private certificate authority pem")
	var tlsRequired tlsRequiredOption
	var tlsMinVersion tlsVersionOption
	flagSet.Var(&tlsRequired, "tls-required", "require TLS for client connections (true, false, tcp-https)")
	flagSet.Var(&tlsMinVersion, "tls-min-version", "minimum SSL/TLS version acceptable ('ssl3.0', 'tls1.0', 'tls1.1', or 'tls1.2')")

	// compression
	flagSet.Bool("deflate", true, "enable deflate feature negotiation (client compression)")
	flagSet.Int("max-deflate-level", 6, "max deflate compression level a client can negotiate (> values == > nsqd CPU usage)")
	flagSet.Bool("snappy", true, "enable snappy feature negotiation (client compression)")

	return flagSet
}

type config map[string]interface{}

// Validate settings in the config file, and fatal on errors
func (cfg config) Validate() {
	// special validation/translation
	if v, exists := cfg["tls_required"]; exists {
		var t tlsRequiredOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["tls_required"] = t.String()
		} else {
			log.Fatalf("ERROR: failed parsing tls required %v", v)
		}
	}
	if v, exists := cfg["tls_min_version"]; exists {
		var t tlsVersionOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			newVal := fmt.Sprintf("%v", t.Get())
			if newVal != "0" {
				cfg["tls_min_version"] = newVal
			} else {
				delete(cfg, "tls_min_version")
			}
		} else {
			log.Fatalf("ERROR: failed parsing tls min version %v", v)
		}
	}
}

func main() {

	flagSet := nsqFlagset()
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var cfg config
	configFile := flagSet.Lookup("config").Value.String()
	if configFile != "" {
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", configFile, err.Error())
		}
	}
	cfg.Validate()

	opts := nsqd.NewNSQDOptions()
	options.Resolve(opts, flagSet, cfg)
	nsqd := nsqd.NewNSQD(opts)

	nsqd.LoadMetadata()
	err := nsqd.PersistMetadata()
	if err != nil {
		log.Fatalf("ERROR: failed to persist metadata - %s", err.Error())
	}
	nsqd.Main()
	<-signalChan
	nsqd.Exit()
}
