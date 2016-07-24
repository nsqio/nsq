package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqadmin"
)

var (
	flagSet = flag.NewFlagSet("nsqadmin", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")

	httpAddress = flagSet.String("http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	templateDir = flagSet.String("template-dir", "", "path to templates directory")

	graphiteURL   = flagSet.String("graphite-url", "", "graphite HTTP address")
	proxyGraphite = flagSet.Bool("proxy-graphite", false, "proxy HTTP requests to graphite")

	useStatsdPrefixes   = flagSet.Bool("use-statsd-prefixes", true, "(Deprecated - Use --statsd-counter-format and --statsd-gauge-format) Expect statsd prefixed keys in graphite (ie: 'stats.counters.' and 'stats.gauges.')")
	statsdCounterFormat = flagSet.String("statsd-counter-format", "stats.counters.%s.count", "The counter stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	statsdGaugeFormat   = flagSet.String("statsd-gauge-format", "stats.gauges.%s", "The gauge stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	statsdPrefix        = flagSet.String("statsd-prefix", "nsq.%s", "prefix used for keys sent to statsd (%s for host replacement, must match nsqd)")
	statsdInterval      = flagSet.Duration("statsd-interval", 60*time.Second, "time interval nsqd is configured to push to statsd (must match nsqd)")

	notificationHTTPEndpoint = flagSet.String("notification-http-endpoint", "", "HTTP endpoint (fully qualified) to which POST notifications of admin actions will be sent")

	httpConnectTimeout = flagSet.Duration("http-client-connect-timeout", 2*time.Second, "timeout for HTTP connect")
	httpRequestTimeout = flagSet.Duration("http-client-request-timeout", 5*time.Second, "timeout for HTTP request")

	httpClientTLSInsecureSkipVerify = flagSet.Bool("http-client-tls-insecure-skip-verify", false, "configure the HTTP client to skip verification of TLS certificates")
	httpClientTLSRootCAFile         = flagSet.String("http-client-tls-root-ca-file", "", "path to CA file for the HTTP client")
	httpClientTLSCert               = flagSet.String("http-client-tls-cert", "", "path to certificate file for the HTTP client")
	httpClientTLSKey                = flagSet.String("http-client-tls-key", "", "path to key file for the HTTP client")

	nsqlookupdHTTPAddresses = app.StringArray{}
	nsqdHTTPAddresses       = app.StringArray{}
)

func init() {
	flagSet.Var(&nsqlookupdHTTPAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flagSet.Var(&nsqdHTTPAddresses, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
}

func main() {
	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(version.String("nsqadmin"))
		return
	}

	if *templateDir != "" {
		log.Printf("WARNING: --template-dir is deprecated and will be removed in the next release (templates are now compiled into the binary)")
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
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err)
		}
	}

	opts := nsqadmin.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	nsqadmin := nsqadmin.New(opts)

	nsqadmin.Main()
	<-exitChan
	nsqadmin.Exit()
}
