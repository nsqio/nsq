package main

import (
	"flag"
	"fmt"
	"github.com/bitly/nsq/util"
	"log"
	"net"
	"os"
	"os/signal"
	"time"
)

var (
	showVersion              = flag.Bool("version", false, "print version string")
	httpAddress              = flag.String("http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	templateDir              = flag.String("template-dir", "", "path to templates directory")
	graphiteUrl              = flag.String("graphite-url", "", "URL to graphite HTTP address")
	proxyGraphite            = flag.Bool("proxy-graphite", false, "Proxy HTTP requests to graphite")
	useStatsdPrefixes        = flag.Bool("use-statsd-prefixes", true, "expect statsd prefixed keys in graphite (ie: 'stats_counts.')")
	statsdPrefix             = flag.String("statsd-prefix", "nsq.%s", "prefix used for keys sent to statsd (%s for host replacement, must match nsqd)")
	statsdInterval           = flag.Duration("statsd-interval", 60*time.Second, "time interval nsqd is configured to push to statsd (must match nsqd)")
	lookupdHTTPAddrs         = util.StringArray{}
	nsqdHTTPAddrs            = util.StringArray{}
	notificationHTTPEndpoint = flag.String("notification-http-endpoint", "", "HTTP endpoint (fully qualified) to which POST notifications of admin actions will be sent")
)

var notifications chan *AdminAction

func init() {
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&nsqdHTTPAddrs, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
}

func main() {
	var waitGroup util.WaitGroupWrapper

	flag.Parse()

	if *showVersion {
		fmt.Println(util.Version("nsqadmin"))
		return
	}

	if *templateDir == "" {
		for _, defaultPath := range []string{"templates", "/usr/local/share/nsqadmin/templates"} {
			if info, err := os.Stat(defaultPath); err == nil && info.IsDir() {
				*templateDir = defaultPath
				break
			}
		}
	}

	if *templateDir == "" {
		log.Fatalf("--template-dir must be specified (or install the templates to /usr/local/share/nsqadmin/templates)")
	}

	if len(nsqdHTTPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required.")
	}

	if len(nsqdHTTPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-http-address or --lookupd-http-address not both")
	}

	log.Println(util.Version("nsqadmin"))

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)

	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpListener, err := net.Listen("tcp", httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", httpAddr, err.Error())
	}
	waitGroup.Wrap(func() { httpServer(httpListener) })

	notifications = make(chan *AdminAction)
	waitGroup.Wrap(func() { HandleAdminActions() })

	<-exitChan

	httpListener.Close()
	close(notifications)

	waitGroup.Wait()
}
