// This is an NSQ client that reads the specified topic/channel
// and performs HTTP requests (GET/POST) to the specified endpoints

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/timer_metrics"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/version"
)

const (
	ModeAll = iota
	ModeRoundRobin
	ModeHostPool
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "", "nsq topic")
	channel     = flag.String("channel", "nsq_to_http", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	numPublishers = flag.Int("n", 100, "number of concurrent publishers")
	mode          = flag.String("mode", "hostpool", "the upstream request mode options: multicast, round-robin, hostpool (default), epsilon-greedy")
	sample        = flag.Float64("sample", 1.0, "% of messages to publish (float b/w 0 -> 1)")
	// TODO: remove; deprecated in favor of http-client-connect-timeout, http-client-request-timeout
	httpTimeout        = flag.Duration("http-timeout", 20*time.Second, "timeout for HTTP connect/read/write (each)")
	httpConnectTimeout = flag.Duration("http-client-connect-timeout", 2*time.Second, "timeout for HTTP connect")
	httpRequestTimeout = flag.Duration("http-client-request-timeout", 20*time.Second, "timeout for HTTP request")
	statusEvery        = flag.Int("status-every", 250, "the # of requests between logging status (per handler), 0 disables")
	contentType        = flag.String("content-type", "application/octet-stream", "the Content-Type used for POST requests")

	getAddrs         = app.StringArray{}
	postAddrs        = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}

	// TODO: remove, deprecated
	roundRobin         = flag.Bool("round-robin", false, "(deprecated) use --mode=round-robin, enable round robin mode")
	maxBackoffDuration = flag.Duration("max-backoff-duration", 120*time.Second, "(deprecated) use --consumer-opt=max_backoff_duration,X")
	throttleFraction   = flag.Float64("throttle-fraction", 1.0, "(deprecated) use --sample=X, publish only a fraction of messages")
	httpTimeoutMs      = flag.Int("http-timeout-ms", 20000, "(deprecated) use --http-timeout=X, timeout for HTTP connect/read/write (each)")
)

func init() {
	flag.Var(&postAddrs, "post", "HTTP address to make a POST request to.  data will be in the body (may be given multiple times)")
	flag.Var(&getAddrs, "get", "HTTP address to make a GET request to. '%s' will be printf replaced with data (may be given multiple times)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type Publisher interface {
	Publish(string, []byte) error
}

type PublishHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	counter uint64

	Publisher
	addresses app.StringArray
	mode      int
	hostPool  hostpool.HostPool

	perAddressStatus map[string]*timer_metrics.TimerMetrics
	timermetrics     *timer_metrics.TimerMetrics
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	if *sample < 1.0 && rand.Float64() > *sample {
		return nil
	}

	startTime := time.Now()
	switch ph.mode {
	case ModeAll:
		for _, addr := range ph.addresses {
			st := time.Now()
			err := ph.Publish(addr, m.Body)
			if err != nil {
				return err
			}
			ph.perAddressStatus[addr].Status(st)
		}
	case ModeRoundRobin:
		counter := atomic.AddUint64(&ph.counter, 1)
		idx := counter % uint64(len(ph.addresses))
		addr := ph.addresses[idx]
		err := ph.Publish(addr, m.Body)
		if err != nil {
			return err
		}
		ph.perAddressStatus[addr].Status(startTime)
	case ModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		addr := hostPoolResponse.Host()
		err := ph.Publish(addr, m.Body)
		hostPoolResponse.Mark(err)
		if err != nil {
			return err
		}
		ph.perAddressStatus[addr].Status(startTime)
	}
	ph.timermetrics.Status(startTime)

	return nil
}

type PostPublisher struct{}

func (p *PostPublisher) Publish(addr string, msg []byte) error {
	buf := bytes.NewBuffer(msg)
	resp, err := HTTPPost(addr, buf)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("got status code %d", resp.StatusCode)
	}
	return nil
}

type GetPublisher struct{}

func (p *GetPublisher) Publish(addr string, msg []byte) error {
	endpoint := fmt.Sprintf(addr, url.QueryEscape(string(msg)))
	resp, err := HTTPGet(endpoint)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("got status code %d", resp.StatusCode)
	}
	return nil
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
	// TODO: remove, deprecated
	flag.Var(&nsq.ConfigFlag{cfg}, "reader-opt", "(deprecated) use --consumer-opt")
	flag.Var(&nsq.ConfigFlag{cfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")

	var publisher Publisher
	var addresses app.StringArray
	var selectedMode int

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_http v%s\n", version.Binary)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatal("--topic and --channel are required")
	}

	if *contentType != flag.Lookup("content-type").DefValue {
		if len(postAddrs) == 0 {
			log.Fatal("--content-type only used with --post")
		}
		if len(*contentType) == 0 {
			log.Fatal("--content-type requires a value when used")
		}
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(getAddrs) == 0 && len(postAddrs) == 0 {
		log.Fatal("--get or --post required")
	}
	if len(getAddrs) > 0 && len(postAddrs) > 0 {
		log.Fatal("use --get or --post not both")
	}
	if len(getAddrs) > 0 {
		for _, get := range getAddrs {
			if strings.Count(get, "%s") != 1 {
				log.Fatal("invalid GET address - must be a printf string")
			}
		}
	}

	switch *mode {
	case "multicast":
		log.Printf("WARNING: multicast mode is deprecated in favor of using separate nsq_to_http on different channels (and will be dropped in a future release)")
		selectedMode = ModeAll
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool", "epsilon-greedy":
		selectedMode = ModeHostPool
	}

	// TODO: remove, deprecated
	if hasArg("--round-robin") {
		log.Printf("WARNING: --round-robin is deprecated in favor of --mode=round-robin")
		selectedMode = ModeRoundRobin
	}

	// TODO: remove, deprecated
	if hasArg("throttle-fraction") {
		log.Printf("WARNING: --throttle-fraction is deprecatedin favor of --sample=X")
		*sample = *throttleFraction
	}

	if *sample > 1.0 || *sample < 0.0 {
		log.Fatal("ERROR: --sample must be between 0.0 and 1.0")
	}

	// TODO: remove, deprecated
	if hasArg("http-timeout-ms") {
		log.Printf("WARNING: --http-timeout-ms is deprecated in favor of --http-timeout=X")
		*httpTimeout = time.Duration(*httpTimeoutMs) * time.Millisecond
	}

	// TODO: remove, deprecated
	if hasArg("http-timeout") {
		log.Printf("WARNING: --http-timeout is deprecated in favor of --http-client-connect-timeout=X and --http-client-request-timeout=Y")
		*httpConnectTimeout = *httpTimeout
		*httpRequestTimeout = *httpTimeout
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	if len(postAddrs) > 0 {
		publisher = &PostPublisher{}
		addresses = postAddrs
	} else {
		publisher = &GetPublisher{}
		addresses = getAddrs
	}

	cfg.UserAgent = fmt.Sprintf("nsq_to_http/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	// TODO: remove, deprecated
	if hasArg("max-backoff-duration") {
		log.Printf("WARNING: --max-backoff-duration is deprecated in favor of --consumer-opt=max_backoff_duration,X")
		cfg.MaxBackoffDuration = *maxBackoffDuration
	}

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	perAddressStatus := make(map[string]*timer_metrics.TimerMetrics)
	if len(addresses) == 1 {
		// disable since there is only one address
		perAddressStatus[addresses[0]] = timer_metrics.NewTimerMetrics(0, "")
	} else {
		for _, a := range addresses {
			perAddressStatus[a] = timer_metrics.NewTimerMetrics(*statusEvery,
				fmt.Sprintf("[%s]:", a))
		}
	}

	hostPool := hostpool.New(addresses)
	if *mode == "epsilon-greedy" {
		hostPool = hostpool.NewEpsilonGreedy(addresses, 0, &hostpool.LinearEpsilonValueCalculator{})
	}

	handler := &PublishHandler{
		Publisher:        publisher,
		addresses:        addresses,
		mode:             selectedMode,
		hostPool:         hostPool,
		perAddressStatus: perAddressStatus,
		timermetrics:     timer_metrics.NewTimerMetrics(*statusEvery, "[aggregate]:"),
	}
	consumer.AddConcurrentHandlers(handler, *numPublishers)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-termChan:
			consumer.Stop()
		}
	}
}
