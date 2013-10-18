// This is an NSQ client that reads the specified topic/channel
// and performs HTTP requests (GET/POST) to the specified endpoints

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
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
	mode          = flag.String("mode", "round-robin", "the upstream request mode options: multicast, round-robin, hostpool")
	sample        = flag.Float64("sample", 1.0, "% of messages to publish (float b/w 0 -> 1)")
	httpTimeout   = flag.Duration("http-timeout", 20*time.Second, "timeout for HTTP connect/read/write (each)")
	statusEvery   = flag.Int("status-every", 250, "the # of requests between logging status (per handler), 0 disables")
	contentType   = flag.String("content-type", "application/octet-stream", "the Content-Type used for POST requests")

	readerOpts       = util.StringArray{}
	getAddrs         = util.StringArray{}
	postAddrs        = util.StringArray{}
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}

	// TODO: remove, deprecated
	roundRobin         = flag.Bool("round-robin", false, "(deprecated) use --mode=round-robin, enable round robin mode")
	maxBackoffDuration = flag.Duration("max-backoff-duration", 120*time.Second, "(deprecated) use --reader-opt=max_backoff_duration=X, the maximum backoff duration")
	verbose            = flag.Bool("verbose", false, "(depgrecated) use --reader-opt=verbose")
	throttleFraction   = flag.Float64("throttle-fraction", 1.0, "(deprecated) use --sample=X, publish only a fraction of messages")
	httpTimeoutMs      = flag.Int("http-timeout-ms", 20000, "(deprecated) use --http-timeout=X, timeout for HTTP connect/read/write (each)")
)

func init() {
	flag.Var(&readerOpts, "reader-opt", "option to passthrough to nsq.Reader (may be given multiple times)")
	flag.Var(&postAddrs, "post", "HTTP address to make a POST request to.  data will be in the body (may be given multiple times)")
	flag.Var(&getAddrs, "get", "HTTP address to make a GET request to. '%s' will be printf replaced with data (may be given multiple times)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type Durations []time.Duration

func (s Durations) Len() int {
	return len(s)
}

func (s Durations) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Durations) Less(i, j int) bool {
	return s[i] < s[j]
}

type Publisher interface {
	Publish(string, []byte) error
}

type PublishHandler struct {
	Publisher
	addresses util.StringArray
	counter   uint64
	mode      int
	hostPool  hostpool.HostPool
	reqs      Durations
	id        int
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	var startTime time.Time

	if *sample < 1.0 && rand.Float64() > *sample {
		return nil
	}

	if *statusEvery > 0 {
		startTime = time.Now()
	}

	switch ph.mode {
	case ModeAll:
		for _, addr := range ph.addresses {
			err := ph.Publish(addr, m.Body)
			if err != nil {
				return err
			}
		}
	case ModeRoundRobin:
		idx := ph.counter % uint64(len(ph.addresses))
		err := ph.Publish(ph.addresses[idx], m.Body)
		if err != nil {
			return err
		}
		ph.counter++
	case ModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		err := ph.Publish(hostPoolResponse.Host(), m.Body)
		hostPoolResponse.Mark(err)
		if err != nil {
			return err
		}
	}

	if *statusEvery > 0 {
		duration := time.Now().Sub(startTime)
		ph.reqs = append(ph.reqs, duration)
	}

	if *statusEvery > 0 && len(ph.reqs) >= *statusEvery {
		var total time.Duration
		for _, v := range ph.reqs {
			total += v
		}
		avgMs := (total.Seconds() * 1000) / float64(len(ph.reqs))

		sort.Sort(ph.reqs)
		p95Ms := percentile(95.0, ph.reqs, len(ph.reqs)).Seconds() * 1000
		p99Ms := percentile(99.0, ph.reqs, len(ph.reqs)).Seconds() * 1000

		log.Printf("handler(%d): finished %d requests - 99th: %.02fms - 95th: %.02fms - avg: %.02fms",
			ph.id, *statusEvery, p99Ms, p95Ms, avgMs)

		ph.reqs = ph.reqs[:0]
	}

	return nil
}

func percentile(perc float64, arr []time.Duration, length int) time.Duration {
	indexOfPerc := int(math.Ceil(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

type PostPublisher struct{}

func (p *PostPublisher) Publish(addr string, msg []byte) error {
	buf := bytes.NewBuffer(msg)
	resp, err := HttpPost(addr, buf)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("got status code %d", resp.StatusCode))
	}
	return nil
}

type GetPublisher struct{}

func (p *GetPublisher) Publish(addr string, msg []byte) error {
	endpoint := fmt.Sprintf(addr, url.QueryEscape(string(msg)))
	resp, err := HttpGet(endpoint)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("got status code %d", resp.StatusCode))
	}
	return nil
}

func hasArg(s string) bool {
	for _, arg := range os.Args {
		if strings.Contains(arg, s) {
			return true
		}
	}
	return false
}

func main() {
	var publisher Publisher
	var addresses util.StringArray
	var selectedMode int

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_http v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *contentType != flag.Lookup("content-type").DefValue {
		if len(postAddrs) == 0 {
			log.Fatalf("--content-type only used with --post")
		}
		if len(*contentType) == 0 {
			log.Fatalf("--content-type requires a value when used")
		}
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(getAddrs) == 0 && len(postAddrs) == 0 {
		log.Fatalf("--get or --post required")
	}
	if len(getAddrs) > 0 && len(postAddrs) > 0 {
		log.Fatalf("use --get or --post not both")
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
	case "hostpool":
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
		log.Fatalf("ERROR: --sample must be between 0.0 and 1.0")
	}

	// TODO: remove, deprecated
	if hasArg("http-timeout-ms") {
		log.Printf("WARNING: --http-timeout-ms is deprecated in favor of --http-timeout=X")
		*httpTimeout = time.Duration(*httpTimeoutMs) * time.Millisecond
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	if len(postAddrs) > 0 {
		publisher = &PostPublisher{}
		addresses = postAddrs
	} else {
		publisher = &GetPublisher{}
		addresses = getAddrs
	}

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	err = util.ParseReaderOpts(r, readerOpts)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)

	// TODO: remove, deprecated
	if hasArg("verbose") {
		log.Printf("WARNING: --verbose is deprecated in favor of --reader-opt=verbose")
		r.Configure("verbose", true)
	}

	// TODO: remove, deprecated
	if hasArg("max-backoff-duration") {
		log.Printf("WARNING: --max-backoff-duration is deprecated in favor of --reader-opt=max_backoff_duration=X")
		r.Configure("max_backoff_duration", *maxBackoffDuration)
	}

	for i := 0; i < *numPublishers; i++ {
		handler := &PublishHandler{
			Publisher: publisher,
			addresses: addresses,
			mode:      selectedMode,
			reqs:      make(Durations, 0, *statusEvery),
			id:        i,
			hostPool:  hostpool.New(addresses),
		}
		r.AddHandler(handler)
	}

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for {
		select {
		case <-r.ExitChan:
			return
		case <-termChan:
			r.Stop()
		}
	}
}
