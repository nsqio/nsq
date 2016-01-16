// This is an NSQ client that reads the specified topic/channel
// and re-publishes the messages to destination nsqd via TCP

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-simplejson"
	"github.com/bitly/timer_metrics"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

const (
	ModeRoundRobin = iota
	ModeHostPool
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	topic       = flag.String("topic", "", "nsq topic")
	channel     = flag.String("channel", "nsq_to_nsq", "nsq channel")
	destTopic   = flag.String("destination-topic", "", "destination nsq topic")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	statusEvery = flag.Int("status-every", 250, "the # of requests between logging status (per destination), 0 disables")
	mode        = flag.String("mode", "hostpool", "the upstream request mode options: round-robin, hostpool (default), epsilon-greedy")

	nsqdTCPAddrs        = app.StringArray{}
	lookupdHTTPAddrs    = app.StringArray{}
	destNsqdTCPAddrs    = app.StringArray{}
	whitelistJSONFields = app.StringArray{}

	requireJSONField = flag.String("require-json-field", "", "for JSON messages: only pass messages that contain this field")
	requireJSONValue = flag.String("require-json-value", "", "for JSON messages: only pass messages in which the required field has this value")

	// TODO: remove, deprecated
	maxBackoffDuration = flag.Duration("max-backoff-duration", 120*time.Second, "(deprecated) use --consumer-opt=max_backoff_duration,X")
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&destNsqdTCPAddrs, "destination-nsqd-tcp-address", "destination nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")

	flag.Var(&whitelistJSONFields, "whitelist-json-field", "for JSON messages: pass this field (may be given multiple times)")
}

type PublishHandler struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	counter uint64

	addresses app.StringArray
	producers map[string]*nsq.Producer
	mode      int
	hostPool  hostpool.HostPool
	respChan  chan *nsq.ProducerTransaction

	requireJSONValueParsed   bool
	requireJSONValueIsNumber bool
	requireJSONNumber        float64

	perAddressStatus map[string]*timer_metrics.TimerMetrics
	timermetrics     *timer_metrics.TimerMetrics
}

func (ph *PublishHandler) responder() {
	var msg *nsq.Message
	var startTime time.Time
	var address string
	var hostPoolResponse hostpool.HostPoolResponse

	for t := range ph.respChan {
		switch ph.mode {
		case ModeRoundRobin:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = nil
			address = t.Args[2].(string)
		case ModeHostPool:
			msg = t.Args[0].(*nsq.Message)
			startTime = t.Args[1].(time.Time)
			hostPoolResponse = t.Args[2].(hostpool.HostPoolResponse)
			address = hostPoolResponse.Host()
		}

		success := t.Error == nil

		if hostPoolResponse != nil {
			if !success {
				hostPoolResponse.Mark(errors.New("failed"))
			} else {
				hostPoolResponse.Mark(nil)
			}
		}

		if success {
			msg.Finish()
		} else {
			msg.Requeue(-1)
		}

		ph.perAddressStatus[address].Status(startTime)
		ph.timermetrics.Status(startTime)
	}
}

func (ph *PublishHandler) shouldPassMessage(jsonMsg *simplejson.Json) (bool, bool) {
	pass := true
	backoff := false

	if *requireJSONField == "" {
		return pass, backoff
	}

	if *requireJSONValue != "" && !ph.requireJSONValueParsed {
		// cache conversion in case needed while filtering json
		var err error
		ph.requireJSONNumber, err = strconv.ParseFloat(*requireJSONValue, 64)
		ph.requireJSONValueIsNumber = (err == nil)
		ph.requireJSONValueParsed = true
	}

	jsonVal, ok := jsonMsg.CheckGet(*requireJSONField)
	if !ok {
		pass = false
		if *requireJSONValue != "" {
			log.Printf("ERROR: missing field to check required value")
			backoff = true
		}
	} else if *requireJSONValue != "" {
		// if command-line argument can't convert to float, then it can't match a number
		// if it can, also integers (up to 2^53 or so) can be compared as float64
		if strVal, err := jsonVal.String(); err == nil {
			if strVal != *requireJSONValue {
				pass = false
			}
		} else if ph.requireJSONValueIsNumber {
			floatVal, err := jsonVal.Float64()
			if err != nil || ph.requireJSONNumber != floatVal {
				pass = false
			}
		} else {
			// json value wasn't a plain string, and argument wasn't a number
			// give up on comparisons of other types
			pass = false
		}
	}

	return pass, backoff
}

func filterMessage(jsonMsg *simplejson.Json, rawMsg []byte) ([]byte, error) {
	if len(whitelistJSONFields) == 0 {
		// no change
		return rawMsg, nil
	}

	msg, err := jsonMsg.Map()
	if err != nil {
		return nil, errors.New("json is not an object")
	}

	newMsg := make(map[string]interface{}, len(whitelistJSONFields))

	for _, key := range whitelistJSONFields {
		value, ok := msg[key]
		if ok {
			// avoid printing int as float (go 1.0)
			switch tvalue := value.(type) {
			case float64:
				ivalue := int64(tvalue)
				if float64(ivalue) == tvalue {
					newMsg[key] = ivalue
				} else {
					newMsg[key] = tvalue
				}
			default:
				newMsg[key] = value
			}
		}
	}

	newRawMsg, err := json.Marshal(newMsg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal filtered message %v", newMsg)
	}
	return newRawMsg, nil
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	var err error
	msgBody := m.Body

	if *requireJSONField != "" || len(whitelistJSONFields) > 0 {
		var jsonMsg *simplejson.Json
		jsonMsg, err = simplejson.NewJson(m.Body)
		if err != nil {
			log.Printf("ERROR: Unable to decode json: %s", m.Body)
			return nil
		}

		if pass, backoff := ph.shouldPassMessage(jsonMsg); !pass {
			if backoff {
				return errors.New("backoff")
			}
			return nil
		}

		msgBody, err = filterMessage(jsonMsg, m.Body)
		if err != nil {
			log.Printf("ERROR: filterMessage() failed: %s", err)
			return err
		}
	}

	startTime := time.Now()

	switch ph.mode {
	case ModeRoundRobin:
		counter := atomic.AddUint64(&ph.counter, 1)
		idx := counter % uint64(len(ph.addresses))
		addr := ph.addresses[idx]
		p := ph.producers[addr]
		err = p.PublishAsync(*destTopic, msgBody, ph.respChan, m, startTime, addr)
	case ModeHostPool:
		hostPoolResponse := ph.hostPool.Get()
		p := ph.producers[hostPoolResponse.Host()]
		err = p.PublishAsync(*destTopic, msgBody, ph.respChan, m, startTime, hostPoolResponse)
		if err != nil {
			hostPoolResponse.Mark(err)
		}
	}

	if err != nil {
		return err
	}
	m.DisableAutoResponse()
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
	var selectedMode int

	cCfg := nsq.NewConfig()
	pCfg := nsq.NewConfig()

	// TODO: remove, deprecated
	flag.Var(&nsq.ConfigFlag{cCfg}, "reader-opt", "(deprecated) use --consumer-opt")
	flag.Var(&nsq.ConfigFlag{cCfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Var(&nsq.ConfigFlag{pCfg}, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_nsq v%s\n", version.Binary)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatal("--topic and --channel are required")
	}

	if *destTopic == "" {
		*destTopic = *topic
	}

	if !protocol.IsValidTopicName(*topic) {
		log.Fatal("--topic is invalid")
	}

	if !protocol.IsValidTopicName(*destTopic) {
		log.Fatal("--destination-topic is invalid")
	}

	if !protocol.IsValidChannelName(*channel) {
		log.Fatal("--channel is invalid")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(destNsqdTCPAddrs) == 0 {
		log.Fatal("--destination-nsqd-tcp-address required")
	}

	switch *mode {
	case "round-robin":
		selectedMode = ModeRoundRobin
	case "hostpool", "epsilon-greedy":
		selectedMode = ModeHostPool
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	defaultUA := fmt.Sprintf("nsq_to_nsq/%s go-nsq/%s", version.Binary, nsq.VERSION)

	cCfg.UserAgent = defaultUA
	cCfg.MaxInFlight = *maxInFlight

	// TODO: remove, deprecated
	if hasArg("max-backoff-duration") {
		log.Printf("WARNING: --max-backoff-duration is deprecated in favor of --consumer-opt=max_backoff_duration,X")
		cCfg.MaxBackoffDuration = *maxBackoffDuration
	}

	consumer, err := nsq.NewConsumer(*topic, *channel, cCfg)
	if err != nil {
		log.Fatal(err)
	}

	pCfg.UserAgent = defaultUA

	producers := make(map[string]*nsq.Producer)
	for _, addr := range destNsqdTCPAddrs {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatalf("failed creating producer %s", err)
		}
		producers[addr] = producer
	}

	perAddressStatus := make(map[string]*timer_metrics.TimerMetrics)
	if len(destNsqdTCPAddrs) == 1 {
		// disable since there is only one address
		perAddressStatus[destNsqdTCPAddrs[0]] = timer_metrics.NewTimerMetrics(0, "")
	} else {
		for _, a := range destNsqdTCPAddrs {
			perAddressStatus[a] = timer_metrics.NewTimerMetrics(*statusEvery,
				fmt.Sprintf("[%s]:", a))
		}
	}

	hostPool := hostpool.New(destNsqdTCPAddrs)
	if *mode == "epsilon-greedy" {
		hostPool = hostpool.NewEpsilonGreedy(destNsqdTCPAddrs, 0, &hostpool.LinearEpsilonValueCalculator{})
	}

	handler := &PublishHandler{
		addresses:        destNsqdTCPAddrs,
		producers:        producers,
		mode:             selectedMode,
		hostPool:         hostPool,
		respChan:         make(chan *nsq.ProducerTransaction, len(destNsqdTCPAddrs)),
		perAddressStatus: perAddressStatus,
		timermetrics:     timer_metrics.NewTimerMetrics(*statusEvery, "[aggregate]:"),
	}
	consumer.AddConcurrentHandlers(handler, len(destNsqdTCPAddrs))

	for i := 0; i < len(destNsqdTCPAddrs); i++ {
		go handler.responder()
	}

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
