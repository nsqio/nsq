package main

import (
	"log"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
)

type TopicDiscoverer struct {
	opts     *Options
	ci       *clusterinfo.ClusterInfo
	topics   map[string]*ConsumerFileLogger
	hupChan  chan os.Signal
	termChan chan os.Signal
	wg       sync.WaitGroup
	cfg      *nsq.Config
}

func newTopicDiscoverer(opts *Options, cfg *nsq.Config, hupChan chan os.Signal, termChan chan os.Signal) *TopicDiscoverer {
	client := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	return &TopicDiscoverer{
		opts:     opts,
		ci:       clusterinfo.New(nil, client),
		topics:   make(map[string]*ConsumerFileLogger),
		hupChan:  hupChan,
		termChan: termChan,
		cfg:      cfg,
	}
}

func (t *TopicDiscoverer) updateTopics(topics []string) {
	for _, topic := range topics {
		if _, ok := t.topics[topic]; ok {
			continue
		}

		if !t.isTopicAllowed(topic) {
			log.Printf("skipping topic %s (doesn't match pattern %s)", topic, t.opts.TopicPattern)
			continue
		}

		cfl, err := newConsumerFileLogger(t.opts, topic, t.cfg)
		if err != nil {
			log.Printf("ERROR: couldn't create logger for new topic %s: %s", topic, err)
			continue
		}
		t.topics[topic] = cfl

		t.wg.Add(1)
		go func(cfl *ConsumerFileLogger) {
			cfl.F.router(cfl.C)
			t.wg.Done()
		}(cfl)
	}
}

func (t *TopicDiscoverer) run() {
	var ticker <-chan time.Time
	if len(t.opts.Topics) == 0 {
		ticker = time.Tick(t.opts.TopicRefreshInterval)
	}
	t.updateTopics(t.opts.Topics)
forloop:
	for {
		select {
		case <-ticker:
			newTopics, err := t.ci.GetLookupdTopics(t.opts.NSQLookupdHTTPAddrs)
			if err != nil {
				log.Printf("ERROR: could not retrieve topic list: %s", err)
				continue
			}
			t.updateTopics(newTopics)
		case <-t.termChan:
			for _, cfl := range t.topics {
				close(cfl.F.termChan)
			}
			break forloop
		case <-t.hupChan:
			for _, cfl := range t.topics {
				cfl.F.hupChan <- true
			}
		}
	}
	t.wg.Wait()
}

func (t *TopicDiscoverer) isTopicAllowed(topic string) bool {
	if t.opts.TopicPattern == "" {
		return true
	}
	match, err := regexp.MatchString(t.opts.TopicPattern, topic)
	if err != nil {
		return false
	}
	return match
}
