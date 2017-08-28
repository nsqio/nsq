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
	ci       *clusterinfo.ClusterInfo
	topics   map[string]*ConsumerFileLogger
	hupChan  chan os.Signal
	termChan chan os.Signal
	wg       sync.WaitGroup
	cfg      *nsq.Config
}

func newTopicDiscoverer(cfg *nsq.Config,
	hupChan chan os.Signal, termChan chan os.Signal,
	connectTimeout time.Duration, requestTimeout time.Duration) *TopicDiscoverer {
	return &TopicDiscoverer{
		ci:       clusterinfo.New(nil, http_api.NewClient(nil, connectTimeout, requestTimeout)),
		topics:   make(map[string]*ConsumerFileLogger),
		hupChan:  hupChan,
		termChan: termChan,
		cfg:      cfg,
	}
}

func (t *TopicDiscoverer) updateTopics(topics []string, pattern string) {
	for _, topic := range topics {
		if _, ok := t.topics[topic]; ok {
			continue
		}

		if !allowTopicName(pattern, topic) {
			log.Printf("skipping topic %s (doesn't match pattern %s)", topic, pattern)
			continue
		}

		cfl, err := newConsumerFileLogger(topic, t.cfg)
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

func (t *TopicDiscoverer) poller(addrs []string, sync bool, pattern string) {
	var ticker <-chan time.Time
	if sync {
		ticker = time.Tick(*topicPollRate)
	}
	for {
		select {
		case <-ticker:
			newTopics, err := t.ci.GetLookupdTopics(addrs)
			if err != nil {
				log.Printf("ERROR: could not retrieve topic list: %s", err)
				continue
			}
			t.updateTopics(newTopics, pattern)
		case <-t.termChan:
			for _, cfl := range t.topics {
				close(cfl.F.termChan)
			}
			break
		case <-t.hupChan:
			for _, cfl := range t.topics {
				cfl.F.hupChan <- true
			}
		}
	}
	t.wg.Wait()
}

func allowTopicName(pattern string, name string) bool {
	if pattern == "" {
		return true
	}
	match, err := regexp.MatchString(pattern, name)
	if err != nil {
		return false
	}
	return match
}
