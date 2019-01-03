package main

import (
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
)

type TopicDiscoverer struct {
	logf     lg.AppLogFunc
	opts     *Options
	ci       *clusterinfo.ClusterInfo
	topics   map[string]*FileLogger
	hupChan  chan os.Signal
	termChan chan os.Signal
	wg       sync.WaitGroup
	cfg      *nsq.Config
}

func newTopicDiscoverer(logf lg.AppLogFunc, opts *Options, cfg *nsq.Config, hupChan chan os.Signal, termChan chan os.Signal) *TopicDiscoverer {
	client := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	return &TopicDiscoverer{
		logf:     logf,
		opts:     opts,
		ci:       clusterinfo.New(nil, client),
		topics:   make(map[string]*FileLogger),
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
			t.logf(lg.WARN, "skipping topic %s (doesn't match pattern %s)", topic, t.opts.TopicPattern)
			continue
		}

		fl, err := NewFileLogger(t.logf, t.opts, topic, t.cfg)
		if err != nil {
			t.logf(lg.ERROR, "couldn't create logger for new topic %s: %s", topic, err)
			continue
		}
		t.topics[topic] = fl

		t.wg.Add(1)
		go func(fl *FileLogger) {
			fl.router()
			t.wg.Done()
		}(fl)
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
				t.logf(lg.ERROR, "could not retrieve topic list: %s", err)
				continue
			}
			t.updateTopics(newTopics)
		case <-t.termChan:
			for _, fl := range t.topics {
				close(fl.termChan)
			}
			break forloop
		case <-t.hupChan:
			for _, fl := range t.topics {
				fl.hupChan <- true
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
