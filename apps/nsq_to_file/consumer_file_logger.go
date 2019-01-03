package main

import (
	"github.com/nsqio/go-nsq"
)

type ConsumerFileLogger struct {
	F *FileLogger
	C *nsq.Consumer
}

func newConsumerFileLogger(opts *Options, topic string, cfg *nsq.Config) (*ConsumerFileLogger, error) {
	f, err := NewFileLogger(opts, topic)
	if err != nil {
		return nil, err
	}

	c, err := nsq.NewConsumer(topic, opts.Channel, cfg)
	if err != nil {
		return nil, err
	}

	c.AddHandler(f)

	err = c.ConnectToNSQDs(opts.NSQDTCPAddrs)
	if err != nil {
		return nil, err
	}

	err = c.ConnectToNSQLookupds(opts.NSQLookupdHTTPAddrs)
	if err != nil {
		return nil, err
	}

	return &ConsumerFileLogger{
		C: c,
		F: f,
	}, nil
}
