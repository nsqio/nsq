package main

import (
	"github.com/nsqio/go-nsq"
)

type ConsumerFileLogger struct {
	F *FileLogger
	C *nsq.Consumer
}

func newConsumerFileLogger(topic string, cfg *nsq.Config) (*ConsumerFileLogger, error) {
	f, err := NewFileLogger(*gzipEnabled, *gzipLevel, *filenameFormat, topic)
	if err != nil {
		return nil, err
	}

	c, err := nsq.NewConsumer(topic, *channel, cfg)
	if err != nil {
		return nil, err
	}

	c.AddHandler(f)

	err = c.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		return nil, err
	}

	err = c.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		return nil, err
	}

	return &ConsumerFileLogger{
		C: c,
		F: f,
	}, nil
}
