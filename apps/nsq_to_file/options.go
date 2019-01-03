package main

import "time"

type Options struct {
	Topics               []string      `flag:"topic"`
	TopicPattern         string        `flag:"topic-pattern"`
	TopicRefreshInterval time.Duration `flag:"topic-refresh"`
	Channel              string        `flag:"channel"`

	NSQDTCPAddrs             []string      `flag:"nsqd-tcp-address"`
	NSQLookupdHTTPAddrs      []string      `flag:"lookupd-http-address"`
	ConsumerOpts             []string      `flag:"consumer-opt"`
	MaxInFlight              int           `flag:"max-in-flight"`
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout"`

	LogPrefix      string        `flag:"log-prefix"`
	LogLevel       string        `flag:"log-level"`
	OutputDir      string        `flag:"output-dir"`
	WorkDir        string        `flag:"work-dir"`
	DatetimeFormat string        `flag:"datetime-format"`
	FilenameFormat string        `flag:"filename-format"`
	HostIdentifier string        `flag:"host-identifier"`
	GZIPLevel      int           `flag:"gzip-level"`
	GZIP           bool          `flag:"gzip"`
	SkipEmptyFiles bool          `flag:"skip-empty-files"`
	RotateSize     int64         `flag:"rotate-size"`
	RotateInterval time.Duration `flag:"rotate-interval"`
	SyncInterval   time.Duration `flag:"sync-interval"`
}

func NewOptions() *Options {
	return &Options{
		LogPrefix:                "[nsq_to_file] ",
		LogLevel:                 "info",
		Channel:                  "nsq_to_file",
		MaxInFlight:              200,
		OutputDir:                "/tmp",
		DatetimeFormat:           "%Y-%m-%d_%H",
		FilenameFormat:           "<TOPIC>.<HOST><REV>.<DATETIME>.log",
		GZIPLevel:                6,
		TopicRefreshInterval:     time.Minute,
		SyncInterval:             30 * time.Second,
		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,
	}
}
