package main

import (
	"testing"

	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqlookupd"
)

func TestConfigFlagParsing(t *testing.T) {
	opts := nsqlookupd.NewOptions()
	opts.Logger = test.NewTestLogger(t)

	flagSet := nsqlookupdFlagSet(opts)
	flagSet.Parse([]string{})

	cfg := config{"log_level": "debug"}
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	if opts.LogLevel != lg.DEBUG {
		t.Fatalf("log level: want debug, got %s", opts.LogLevel.String())
	}
}
