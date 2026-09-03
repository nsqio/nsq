package main

import (
	"crypto/tls"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
)

func TestConfigFlagParsing(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = test.NewTestLogger(t)

	flagSet := nsqdFlagSet(opts)
	err := flagSet.Parse([]string{})
	if err != nil {
		t.Fatalf("%s", err)
	}

	var cfg config
	f, err := os.Open("../../contrib/nsqd.cfg.example")
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Fatalf("%s", err)
		}
	}()
	_, err = toml.NewDecoder(f).Decode(&cfg)
	if err != nil {
		t.Fatalf("%s", err)
	}
	cfg["log_level"] = "debug"
	cfg["log_json"] = true
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	_, err = nsqd.New(opts)
	if err != nil {
		t.Fatalf("%s", err)
	}

	if opts.TLSMinVersion != tls.VersionTLS10 {
		t.Errorf("min %#v not expected %#v", opts.TLSMinVersion, tls.VersionTLS10)
	}
	if opts.LogLevel != lg.DEBUG {
		t.Fatalf("log level: want debug, got %s", opts.LogLevel.String())
	}
	if !opts.LogJSON {
		t.Fatalf("log json: want true, got %v", opts.LogJSON)
	}
}
