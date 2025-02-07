package main

import (
	"crypto/tls"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
)

func TestConfigFlagParsing(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = test.NewTestLogger(t)

	flagSet := nsqdFlagSet(opts)
	flagSet.Parse([]string{})

	var cfg config
	f, err := os.Open("../../contrib/nsqd.cfg.example")
	if err != nil {
		t.Fatalf("%s", err)
	}
	defer f.Close()
	toml.NewDecoder(f).Decode(&cfg)
	cfg["log_level"] = "debug"
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	nsqd.New(opts)

	if opts.TLSMinVersion != tls.VersionTLS10 {
		t.Errorf("min %#v not expected %#v", opts.TLSMinVersion, tls.VersionTLS10)
	}
	if opts.LogLevel != lg.DEBUG {
		t.Fatalf("log level: want debug, got %s", opts.LogLevel.String())
	}
}

// Test generated using Keploy
func TestProgramHandle(t *testing.T) {
	p := &program{}
	err := p.Handle(os.Interrupt)
	if err != svc.ErrStop {
		t.Fatalf("Expected svc.ErrStop, got %v", err)
	}
}
