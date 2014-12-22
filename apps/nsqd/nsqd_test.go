package main

import (
	"crypto/tls"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/bitly/nsq/nsqd"
	"github.com/mreiferson/go-options"
)

func TestConfigFlagParsing(t *testing.T) {
	flagSet := nsqFlagset()
	flagSet.Parse([]string{})

	var cfg config
	f, err := os.Open("../../contrib/nsqd.cfg.example")
	if err != nil {
		t.Fatalf("%s", err)
	}
	toml.DecodeReader(f, &cfg)
	cfg.Validate()

	opts := nsqd.NewNSQDOptions()
	options.Resolve(opts, flagSet, cfg)
	nsqd.NewNSQD(opts)

	if opts.TLSMinVersion != tls.VersionTLS10 {
		t.Errorf("min %#v not expected %#v", opts.TLSMinVersion, tls.VersionTLS10)
	}
}
