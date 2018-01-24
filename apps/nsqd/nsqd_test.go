package main

import (
	"crypto/tls"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/nsqd"
)

func TestConfigFlagParsing(t *testing.T) {
	opts := nsqd.NewOptions()

	flagSet := nsqdFlagSet(opts)
	flagSet.Parse([]string{})

	var cfg config
	f, err := os.Open("../../contrib/nsqd.cfg.example")
	if err != nil {
		t.Fatalf("%s", err)
	}
	toml.DecodeReader(f, &cfg)
	cfg.Validate()

	options.Resolve(opts, flagSet, cfg)
	nsqd.New(opts)

	if opts.TLSMinVersion != tls.VersionTLS10 {
		t.Errorf("min %#v not expected %#v", opts.TLSMinVersion, tls.VersionTLS10)
	}
}

func TestAuthRequiredFlagParsing_CommaSeparated(t *testing.T) {
	args := []string{"--auth-required=tcp,http,https"}

	opts := nsqd.NewOptions()
	flagSet := nsqdFlagSet(opts)
	err := flagSet.Parse(args)
	if err != nil {
		t.Fatal(err)
	}

	options.Resolve(opts, flagSet, config{})
	nsqd.New(opts)

	expected := uint16(nsqd.FlagTCPProtocol | nsqd.FlagHTTPProtocol | nsqd.FlagHTTPSProtocol)

	if expected != opts.AuthRequired {
		t.Fatalf("%v does not match expected %v", opts.AuthRequired, expected)
	}
}

func TestAuthRequiredFlagParsing_SpecifiedMultipleTimes(t *testing.T) {
	args := []string{"--auth-required=http", "--auth-required=https"}

	opts := nsqd.NewOptions()
	flagSet := nsqdFlagSet(opts)
	err := flagSet.Parse(args)
	if err != nil {
		t.Fatal(err)
	}

	options.Resolve(opts, flagSet, config{})
	nsqd.New(opts)

	expected := uint16(nsqd.FlagHTTPProtocol | nsqd.FlagHTTPSProtocol)

	if expected != opts.AuthRequired {
		t.Fatalf("%v does not match expected %v", opts.AuthRequired, expected)
	}
}

func TestAuthRequiredFlagParsing_DefaultTCP(t *testing.T) {
	var args []string

	opts := nsqd.NewOptions()
	flagSet := nsqdFlagSet(opts)
	err := flagSet.Parse(args)
	if err != nil {
		t.Fatal(err)
	}

	options.Resolve(opts, flagSet, config{})
	nsqd.New(opts)

	expected := uint16(nsqd.FlagTCPProtocol)

	if expected != opts.AuthRequired {
		t.Fatalf("%v does not match expected %v", opts.AuthRequired, expected)
	}
}
