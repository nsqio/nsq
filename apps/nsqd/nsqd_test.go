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
	flagSet := nsqFlagset()
	flagSet.Parse([]string{})

	var cfg config
	f, err := os.Open("../../contrib/nsqd.cfg.example")
	if err != nil {
		t.Fatalf("%s", err)
	}
	toml.DecodeReader(f, &cfg)
	cfg.Validate()

	opts := nsqd.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	nsqd.New(opts)

	if opts.TLSMinVersion != tls.VersionTLS10 {
		t.Errorf("min %#v not expected %#v", opts.TLSMinVersion, tls.VersionTLS10)
	}
}

func TestAuthRequiredFlagParsing_CommaSeparated(t *testing.T) {
	os.Args = []string{"", "--auth-required=tcp,http,https"}
	flagSet := nsqFlagset()
	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		t.Fatal(err)
	}

	opts := nsqd.NewOptions()
	options.Resolve(opts, flagSet, config{})

	expected := uint16(nsqd.FlagTCPProtocol | nsqd.FlagHTTPProtocol | nsqd.FlagHTTPSProtocol)

	if expected != opts.AuthRequired {
		t.Fatalf("%v does not match expected %v", opts.AuthRequired, expected)
	}
}

func TestAuthRequiredFlagParsing_SpecifiedMultipleTimes(t *testing.T) {
	os.Args = []string{"", "--auth-required=http", "--auth-required=https"}
	flagSet := nsqFlagset()
	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		t.Fatal(err)
	}

	opts := nsqd.NewOptions()
	options.Resolve(opts, flagSet, config{})

	expected := uint16(nsqd.FlagHTTPProtocol | nsqd.FlagHTTPSProtocol)

	if expected != opts.AuthRequired {
		t.Fatalf("%v does not match expected %v", opts.AuthRequired, expected)
	}
}

func TestAuthRequiredFlagParsing_DefaultTCP(t *testing.T) {
	os.Args = []string{""}
	flagSet := nsqFlagset()
	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		t.Fatal(err)
	}

	opts := nsqd.NewOptions()
	options.Resolve(opts, flagSet, config{})

	expected := uint16(nsqd.FlagTCPProtocol)

	if expected != opts.AuthRequired {
		t.Fatalf("%v does not match expected %v", opts.AuthRequired, expected)
	}
}
