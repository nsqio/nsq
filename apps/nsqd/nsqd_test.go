package main

import (
	"crypto/tls"
	"io/ioutil"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/test"
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
	opts.Logger = test.NewTestLogger(t)
	tmpDir, err := ioutil.TempDir("", "nsq-test-")
	if err != nil {
		panic(err)
	}
	opts.DataPath = tmpDir
	nsqd.New(opts)
	defer os.RemoveAll(tmpDir)

	if opts.TLSMinVersion != tls.VersionTLS10 {
		t.Errorf("min %#v not expected %#v", opts.TLSMinVersion, tls.VersionTLS10)
	}
}
