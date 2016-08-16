package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/absolute8511/glog"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqlookupd"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
)

var (
	flagSet = flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")
	verbose     = flagSet.Bool("verbose", false, "enable verbose logging")

	tcpAddress         = flagSet.String("tcp-address", "0.0.0.0:4160", "<addr>:<port> to listen on for TCP clients")
	httpAddress        = flagSet.String("http-address", "0.0.0.0:4161", "<addr>:<port> to listen on for HTTP clients")
	rpcPort            = flagSet.String("rpc-port", "", "<port> to listen on for Rpc call")
	broadcastAddress   = flagSet.String("broadcast-address", "", "address of this lookupd node, (default to the OS hostname)")
	broadcastInterface = flagSet.String("broadcast-interface", "", "address of this lookupd node, (default to the OS hostname)")
	reverseProxyPort   = flagSet.String("reverse-proxy-port", "", "<port> for reverse proxy")

	clusterLeadershipAddresses = flagSet.String("cluster-leadership-addresses", "", " the cluster leadership server list")
	clusterID                  = flagSet.String("cluster-id", "nsq-test-cluster", "the cluster id used for separating different nsq cluster.")

	inactiveProducerTimeout = flagSet.Duration("inactive-producer-timeout", 60*time.Second, "duration of time a producer will remain in the active list since its last ping")
	nsqdPingTimeout         = flagSet.Duration("nsqd-ping-timeout", 15*time.Second, "duration of nsqd ping timeout, should be at least twice as the nsqd ping interval")
	tombstoneLifetime       = flagSet.Duration("tombstone-lifetime", 45*time.Second, "duration of time a producer will remain tombstoned if registration remains")
	logLevel                = flagSet.Int("log-level", 1, "log verbose level")
	logDir                  = flagSet.String("log-dir", "", "directory for log file")
)

func init() {
}

type program struct {
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	defer glog.Flush()
	prg := &program{}
	if err := svc.Run(prg, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGINT); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	glog.InitWithFlag(flagSet)

	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := nsqlookupd.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	if opts.LogDir != "" {
		glog.SetGLogDir(opts.LogDir)
	}
	nsqlookupd.SetLogger(opts)
	glog.StartWorker(time.Second * 2)
	daemon := nsqlookupd.New(opts)

	daemon.Main()
	p.nsqlookupd = daemon
	return nil
}

func (p *program) Stop() error {
	if p.nsqlookupd != nil {
		p.nsqlookupd.Exit()
	}
	return nil
}
