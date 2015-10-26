package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/absolute8511/nsq/nsqlookupd"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/version"
)

var (
	flagSet = flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")
	verbose     = flagSet.Bool("verbose", false, "enable verbose logging")

	tcpAddress       = flagSet.String("tcp-address", "0.0.0.0:4160", "<addr>:<port> to listen on for TCP clients")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:4161", "<addr>:<port> to listen on for HTTP clients")
	rpcAddress       = flagSet.String("rpc-address", "0.0.0.0:4162", "<addr>:<port> to listen on for RCP call")
	broadcastAddress = flagSet.String("broadcast-address", "", "address of this lookupd node, (default to the OS hostname)")

	etcdAddress = flagSet.String("etcd-address", "", "<addr1>:<port1>, <addr2>:<port2>  the etcd server list")
	clusterID   = flagSet.String("cluster-id", "nsq-test-cluster", "the cluster id used for separating different nsq cluster.")

	inactiveProducerTimeout = flagSet.Duration("inactive-producer-timeout", 300*time.Second, "duration of time a producer will remain in the active list since its last ping")
	tombstoneLifetime       = flagSet.Duration("tombstone-lifetime", 45*time.Second, "duration of time a producer will remain tombstoned if registration remains")
)

func main() {
	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(version.String("nsqlookupd"))
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := nsqlookupd.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	daemon := nsqlookupd.New(opts)

	daemon.Main()
	<-signalChan
	daemon.Exit()
}
