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
	"github.com/bitly/nsq/internal/version"
	"github.com/bitly/nsq/nsqlookupd"
	"github.com/mreiferson/go-options"
)

var (
	flagSet = flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")
	verbose     = flagSet.Bool("verbose", false, "enable verbose logging")

	tcpAddress       = flagSet.String("tcp-address", "0.0.0.0:4160", "<addr>:<port> to listen on for TCP clients")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:4161", "<addr>:<port> to listen on for HTTP clients")
	broadcastAddress = flagSet.String("broadcast-address", "", "address of this lookupd node, (default to the OS hostname)")

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

	opts := nsqlookupd.NewNSQLookupdOptions()
	options.Resolve(opts, flagSet, cfg)
	daemon := nsqlookupd.NewNSQLookupd(opts)

	daemon.Main()
	<-signalChan
	daemon.Exit()
}
