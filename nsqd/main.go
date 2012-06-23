package main

import (
	"../util"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
)

const VERSION = "0.1"

var (
	showVersion     = flag.Bool("version", false, "print version string")
	webAddress      = flag.String("web-address", "0.0.0.0:5151", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress      = flag.String("tcp-address", "0.0.0.0:5150", "<addr>:<port> to listen on for TCP clients")
	debugMode       = flag.Bool("debug", false, "enable debug mode")
	memQueueSize    = flag.Int("mem-queue-size", 10000, "number of messages to keep in memory (per topic)")
	cpuProfile      = flag.String("cpu-profile", "", "write cpu profile to file")
	goMaxProcs      = flag.Int("go-max-procs", 0, "runtime configuration for GOMAXPROCS")
	dataPath        = flag.String("data-path", "", "path to store disk-backed messages")
	lookupAddresses = util.StringArray{}
)

func init() {
	flag.Var(&lookupAddresses, "lookupd-address", "lookupd address (may be given multiple times)")
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsqd v%s\n", VERSION)
		return
	}

	if *goMaxProcs > 0 {
		runtime.GOMAXPROCS(*goMaxProcs)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}

	webAddr, err := net.ResolveTCPAddr("tcp", *webAddress)
	if err != nil {
		log.Fatal(err)
	}

	if *cpuProfile != "" {
		log.Printf("CPU Profiling Enabled")
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.Printf("nsqd v%s", VERSION)

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	nsqd = NewNSQd(tcpAddr, webAddr, lookupAddresses, *memQueueSize, *dataPath)
	nsqd.Main()
	<-exitChan
	nsqd.Exit()
}
