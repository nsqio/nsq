package main

import (
	"../util"
	"crypto/md5"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
)

const VERSION = "0.1.1"

var (
	showVersion     = flag.Bool("version", false, "print version string")
	webAddress      = flag.String("web-address", "0.0.0.0:4151", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress      = flag.String("tcp-address", "0.0.0.0:4150", "<addr>:<port> to listen on for TCP clients")
	debugMode       = flag.Bool("debug", false, "enable debug mode")
	memQueueSize    = flag.Int64("mem-queue-size", 10000, "number of messages to keep in memory (per topic)")
	maxBytesPerFile = flag.Int64("max-bytes-per-file", 1024768, "number of bytes per diskqueue file before rolling")
	cpuProfile      = flag.String("cpu-profile", "", "write cpu profile to file")
	goMaxProcs      = flag.Int("go-max-procs", 0, "runtime configuration for GOMAXPROCS")
	dataPath        = flag.String("data-path", "", "path to store disk-backed messages")
	workerId        = flag.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	verbose         = flag.Bool("verbose", false, "enable verbose logging")
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

	if *workerId == 0 {
		hostname, err := os.Hostname()
		if err != nil {
			log.Fatal(err)
		}
		h := md5.New()
		io.WriteString(h, hostname)
		*workerId = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)
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
	log.Printf("worker id %d", *workerId)

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	nsqd = NewNSQd(*workerId, tcpAddr, webAddr, lookupAddresses, *memQueueSize, *dataPath, *maxBytesPerFile)
	nsqd.Main()
	<-exitChan
	nsqd.Exit()
}
