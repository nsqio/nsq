package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
)

var bindAddress = flag.String("address", "", "address to bind to")
var webPort = flag.Int("web-port", 5150, "port to listen on for HTTP connections")
var tcpPort = flag.Int("tcp-port", 5151, "port to listen on for TCP connections")
var debugMode = flag.Bool("debug", false, "enable debug mode")
var memQueueSize = flag.Int("mem-queue-size", 10000, "number of messages to keep in memory (per topic)")
var cpuProfile = flag.String("cpu-profile", "", "write cpu profile to file")
var goMaxProcs = flag.Int("go-max-procs", 4, "runtime configuration for GOMAXPROCS")
var dataPath = flag.String("data-path", "", "path to store disk-backed messages")

func main() {
	flag.Parse()

	if *goMaxProcs > 0 {
		runtime.GOMAXPROCS(*goMaxProcs)
	}

	nsqEndChan := make(chan int)
	signalChan := make(chan os.Signal, 1)

	if *cpuProfile != "" {
		log.Printf("CPU Profiling Enabled")
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	go func() {
		<-signalChan
		nsqEndChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	go TopicFactory(*memQueueSize, *dataPath)
	go UuidFactory()
	go TcpServer(*bindAddress, strconv.Itoa(*tcpPort))
	HttpServer(*bindAddress, strconv.Itoa(*webPort), nsqEndChan)

	for _, topic := range TopicMap {
		topic.Close()
	}
}
