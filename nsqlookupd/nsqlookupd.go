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
var webPort = flag.Int("web-port", 5160, "port to listen on for HTTP connections")
var tcpPort = flag.Int("tcp-port", 5161, "port to listen on for TCP connections")
var debugMode = flag.Bool("debug", false, "enable debug mode")
var cpuProfile = flag.String("cpu-profile", "", "write cpu profile to file")
var goMaxProcs = flag.Int("go-max-procs", 0, "runtime configuration for GOMAXPROCS")

var ldb *LookupDB

func main() {
	flag.Parse()

	if *goMaxProcs > 0 {
		runtime.GOMAXPROCS(*goMaxProcs)
	}

	endChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	ldb = NewLookupDB()

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
		endChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	go TcpServer(*bindAddress, strconv.Itoa(*tcpPort))
	HttpServer(*bindAddress, strconv.Itoa(*webPort), endChan)
}
