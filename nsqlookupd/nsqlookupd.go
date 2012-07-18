package main

import (
	"../nsq"
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

var (
	showVersion = flag.Bool("version", false, "print version string")
	tcpAddress  = flag.String("tcp-address", "0.0.0.0:4160", "<addr>:<port> to listen on for TCP clients")
	webAddress  = flag.String("web-address", "0.0.0.0:4161", "<addr>:<port> to listen on for HTTP clients")
	debugMode   = flag.Bool("debug", false, "enable debug mode")
	cpuProfile  = flag.String("cpu-profile", "", "write cpu profile to file")
	goMaxProcs  = flag.Int("go-max-procs", 0, "runtime configuration for GOMAXPROCS")
)

var protocols = map[int32]nsq.Protocol{}
var sm *util.SafeMap

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsqlookupd v%s\n", VERSION)
		return
	}

	if *goMaxProcs > 0 {
		runtime.GOMAXPROCS(*goMaxProcs)
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	sm = util.NewSafeMap()

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
		exitChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}

	webAddr, err := net.ResolveTCPAddr("tcp", *webAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("nsqlookupd v%s", VERSION)

	tcpListener, err := net.Listen("tcp", tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", tcpAddr, err.Error())
	}
	go util.TcpServer(tcpListener, &TcpProtocol{protocols: protocols})

	webListener, err := net.Listen("tcp", webAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", webAddr, err.Error())
	}
	go HttpServer(webListener)

	<-exitChan

	tcpListener.Close()
	webListener.Close()
}
