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
	"syscall"
	"time"
)

var (
	showVersion     = flag.Bool("version", false, "print version string")
	webAddress      = flag.String("web-address", "0.0.0.0:4151", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress      = flag.String("tcp-address", "0.0.0.0:4150", "<addr>:<port> to listen on for TCP clients")
	debugMode       = flag.Bool("debug", false, "enable debug mode")
	memQueueSize    = flag.Int64("mem-queue-size", 10000, "number of messages to keep in memory (per topic)")
	maxBytesPerFile = flag.Int64("max-bytes-per-file", 104857600, "number of bytes per diskqueue file before rolling")
	syncEvery       = flag.Int64("sync-every", 2500, "number of messages between diskqueue syncs")
	msgTimeoutMs    = flag.Int64("msg-timeout", 60000, "time (ms) to wait before auto-requeing a message")
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

	// go func() {
	// 	var m runtime.MemStats
	// 	ticker := time.NewTicker(5 * time.Second)
	// 	for {
	// 		<-ticker.C
	// 		runtime.ReadMemStats(&m)
	// 		log.Printf("GoRoutines: %d - HeapInuse: %d - HeapReleased: %d - HeapObjects: %d", runtime.NumGoroutine(), m.HeapInuse, m.HeapReleased, m.HeapObjects)
	// 		log.Printf("GC Runs: %#v", m.PauseNs)
	// 	}
	// }()

	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *webAddress)
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
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	msgTimeoutDuration := int64(time.Duration(*msgTimeoutMs) * time.Millisecond)
	nsqd = NewNSQd(*workerId)
	nsqd.tcpAddr = tcpAddr
	nsqd.httpAddr = httpAddr
	nsqd.lookupAddrs = lookupAddresses
	nsqd.memQueueSize = *memQueueSize
	nsqd.dataPath = *dataPath
	nsqd.maxBytesPerFile = *maxBytesPerFile
	nsqd.syncEvery = *syncEvery
	nsqd.msgTimeout = msgTimeoutDuration
	nsqd.Main()
	<-exitChan
	nsqd.Exit()
}
