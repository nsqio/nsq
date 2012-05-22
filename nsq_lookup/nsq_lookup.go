package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
)

var bindAddress = flag.String("address", "", "address to bind to")
var webPort = flag.Int("web-port", 5160, "port to listen on for HTTP connections")
var tcpPort = flag.Int("tcp-port", 5161, "port to listen on for TCP connections")
var debugMode = flag.Bool("debug", false, "enable debug mode")
var cpuProfile = flag.String("cpu-profile", "", "write cpu profile to file")
var goMaxProcs = flag.Int("go-max-procs", 4, "runtime configuration for GOMAXPROCS")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

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

	go TcpServer(*bindAddress, strconv.Itoa(*tcpPort))
	HttpServer(*bindAddress, strconv.Itoa(*webPort), nsqEndChan)
}

func TcpServer(address string, port string) {
	fqAddress := address + ":" + port
	listener, err := net.Listen("tcp", fqAddress)
	if err != nil {
		panic("listen (" + fqAddress + ") failed: " + err.Error())
	}
	log.Printf("listening for clients on %s", fqAddress)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			panic("accept failed: " + err.Error())
		}
		go func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					break
				}

				line = strings.Replace(line, "\n", "", -1)
				line = strings.Replace(line, "\r", "", -1)
				params := strings.Split(line, " ")

				log.Printf("%#v", params)
			}
		}(clientConn)
	}
}

func HttpServer(address string, port string, endChan chan int) {
	http.HandleFunc("/ping", pingHandler)
	go func() {
		fqAddress := address + ":" + port
		log.Printf("listening for http requests on %s", fqAddress)
		err := http.ListenAndServe(fqAddress, nil)
		if err != nil {
			log.Fatal("http.ListenAndServe:", err)
		}
	}()
	<-endChan
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}
