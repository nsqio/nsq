package main

import (
	"../util"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
)

var (
	showVersion      = flag.Bool("version", false, "print version string")
	httpAddress      = flag.String("http-address", "0.0.0.0:4171", "<addr>:<port> to listen on for HTTP clients")
	templateDir      = flag.String("template-dir", "templates", "path to templates directory")
	lookupdAddresses = util.StringArray{}
	nsqdAddresses    = util.StringArray{}
)

func init() {
	flag.Var(&lookupdAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&nsqdAddresses, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
}

func main() {
	var waitGroup util.WaitGroupWrapper

	flag.Parse()

	log.Printf("nsqadmin v%s", VERSION)
	if *showVersion {
		return
	}

	if len(nsqdAddresses) == 0 && len(lookupdAddresses) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required.")
	}

	if len(nsqdAddresses) != 0 && len(lookupdAddresses) != 0 {
		log.Fatalf("use --nsqd-http-address or --lookupd-http-address not both")
	}

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)

	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, os.Interrupt)

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpListener, err := net.Listen("tcp", httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", httpAddr, err.Error())
	}
	waitGroup.Wrap(func() { httpServer(httpListener) })

	<-exitChan

	httpListener.Close()

	waitGroup.Wait()
}
