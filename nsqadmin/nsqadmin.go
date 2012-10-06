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
	templateDir      = flag.String("template-dir", "", "path to templates directory")
	lookupdHTTPAddrs = util.StringArray{}
	nsqdHTTPAddrs    = util.StringArray{}
)

func init() {
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&nsqdHTTPAddrs, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
}

func main() {
	var waitGroup util.WaitGroupWrapper

	flag.Parse()

	log.Printf("nsqadmin v%s", VERSION)
	if *showVersion {
		return
	}

	if *templateDir == "" {
		for _, defaultPath := range []string{"templates", "/usr/local/share/nsqadmin/templates"} {
			if info, err := os.Stat(defaultPath); err == nil && info.IsDir() {
				*templateDir = defaultPath
				break
			}
		}
	}

	if *templateDir == "" {
		log.Fatalf("--template-dir must be specified (or install the templates to /usr/local/share/nsqadmin/templates)")
	}

	if len(nsqdHTTPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-http-address or --lookupd-http-address required.")
	}

	if len(nsqdHTTPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
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
