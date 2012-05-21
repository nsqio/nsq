package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

var bindAddress = flag.String("address", "", "address to bind to")
var webPort = flag.Int("web-port", 5152, "port to listen on for HTTP connections")
var debugMode = flag.Bool("debug", false, "enable debug mode")

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func main() {
	flag.Parse()
	fqAddress := *bindAddress + ":" + strconv.Itoa(*webPort)
	log.Printf("listening for http requests on %s", fqAddress)
	s := &http.Server{
		Addr:           fqAddress,
		Handler:        http.DefaultServeMux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	http.HandleFunc("/ping", pingHandler)
	http.Handle("/static", http.FileServer(http.Dir("static")))
	log.Fatal(s.ListenAndServe())

}
