package main

import (
	"io"
	"log"
	"net"
	"net/http"
)

func HttpServer(tcpAddr *net.TCPAddr, endChan chan int) {
	http.HandleFunc("/ping", pingHandler)
	go func() {
		log.Printf("HTTP: listening on %s", tcpAddr.String())
		err := http.ListenAndServe(tcpAddr.String(), nil)
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
