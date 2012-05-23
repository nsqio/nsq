package main

import (
	"io"
	"log"
	"net/http"
)

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
