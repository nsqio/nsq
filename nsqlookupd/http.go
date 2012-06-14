package main

import (
	"io"
	"log"
	"net"
	"net/http"
)

func HttpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())
	http.HandleFunc("/ping", pingHandler)
	err := http.Serve(listener, nil)
	if err != nil {
		log.Fatal("http.ListenAndServe:", err)
	}
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}
