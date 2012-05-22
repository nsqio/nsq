package main

import (
	"../util"
	"io"
	"log"
	"net/http"
	"strconv"
)

import _ "net/http/pprof"

func HttpServer(address string, port string, endChan chan int) {
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/stats", statsHandler)
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

func putHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	conn := &HTTPConn{}
	client := NewClient(conn, "HTTP")
	prot := Protocols[538990129] // v1
	response, err := prot.Execute(client, "PUB", topicName, string(reqParams.Body))
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.Write(response)
}
