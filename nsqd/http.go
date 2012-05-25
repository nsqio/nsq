package main

import (
	"../nsq"
	"../util"
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
)

import _ "net/http/pprof"

func HttpServer(tcpAddr *net.TCPAddr, endChan chan int) {
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/stats", statsHandler)
	go func() {
		log.Printf("listening for http requests on %s", tcpAddr.String())
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

	messageBuf := bytes.NewBuffer(reqParams.Body)
	conn := &HTTPConn{messageBuf}
	client := nsq.NewServerClient(conn, "HTTP")
	prot := Protocols[538990129] // v1
	response, err := prot.Execute(client, "PUB", topicName, strconv.Itoa(messageBuf.Len()))
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		response = []byte(err.Error())
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.Write(response)
}
