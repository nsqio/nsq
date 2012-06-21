package main

import (
	"../nsq"
	"../util"
	"io"
	"log"
	"net"
	"net/http"
)

import _ "net/http/pprof"

func HttpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/stats", statsHandler)
	err := http.Serve(listener, nil)
	if err != nil {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write([]byte(`{"status_code":500, "status_txt":"INVALID_REQUEST","data":null}`))
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		w.Write([]byte(`{"status_code":500, "status_txt":"MISSING_ARG_TOPIC","data":null}`))
		return
	}

	msg := nsq.NewMessage(<-UuidChan, reqParams.Body)
	topic := GetTopic(topicName, *memQueueSize, *dataPath)
	msg := nsq.NewMessage(<-idChan, reqParams.Body)
	topic.PutMessage(msg)

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}
