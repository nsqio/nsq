package main

import (
	"../util"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
)

func HttpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/lookup", lookupHandler)
	err := http.Serve(listener, nil)
	if err != nil {
		log.Fatal("http.ListenAndServe:", err)
	}
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func lookupHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		// TODO: return default response
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		// TODO: return default response
		return
	}

	dataInterface, err := sm.Get("topic." + topicName)
	if err != nil {
		// TODO: return default response
		return
	}

	data := dataInterface.(map[string]interface{})
	response, err := json.Marshal(&data)
	if err != nil {
		// TODO: return default response
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.Write(response)
}
