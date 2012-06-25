package main

import (
	"../util"
	"encoding/json"
	"fmt"
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
		w.Write([]byte(`{"status_code":500, "status_txt":"INVALID_REQUEST", "data":null}`))
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		w.Write([]byte(`{"status_code":500, "status_txt":"MISSING_ARG_TOPIC", "data":null}`))
		return
	}

	dataInterface, ok := sm.Get("topic." + topicName)
	if !ok {
		errorTxt := fmt.Sprintf(`{"status_code":500, "status_txt":"%s", "data":null}`)
		w.Write([]byte(errorTxt))
		return
	}

	data := dataInterface.(map[string]interface{})
	output := make(map[string]interface{})
	output["data"] = data
	output["status_code"] = 200
	output["status_txt"] = "OK"
	response, err := json.Marshal(&output)
	if err != nil {
		errorTxt := fmt.Sprintf(`{"status_code":500, "status_txt":"%s", "data":null}`, err.Error())
		w.Write([]byte(errorTxt))
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.Write(response)
}
