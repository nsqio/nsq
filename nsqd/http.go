package main

import (
	"../nsq"
	"../util"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"strings"
	"time"
)

import httpprof "net/http/pprof"

func httpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/put", putHandler)
	handler.HandleFunc("/mput", mputHandler)
	handler.HandleFunc("/stats", statsHandler)
	handler.HandleFunc("/empty", emptyHandler)
	handler.HandleFunc("/mem_profile", memProfileHandler)
	handler.HandleFunc("/cpu_profile", httpprof.Profile)
	handler.HandleFunc("/dump_inflight", dumpInFlightHandler)

	// these timeouts are absolute per server connection NOT per request
	// this means that a single persistent connection will only last N seconds
	server := &http.Server{
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}

	log.Printf("HTTP: closing %s", listener.Addr().String())
}

func dumpInFlightHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write(util.ApiResponse(500, "INVALID_REQUEST", nil))
		return
	}

	topicName, channelName, err := getTopicChannelArgs(reqParams)
	if err != nil {
		w.Write(util.ApiResponse(500, err.Error(), nil))
		return
	}

	log.Printf("NOTICE: dumping inflight for %s:%s", topicName, channelName)

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)

	fmt.Fprintf(w, "inFlightMessages:\n")
	channel.Lock()
	for _, item := range channel.inFlightMessages {
		msg := item.Value.(*inFlightMessage).msg
		fmt.Fprintf(w, "%s %s %d\n", msg.Id, time.Unix(msg.Timestamp, 0).String(), msg.Attempts)
	}
	channel.Unlock()

	fmt.Fprintf(w, "inFlightPQ:\n")
	channel.inFlightMutex.Lock()
	for i := 0; i < len(channel.inFlightPQ); i++ {
		item := channel.inFlightPQ[i]
		msg := item.Value.(*inFlightMessage).msg
		fmt.Fprintf(w, "id: %s created: %s attempts: %d index: %d priority: %d\n", msg.Id, time.Unix(msg.Timestamp, 0).String(), msg.Attempts, item.Index, item.Priority)
	}
	channel.inFlightMutex.Unlock()
}

func memProfileHandler(w http.ResponseWriter, req *http.Request) {
	log.Printf("MEMORY Profiling Enabled")
	f, err := os.Create("nsqd.mprof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.WriteHeapProfile(f)
	f.Close()

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write(util.ApiResponse(500, "INVALID_REQUEST", nil))
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		w.Write(util.ApiResponse(500, "MISSING_ARG_TOPIC", nil))
		return
	}

	if !nsq.IsValidTopicName(topicName) {
		w.Write(util.ApiResponse(500, "INVALID_ARG_TOPIC", nil))
		return
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, reqParams.Body)
	err = topic.PutMessage(msg)
	if err != nil {
		w.WriteHeader(500)
		w.Header().Set("Content-Length", "3")
		io.WriteString(w, "NOK")
		return
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func mputHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write(util.ApiResponse(500, "INVALID_REQUEST", nil))
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		w.Write(util.ApiResponse(500, "MISSING_ARG_TOPIC", nil))
		return
	}

	if !nsq.IsValidTopicName(topicName) {
		w.Write(util.ApiResponse(500, "INVALID_ARG_TOPIC", nil))
		return
	}

	topic := nsqd.GetTopic(topicName)
	for _, block := range bytes.Split(reqParams.Body, []byte("\n")) {
		if len(block) != 0 {
			msg := nsq.NewMessage(<-nsqd.idChan, block)
			err := topic.PutMessage(msg)
			if err != nil {
				w.WriteHeader(500)
				w.Header().Set("Content-Length", "3")
				io.WriteString(w, "NOK")
				return
			}
		}
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func emptyHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write(util.ApiResponse(500, "INVALID_REQUEST", nil))
		return
	}

	topicName, channelName, err := getTopicChannelArgs(reqParams)
	if err != nil {
		w.Write(util.ApiResponse(500, err.Error(), nil))
		return
	}

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	err = EmptyQueue(channel)
	if err != nil {
		w.Write(util.ApiResponse(500, "INTERNAL_ERROR", nil))
		return
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func getTopicChannelArgs(rp *util.ReqParams) (string, string, error) {
	topicName, err := rp.Query("topic")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	if !nsq.IsValidTopicName(topicName) {
		return "", "", errors.New("INVALID_ARG_TOPIC")
	}

	channelName, err := rp.Query("channel")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !nsq.IsValidChannelName(channelName) {
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}
