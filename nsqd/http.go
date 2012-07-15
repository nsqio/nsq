package main

import (
	"../nsq"
	"../util"
	"../util/pqueue"
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

import _ "net/http/pprof"

func HttpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())
	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/put", putHandler)
	handler.HandleFunc("/mput", mputHandler)
	handler.HandleFunc("/stats", statsHandler)
	handler.HandleFunc("/empty", emptyHandler)
	handler.HandleFunc("/mem_profile", memProfileHandler)
	handler.HandleFunc("/dump_inflight", dumpInFlightHandler)
	server := &http.Server{Handler: handler}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}
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
		msg := item.(*pqueue.Item).Value.(*inFlightMessage).msg
		fmt.Fprintf(w, "%s %s %d\n", msg.Id, time.Unix(msg.Timestamp, 0).String(), msg.Attempts)
	}
	channel.Unlock()

	fmt.Fprintf(w, "inFlightPQ:\n")
	channel.deferredMutex.Lock()
	for i := 0; i < len(channel.inFlightPQ); i++ {
		item := channel.inFlightPQ[i]
		msg := item.Value.(*inFlightMessage).msg
		fmt.Fprintf(w, "id: %s created: %s attempts: %d priority: %d\n", msg.Id, time.Unix(msg.Timestamp, 0).String(), msg.Attempts, item.Priority)
	}
	channel.deferredMutex.Unlock()
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

	if len(topicName) > nsq.MaxNameLength {
		w.Write(util.ApiResponse(500, "INVALID_ARG_TOPIC", nil))
		return
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, reqParams.Body)
	topic.PutMessage(msg)

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

	if len(topicName) > nsq.MaxNameLength {
		w.Write(util.ApiResponse(500, "INVALID_ARG_TOPIC", nil))
		return
	}

	topic := nsqd.GetTopic(topicName)
	for _, block := range bytes.Split(reqParams.Body, []byte("\n")) {
		if len(block) != 0 {
			msg := nsq.NewMessage(<-nsqd.idChan, block)
			topic.PutMessage(msg)
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

	if len(topicName) > nsq.MaxNameLength {
		return "", "", errors.New("INVALID_ARG_TOPIC")
	}

	channelName, err := rp.Query("channel")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	if len(topicName) > nsq.MaxNameLength {
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}
