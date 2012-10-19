package main

import (
	"../nsq"
	"../util"
	"bytes"
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
	handler.HandleFunc("/info", infoHandler)
	handler.HandleFunc("/put", putHandler)
	handler.HandleFunc("/mput", mputHandler)
	handler.HandleFunc("/stats", statsHandler)
	handler.HandleFunc("/delete_topic", deleteTopicHandler)
	handler.HandleFunc("/empty_channel", emptyChannelHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)
	handler.HandleFunc("/mem_profile", memProfileHandler)
	handler.HandleFunc("/cpu_profile", httpprof.Profile)
	handler.HandleFunc("/dump_inflight", dumpInFlightHandler)
	handler.HandleFunc("/pause_channel", pauseChannelHandler)
	handler.HandleFunc("/unpause_channel", pauseChannelHandler)

	// these timeouts are absolute per server connection NOT per request
	// this means that a single persistent connection will only last N seconds
	server := &http.Server{
		Handler: handler,
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
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
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

func infoHandler(w http.ResponseWriter, req *http.Request) {
	util.ApiResponse(w, 200, "OK", struct {
		Version string `json:"version"`
	}{
		Version: util.BINARY_VERSION,
	})
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if !nsq.IsValidTopicName(topicName) {
		util.ApiResponse(w, 500, "INVALID_ARG_TOPIC", nil)
		return
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, reqParams.Body)
	err = topic.PutMessage(msg)
	if err != nil {
		util.ApiResponse(w, 500, "NOK", nil)
		return
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func mputHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if !nsq.IsValidTopicName(topicName) {
		util.ApiResponse(w, 500, "INVALID_ARG_TOPIC", nil)
		return
	}

	topic := nsqd.GetTopic(topicName)
	for _, block := range bytes.Split(reqParams.Body, []byte("\n")) {
		if len(block) != 0 {
			msg := nsq.NewMessage(<-nsqd.idChan, block)
			err := topic.PutMessage(msg)
			if err != nil {
				util.ApiResponse(w, 500, "NOK", nil)
				return
			}
		}
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func deleteTopicHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	err = nsqd.DeleteExistingTopic(topicName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func emptyChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}

	topic, err := nsqd.GetExistingTopic(topicName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_CHANNEL", nil)
		return
	}

	err = EmptyQueue(channel)
	if err != nil {
		util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
		return
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func deleteChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}

	topic, err := nsqd.GetExistingTopic(topicName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	err = topic.DeleteExistingChannel(channelName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_CHANNEL", nil)
		return
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func pauseChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}

	topic, err := nsqd.GetExistingTopic(topicName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_CHANNEL", nil)
		return
	}

	if strings.HasPrefix(req.URL.Path, "/pause") {
		channel.Pause()
	} else {
		channel.UnPause()
	}

	util.ApiResponse(w, 200, "OK", nil)
}
