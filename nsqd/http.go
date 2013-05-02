package main

import (
	"bytes"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
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
	handler.HandleFunc("/empty_topic", emptyTopicHandler)
	handler.HandleFunc("/delete_topic", deleteTopicHandler)
	handler.HandleFunc("/empty_channel", emptyChannelHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)
	handler.HandleFunc("/mem_profile", memProfileHandler)
	handler.HandleFunc("/cpu_profile", httpprof.Profile)
	handler.HandleFunc("/pause_channel", pauseChannelHandler)
	handler.HandleFunc("/unpause_channel", pauseChannelHandler)
	handler.HandleFunc("/create_topic", createTopicHandler)
	handler.HandleFunc("/create_channel", createChannelHandler)

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
	if req.Method != "POST" {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if !nsq.IsValidTopicName(topicName) {
		util.ApiResponse(w, 500, "INVALID_ARG_TOPIC", nil)
		return
	}

	if int64(len(reqParams.Body)) > nsqd.options.maxMessageSize {
		util.ApiResponse(w, 500, "MSG_TOO_BIG", nil)
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
	if req.Method != "POST" {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
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
			if int64(len(reqParams.Body)) > nsqd.options.maxMessageSize {
				util.ApiResponse(w, 500, "MSG_TOO_BIG", nil)
				return
			}

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

func createTopicHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if !nsq.IsValidTopicName(topicName) {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	nsqd.GetTopic(topicName)
	util.ApiResponse(w, 200, "OK", nil)
}

func emptyTopicHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	if !nsq.IsValidTopicName(topicName) {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	topic, err := nsqd.GetExistingTopic(topicName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	err = topic.Empty()
	if err != nil {
		util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
		return
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func deleteTopicHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
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

func createChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	topic.GetChannel(channelName)
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

	err = channel.Empty()
	if err != nil {
		util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
		return
	}

	util.ApiResponse(w, 200, "OK", nil)
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

func statsHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	formatString, _ := reqParams.Get("format")
	jsonFormat := formatString == "json"
	now := time.Now()

	if !jsonFormat {
		io.WriteString(w, fmt.Sprintf("%s\n", util.Version("nsqd")))
	}

	stats := nsqd.getStats()

	if jsonFormat {
		util.ApiResponse(w, 200, "OK", struct {
			Topics []TopicStats `json:"topics"`
		}{stats})
	} else {
		if len(stats) == 0 {
			io.WriteString(w, "\nNO_TOPICS\n")
			return
		}
		for _, t := range stats {
			io.WriteString(w, fmt.Sprintf("\n[%-15s] depth: %-5d be-depth: %-5d msgs: %-8d\n",
				t.TopicName,
				t.Depth,
				t.BackendDepth,
				t.MessageCount))
			for _, c := range t.Channels {
				var pausedPrefix string
				if c.Paused {
					pausedPrefix = " *P "
				} else {
					pausedPrefix = "    "
				}
				io.WriteString(w,
					fmt.Sprintf("%s[%-25s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d re-q: %-5d timeout: %-5d msgs: %-8d\n",
						pausedPrefix,
						c.ChannelName,
						c.Depth,
						c.BackendDepth,
						c.InFlightCount,
						c.DeferredCount,
						c.RequeueCount,
						c.TimeoutCount,
						c.MessageCount))
				for _, client := range c.Clients {
					connectTime := time.Unix(client.ConnectTime, 0)
					// truncate to the second
					duration := time.Duration(int64(now.Sub(connectTime).Seconds())) * time.Second
					_, port, _ := net.SplitHostPort(client.RemoteAddress)
					io.WriteString(w, fmt.Sprintf("        [%s %-21s] state: %d inflt: %-4d rdy: %-4d fin: %-8d re-q: %-8d msgs: %-8d connected: %s\n",
						client.Version,
						fmt.Sprintf("%s:%s", client.Name, port),
						client.State,
						client.InFlightCount,
						client.ReadyCount,
						client.FinishCount,
						client.RequeueCount,
						client.MessageCount,
						duration,
					))
				}
			}
		}
	}
}
