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
	"strings"
	"time"
)

import httpprof "net/http/pprof"

type httpServer struct {
	context *Context
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/pub":
		fallthrough
	case "/put":
		s.putHandler(w, req)
	case "/mpub":
		fallthrough
	case "/mput":
		s.mputHandler(w, req)
	case "/stats":
		s.statsHandler(w, req)
	case "/ping":
		s.pingHandler(w, req)
	case "/info":
		s.infoHandler(w, req)
	case "/empty_topic":
		s.emptyTopicHandler(w, req)
	case "/delete_topic":
		s.deleteTopicHandler(w, req)
	case "/empty_channel":
		s.emptyChannelHandler(w, req)
	case "/delete_channel":
		s.deleteChannelHandler(w, req)
	case "/pause_channel":
		s.pauseChannelHandler(w, req)
	case "/unpause_channel":
		s.pauseChannelHandler(w, req)
	case "/create_topic":
		s.createTopicHandler(w, req)
	case "/create_channel":
		s.createChannelHandler(w, req)
	case "/debug/pprof":
		httpprof.Index(w, req)
	case "/debug/pprof/heap":
		httpprof.Handler("heap").ServeHTTP(w, req)
	case "/debug/pprof/goroutine":
		httpprof.Handler("goroutine").ServeHTTP(w, req)
	case "/debug/pprof/cpu":
		httpprof.Profile(w, req)
	case "/debug/pprof/block":
		httpprof.Handler("block").ServeHTTP(w, req)
	case "/debug/pprof/threadcreate":
		httpprof.Handler("threadcreate").ServeHTTP(w, req)
	default:
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
	}
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) infoHandler(w http.ResponseWriter, req *http.Request) {
	util.ApiResponse(w, 200, "OK", struct {
		Version string `json:"version"`
	}{
		Version: util.BINARY_VERSION,
	})
}

func (s *httpServer) putHandler(w http.ResponseWriter, req *http.Request) {
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

	if int64(len(reqParams.Body)) > s.context.nsqd.options.maxMessageSize {
		util.ApiResponse(w, 500, "MSG_TOO_BIG", nil)
		return
	}

	topic := s.context.nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-s.context.nsqd.idChan, reqParams.Body)
	err = topic.PutMessage(msg)
	if err != nil {
		util.ApiResponse(w, 500, "NOK", nil)
		return
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) mputHandler(w http.ResponseWriter, req *http.Request) {
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

	topic := s.context.nsqd.GetTopic(topicName)
	for _, block := range bytes.Split(reqParams.Body, []byte("\n")) {
		if len(block) != 0 {
			if int64(len(reqParams.Body)) > s.context.nsqd.options.maxMessageSize {
				util.ApiResponse(w, 500, "MSG_TOO_BIG", nil)
				return
			}

			msg := nsq.NewMessage(<-s.context.nsqd.idChan, block)
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

func (s *httpServer) createTopicHandler(w http.ResponseWriter, req *http.Request) {
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

	s.context.nsqd.GetTopic(topicName)
	util.ApiResponse(w, 200, "OK", nil)
}

func (s *httpServer) emptyTopicHandler(w http.ResponseWriter, req *http.Request) {
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

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
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

func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request) {
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

	err = s.context.nsqd.DeleteExistingTopic(topicName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func (s *httpServer) createChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_TOPIC", nil)
		return
	}

	topic.GetChannel(channelName)
	util.ApiResponse(w, 200, "OK", nil)
}

func (s *httpServer) emptyChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
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

func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
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

func (s *httpServer) pauseChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
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

func (s *httpServer) statsHandler(w http.ResponseWriter, req *http.Request) {
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

	stats := s.context.nsqd.getStats()

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
