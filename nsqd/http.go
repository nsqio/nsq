package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
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
	case "/debug/pprof/cmdline":
		httpprof.Cmdline(w, req)
	case "/debug/pprof/symbol":
		httpprof.Symbol(w, req)
	case "/debug/pprof/heap":
		httpprof.Handler("heap").ServeHTTP(w, req)
	case "/debug/pprof/goroutine":
		httpprof.Handler("goroutine").ServeHTTP(w, req)
	case "/debug/pprof/profile":
		httpprof.Profile(w, req)
	case "/debug/pprof/block":
		httpprof.Handler("block").ServeHTTP(w, req)
	case "/debug/pprof/threadcreate":
		httpprof.Handler("threadcreate").ServeHTTP(w, req)
	default:
		log.Printf("ERROR: 404 %s", req.URL.Path)
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

func (s *httpServer) getTopicFromQuery(req *http.Request) (url.Values, *Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		return nil, nil, errors.New("INVALID_REQUEST")
	}

	topicNames, ok := reqParams["topic"]
	if !ok {
		return nil, nil, errors.New("MISSING_ARG_TOPIC")
	}
	topicName := topicNames[0]

	if !nsq.IsValidTopicName(topicName) {
		return nil, nil, errors.New("INVALID_ARG_TOPIC")
	}

	return reqParams, s.context.nsqd.GetTopic(topicName), nil
}

func (s *httpServer) putHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.context.nsqd.options.maxMessageSize {
		util.ApiResponse(w, 500, "MSG_TOO_BIG", nil)
		return
	}

	// add 1 so that it's greater than our max when we test for it
	// (LimitReader returns a "fake" EOF)
	readMax := s.context.nsqd.options.maxMessageSize + 1
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}
	if int64(len(body)) == readMax {
		log.Printf("ERROR: /put hit max message size")
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}
	if len(body) == 0 {
		util.ApiResponse(w, 500, "MSG_EMPTY", nil)
		return
	}

	_, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}

	msg := nsq.NewMessage(<-s.context.nsqd.idChan, body)
	err = topic.PutMessage(msg)
	if err != nil {
		util.ApiResponse(w, 500, "NOK", nil)
		return
	}

	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) mputHandler(w http.ResponseWriter, req *http.Request) {
	var msgs []*nsq.Message
	var exit bool

	if req.Method != "POST" {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.context.nsqd.options.maxBodySize {
		util.ApiResponse(w, 500, "BODY_TOO_BIG", nil)
		return
	}

	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}

	_, ok := reqParams["binary"]
	if ok {
		tmp := make([]byte, 4)
		msgs, err = readMPUB(req.Body, tmp, s.context.nsqd.idChan,
			s.context.nsqd.options.maxMessageSize)
		if err != nil {
			util.ApiResponse(w, 500, err.(*util.FatalClientErr).Code[2:], nil)
			return
		}
	} else {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.context.nsqd.options.maxBodySize + 1
		rdr := bufio.NewReader(io.LimitReader(req.Body, readMax))
		total := 0
		for !exit {
			block, err := rdr.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
					return
				}
				exit = true
			}
			total += len(block)
			if int64(total) == readMax {
				log.Printf("ERROR: /mput hit max body size")
				continue
			}

			if len(block) > 0 && block[len(block)-1] == '\n' {
				block = block[:len(block)-1]
			}

			// silently discard 0 length messages
			// this maintains the behavior pre 0.2.22
			if len(block) == 0 {
				continue
			}

			if int64(len(block)) > s.context.nsqd.options.maxMessageSize {
				util.ApiResponse(w, 500, "MSG_TOO_BIG", nil)
				return
			}

			msg := nsq.NewMessage(<-s.context.nsqd.idChan, block)
			msgs = append(msgs, msg)
		}
	}

	err = topic.PutMessages(msgs)
	if err != nil {
		util.ApiResponse(w, 500, "NOK", nil)
		return
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
		err = channel.Pause()
	} else {
		err = channel.UnPause()
	}
	if err != nil {
		log.Printf("ERROR: failure in %s - %s", req.URL.Path, err.Error())
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
