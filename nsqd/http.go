package nsqd

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	httpprof "net/http/pprof"
	"net/url"
	"strings"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
)

type httpError struct {
	code int
	text string
}

func (e httpError) Error() string {
	return e.text
}

func (e httpError) Code() int {
	return e.code
}

type httpServer struct {
	context     *context
	tlsEnabled  bool
	tlsRequired bool
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !s.tlsEnabled && s.tlsRequired {
		util.ApiResponse(w, 403, "TLS_REQUIRED", nil)
		return
	}

	err := s.v1Router(w, req)
	if err == nil {
		return
	}

	err = s.deprecatedRouter(w, req)
	if err == nil {
		return
	}

	err = s.debugRouter(w, req)
	if err != nil {
		log.Printf("ERROR: %s", err)
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
	}
}

func (s *httpServer) debugRouter(w http.ResponseWriter, req *http.Request) error {
	switch req.URL.Path {
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
		return errors.New(fmt.Sprintf("404 %s", req.URL.Path))
	}
	return nil
}

func (s *httpServer) v1Router(w http.ResponseWriter, req *http.Request) error {
	switch req.URL.Path {
	case "/topic/create":
		s.v1CreateTopicHandler(w, req)
	case "/topic/delete":
		s.v1DeleteTopicHandler(w, req)
	case "/topic/empty":
		s.v1EmptyTopicHandler(w, req)
	case "/topic/pause":
		s.v1PauseTopicHandler(w, req)
	case "/topic/unpause":
		s.v1PauseTopicHandler(w, req)

	case "/channel/create":
		s.v1CreateChannelHandler(w, req)
	case "/channel/delete":
		s.v1DeleteChannelHandler(w, req)
	case "/channel/empty":
		s.v1EmptyChannelHandler(w, req)
	case "/channel/pause":
		s.v1PauseChannelHandler(w, req)
	case "/channel/unpause":
		s.v1PauseChannelHandler(w, req)

	default:
		return errors.New(fmt.Sprintf("404 %s", req.URL.Path))
	}
	return nil
}

func (s *httpServer) deprecatedRouter(w http.ResponseWriter, req *http.Request) error {
	switch req.URL.Path {
	case "/pub":
		fallthrough
	case "/put":
		s.pubHandler(w, req)
	case "/mpub":
		fallthrough
	case "/mput":
		s.mpubHandler(w, req)
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
	case "/pause_topic":
		s.pauseTopicHandler(w, req)
	case "/unpause_topic":
		s.pauseTopicHandler(w, req)
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
	default:
		return errors.New(fmt.Sprintf("404 %s", req.URL.Path))
	}
	return nil
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

func (s *httpServer) getExistingTopicFromQuery(req *http.Request) (*util.ReqParams, *Topic, string, error) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err)
		return nil, nil, "", httpError{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, nil, "", httpError{400, err.Error()}
	}

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, nil, "", httpError{404, "TOPIC_NOT_FOUND"}
	}

	return reqParams, topic, channelName, err
}

func (s *httpServer) getTopicFromQuery(req *http.Request) (url.Values, *Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err)
		return nil, nil, httpError{400, "INVALID_REQUEST"}
	}

	topicNames, ok := reqParams["topic"]
	if !ok {
		return nil, nil, httpError{400, "MISSING_ARG_TOPIC"}
	}
	topicName := topicNames[0]

	if !nsq.IsValidTopicName(topicName) {
		return nil, nil, httpError{400, "INVALID_TOPIC"}
	}

	return reqParams, s.context.nsqd.GetTopic(topicName), nil
}

func (s *httpServer) pubHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doPUB(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) doPUB(req *http.Request) error {
	if req.Method != "POST" {
		return httpError{400, "INVALID_REQUEST"}
	}

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.context.nsqd.options.MaxMsgSize {
		return httpError{413, "MSG_TOO_BIG"}
	}

	// add 1 so that it's greater than our max when we test for it
	// (LimitReader returns a "fake" EOF)
	readMax := s.context.nsqd.options.MaxMsgSize + 1
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		return httpError{500, "INTERNAL_ERROR"}
	}
	if int64(len(body)) == readMax {
		log.Printf("ERROR: /put hit max message size")
		return httpError{413, "MSG_TOO_BIG"}
	}
	if len(body) == 0 {
		return httpError{400, "MSG_EMPTY"}
	}

	_, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return err
	}

	msg := nsq.NewMessage(<-s.context.nsqd.idChan, body)
	err = topic.PutMessage(msg)
	if err != nil {
		return httpError{503, "EXITING"}
	}

	return nil
}

func (s *httpServer) mpubHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doMPUB(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) doMPUB(req *http.Request) error {
	var msgs []*nsq.Message
	var exit bool

	if req.Method != "POST" {
		return httpError{400, "INVALID_REQUEST"}
	}

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.context.nsqd.options.MaxBodySize {
		return httpError{413, "BODY_TOO_BIG"}
	}

	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return err
	}

	_, ok := reqParams["binary"]
	if ok {
		tmp := make([]byte, 4)
		msgs, err = readMPUB(req.Body, tmp, s.context.nsqd.idChan,
			s.context.nsqd.options.MaxMsgSize)
		if err != nil {
			return httpError{413, err.(*util.FatalClientErr).Code[2:]}
		}
	} else {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.context.nsqd.options.MaxBodySize + 1
		rdr := bufio.NewReader(io.LimitReader(req.Body, readMax))
		total := 0
		for !exit {
			block, err := rdr.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					return httpError{500, "INTERNAL_ERROR"}
				}
				exit = true
			}
			total += len(block)
			if int64(total) == readMax {
				return httpError{413, "BODY_TOO_BIG"}
			}

			if len(block) > 0 && block[len(block)-1] == '\n' {
				block = block[:len(block)-1]
			}

			// silently discard 0 length messages
			// this maintains the behavior pre 0.2.22
			if len(block) == 0 {
				continue
			}

			if int64(len(block)) > s.context.nsqd.options.MaxMsgSize {
				return httpError{413, "MSG_TOO_BIG"}
			}

			msg := nsq.NewMessage(<-s.context.nsqd.idChan, block)
			msgs = append(msgs, msg)
		}
	}

	err = topic.PutMessages(msgs)
	if err != nil {
		return httpError{503, "EXITING"}
	}

	return nil
}

func (s *httpServer) createTopicHandler(w http.ResponseWriter, req *http.Request) {
	_, _, err := s.getTopicFromQuery(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1CreateTopicHandler(w http.ResponseWriter, req *http.Request) {
	_, _, err := s.getTopicFromQuery(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) emptyTopicHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doEmptyTopic(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1EmptyTopicHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doEmptyTopic(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) doEmptyTopic(req *http.Request) error {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err)
		return httpError{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return httpError{400, "MISSING_ARG_TOPIC"}
	}

	if !nsq.IsValidTopicName(topicName) {
		return httpError{400, "INVALID_TOPIC"}
	}

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return httpError{404, "TOPIC_NOT_FOUND"}
	}

	err = topic.Empty()
	if err != nil {
		return httpError{500, "INTERNAL_ERROR"}
	}

	return nil
}

func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doDeleteTopic(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1DeleteTopicHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doDeleteTopic(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) doDeleteTopic(req *http.Request) error {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err)
		return httpError{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return httpError{400, "MISSING_ARG_TOPIC"}
	}

	err = s.context.nsqd.DeleteExistingTopic(topicName)
	if err != nil {
		return httpError{404, "TOPIC_NOT_FOUND"}
	}

	return nil
}

func (s *httpServer) pauseTopicHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doPauseTopic(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1PauseTopicHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doPauseTopic(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) doPauseTopic(req *http.Request) error {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err)
		return httpError{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return httpError{400, "MISSING_ARG_TOPIC"}
	}

	topic, err := s.context.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return httpError{404, "TOPIC_NOT_FOUND"}
	}

	if strings.Contains(req.URL.Path, "unpause") {
		err = topic.UnPause()
	} else {
		err = topic.Pause()
	}
	if err != nil {
		log.Printf("ERROR: failure in %s - %s", req.URL.Path, err)
		return httpError{500, "INTERNAL_ERROR"}
	}

	return nil
}

func (s *httpServer) createChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doCreateChannel(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1CreateChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doCreateChannel(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) doCreateChannel(req *http.Request) error {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return err
	}
	topic.GetChannel(channelName)
	return nil
}

func (s *httpServer) emptyChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doEmptyChannel(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1EmptyChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doEmptyChannel(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) doEmptyChannel(req *http.Request) error {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return httpError{404, "CHANNEL_NOT_FOUND"}
	}

	err = channel.Empty()
	if err != nil {
		return httpError{500, "INTERNAL_ERROR"}
	}

	return nil
}

func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doDeleteChannel(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1DeleteChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doDeleteChannel(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) doDeleteChannel(req *http.Request) error {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return err
	}

	err = topic.DeleteExistingChannel(channelName)
	if err != nil {
		return httpError{404, "CHANNEL_NOT_FOUND"}
	}

	return nil
}

func (s *httpServer) pauseChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doPauseChannel(req)
	if err != nil {
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, err.(httpError).Code(), err)
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, err.Error(), nil)
		}
		return
	}
	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, nil)
	} else {
		util.ApiResponse(w, 200, "OK", nil)
	}
}

func (s *httpServer) v1PauseChannelHandler(w http.ResponseWriter, req *http.Request) {
	err := s.doPauseChannel(req)
	if err != nil {
		util.V1ApiResponse(w, err.(httpError).Code(), err)
		return
	}
	util.V1ApiResponse(w, 200, "OK")
}

func (s *httpServer) doPauseChannel(req *http.Request) error {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return httpError{404, "CHANNEL_NOT_FOUND"}
	}

	if strings.Contains(req.URL.Path, "unpause") {
		err = channel.UnPause()
	} else {
		err = channel.Pause()
	}
	if err != nil {
		log.Printf("ERROR: failure in %s - %s", req.URL.Path, err)
		return httpError{500, "INTERNAL_ERROR"}
	}

	return nil
}

func (s *httpServer) statsHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err)
		if acceptVersion(req) == 1 {
			util.V1ApiResponse(w, 400, "INVALID_REQUEST")
		} else {
			// this handler always returns 500 for backwards compatibility
			util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		}
		return
	}
	formatString, _ := reqParams.Get("format")
	jsonFormat := formatString == "json"
	stats := s.context.nsqd.GetStats()

	if !jsonFormat {
		s.doTextStats(stats, w)
		return
	}

	if acceptVersion(req) == 1 {
		util.V1ApiResponse(w, 200, struct {
			Version string       `json:"version"`
			Topics  []TopicStats `json:"topics"`
		}{util.BINARY_VERSION, stats})
	} else {
		util.ApiResponse(w, 200, "OK", struct {
			Version string       `json:"version"`
			Topics  []TopicStats `json:"topics"`
		}{util.BINARY_VERSION, stats})
	}
}

func (s *httpServer) doTextStats(stats []TopicStats, w http.ResponseWriter) {
	now := time.Now()
	io.WriteString(w, fmt.Sprintf("%s\n", util.Version("nsqd")))
	if len(stats) == 0 {
		io.WriteString(w, "\nNO_TOPICS\n")
		return
	}
	for _, t := range stats {
		var pausedPrefix string
		if t.Paused {
			pausedPrefix = "*P "
		} else {
			pausedPrefix = "   "
		}
		io.WriteString(w, fmt.Sprintf("\n%s[%-15s] depth: %-5d be-depth: %-5d msgs: %-8d e2e%%: %s\n",
			pausedPrefix,
			t.TopicName,
			t.Depth,
			t.BackendDepth,
			t.MessageCount,
			t.E2eProcessingLatency))
		for _, c := range t.Channels {
			if c.Paused {
				pausedPrefix = "   *P "
			} else {
				pausedPrefix = "      "
			}
			io.WriteString(w,
				fmt.Sprintf("%s[%-25s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d re-q: %-5d timeout: %-5d msgs: %-8d e2e%%: %s\n",
					pausedPrefix,
					c.ChannelName,
					c.Depth,
					c.BackendDepth,
					c.InFlightCount,
					c.DeferredCount,
					c.RequeueCount,
					c.TimeoutCount,
					c.MessageCount,
					c.E2eProcessingLatency))
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

func acceptVersion(req *http.Request) int {
	if req.Header.Get("accept") == "vnd/nsq; version=1.0" {
		return 1
	}

	return 0
}
