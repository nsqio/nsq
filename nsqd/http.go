package nsqd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	httpprof "net/http/pprof"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/nsq/internal/http_api"
	"github.com/bitly/nsq/internal/protocol"
	"github.com/bitly/nsq/internal/version"
)

type httpServer struct {
	ctx         *context
	tlsEnabled  bool
	tlsRequired bool
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !s.tlsEnabled && s.tlsRequired {
		http_api.Respond(w, 403, "TLS_REQUIRED", nil)
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
		s.ctx.nsqd.logf("ERROR: %s", err)
		http_api.Respond(w, 404, "NOT_FOUND", nil)
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
		return fmt.Errorf("404 %s", req.URL.Path)
	}
	return nil
}

func (s *httpServer) v1Router(w http.ResponseWriter, req *http.Request) error {
	switch req.URL.Path {
	case "/pub":
		http_api.NegotiateVersionWrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doPUB(req) }))
	case "/mpub":
		http_api.NegotiateVersionWrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doMPUB(req) }))

	case "/stats":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doStats(req) })
	case "/ping":
		s.pingHandler(w, req)

	case "/topic/create":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doCreateTopic(req) }))
	case "/topic/delete":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doDeleteTopic(req) }))
	case "/topic/empty":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doEmptyTopic(req) }))
	case "/topic/pause":
		fallthrough
	case "/topic/unpause":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doPauseTopic(req) }))

	case "/channel/create":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doCreateChannel(req) }))
	case "/channel/delete":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doDeleteChannel(req) }))
	case "/channel/empty":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doEmptyChannel(req) }))
	case "/channel/pause":
		fallthrough
	case "/channel/unpause":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doPauseChannel(req) }))

	default:
		return fmt.Errorf("404 %s", req.URL.Path)
	}
	return nil
}

func (s *httpServer) deprecatedRouter(w http.ResponseWriter, req *http.Request) error {
	switch req.URL.Path {
	case "/put":
		http_api.NegotiateVersionWrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doPUB(req) }))
	case "/mput":
		http_api.NegotiateVersionWrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doMPUB(req) }))
	case "/info":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doInfo(req) })
	case "/empty_topic":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doEmptyTopic(req) })
	case "/delete_topic":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doDeleteTopic(req) })
	case "/pause_topic":
		fallthrough
	case "/unpause_topic":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doPauseTopic(req) })
	case "/empty_channel":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doEmptyChannel(req) })
	case "/delete_channel":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doDeleteChannel(req) })
	case "/pause_channel":
		fallthrough
	case "/unpause_channel":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doPauseChannel(req) })
	case "/create_topic":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doCreateTopic(req) })
	case "/create_channel":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doCreateChannel(req) })
	default:
		return fmt.Errorf("404 %s", req.URL.Path)
	}
	return nil
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	health := s.ctx.nsqd.GetHealth()
	code := 200
	if !s.ctx.nsqd.IsHealthy() {
		code = 500
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(health)))
	w.WriteHeader(code)
	io.WriteString(w, health)
}

func (s *httpServer) doInfo(req *http.Request) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

func (s *httpServer) getExistingTopicFromQuery(req *http.Request) (*http_api.ReqParams, *Topic, string, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failed to parse request params - %s", err)
		return nil, nil, "", http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, nil, "", http_api.Err{400, err.Error()}
	}

	topic, err := s.ctx.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, nil, "", http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	return reqParams, topic, channelName, err
}

func (s *httpServer) getTopicFromQuery(req *http.Request) (url.Values, *Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failed to parse request params - %s", err)
		return nil, nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicNames, ok := reqParams["topic"]
	if !ok {
		return nil, nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	topicName := topicNames[0]

	if !protocol.IsValidTopicName(topicName) {
		return nil, nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	return reqParams, s.ctx.nsqd.GetTopic(topicName), nil
}

func (s *httpServer) doPUB(req *http.Request) (interface{}, error) {
	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.ctx.nsqd.opts.MaxMsgSize {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	}

	// add 1 so that it's greater than our max when we test for it
	// (LimitReader returns a "fake" EOF)
	readMax := s.ctx.nsqd.opts.MaxMsgSize + 1
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	if int64(len(body)) == readMax {
		s.ctx.nsqd.logf("ERROR: /put hit max message size")
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	}
	if len(body) == 0 {
		return nil, http_api.Err{400, "MSG_EMPTY"}
	}

	_, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	msg := NewMessage(<-s.ctx.nsqd.idChan, body)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, http_api.Err{503, "EXITING"}
	}

	return "OK", nil
}

func (s *httpServer) doMPUB(req *http.Request) (interface{}, error) {
	var msgs []*Message
	var exit bool

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.ctx.nsqd.opts.MaxBodySize {
		return nil, http_api.Err{413, "BODY_TOO_BIG"}
	}

	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	_, ok := reqParams["binary"]
	if ok {
		tmp := make([]byte, 4)
		msgs, err = readMPUB(req.Body, tmp, s.ctx.nsqd.idChan,
			s.ctx.nsqd.opts.MaxMsgSize)
		if err != nil {
			return nil, http_api.Err{413, err.(*protocol.FatalClientErr).Code[2:]}
		}
	} else {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.ctx.nsqd.opts.MaxBodySize + 1
		rdr := bufio.NewReader(io.LimitReader(req.Body, readMax))
		total := 0
		for !exit {
			block, err := rdr.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					return nil, http_api.Err{500, "INTERNAL_ERROR"}
				}
				exit = true
			}
			total += len(block)
			if int64(total) == readMax {
				return nil, http_api.Err{413, "BODY_TOO_BIG"}
			}

			if len(block) > 0 && block[len(block)-1] == '\n' {
				block = block[:len(block)-1]
			}

			// silently discard 0 length messages
			// this maintains the behavior pre 0.2.22
			if len(block) == 0 {
				continue
			}

			if int64(len(block)) > s.ctx.nsqd.opts.MaxMsgSize {
				return nil, http_api.Err{413, "MSG_TOO_BIG"}
			}

			msg := NewMessage(<-s.ctx.nsqd.idChan, block)
			msgs = append(msgs, msg)
		}
	}

	err = topic.PutMessages(msgs)
	if err != nil {
		return nil, http_api.Err{503, "EXITING"}
	}

	return "OK", nil
}

func (s *httpServer) doCreateTopic(req *http.Request) (interface{}, error) {
	_, _, err := s.getTopicFromQuery(req)
	return nil, err
}

func (s *httpServer) doEmptyTopic(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	topic, err := s.ctx.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	err = topic.Empty()
	if err != nil {
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	return nil, nil
}

func (s *httpServer) doDeleteTopic(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	err = s.ctx.nsqd.DeleteExistingTopic(topicName)
	if err != nil {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	return nil, nil
}

func (s *httpServer) doPauseTopic(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	topic, err := s.ctx.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	if strings.Contains(req.URL.Path, "unpause") {
		err = topic.UnPause()
	} else {
		err = topic.Pause()
	}
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// pro-actively persist metadata so in case of process failure
	// nsqd won't suddenly (un)pause a topic
	s.ctx.nsqd.Lock()
	s.ctx.nsqd.PersistMetadata()
	s.ctx.nsqd.Unlock()
	return nil, nil
}

func (s *httpServer) doCreateChannel(req *http.Request) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}
	topic.GetChannel(channelName)
	return nil, nil
}

func (s *httpServer) doEmptyChannel(req *http.Request) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	err = channel.Empty()
	if err != nil {
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	return nil, nil
}

func (s *httpServer) doDeleteChannel(req *http.Request) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	err = topic.DeleteExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	return nil, nil
}

func (s *httpServer) doPauseChannel(req *http.Request) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	if strings.Contains(req.URL.Path, "unpause") {
		err = channel.UnPause()
	} else {
		err = channel.Pause()
	}
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// pro-actively persist metadata so in case of process failure
	// nsqd won't suddenly (un)pause a channel
	s.ctx.nsqd.Lock()
	s.ctx.nsqd.PersistMetadata()
	s.ctx.nsqd.Unlock()
	return nil, nil
}

func (s *httpServer) doStats(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf("ERROR: failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	formatString, _ := reqParams.Get("format")
	jsonFormat := formatString == "json"
	stats := s.ctx.nsqd.GetStats()
	health := s.ctx.nsqd.GetHealth()
	startTime := s.ctx.nsqd.GetStartTime()
	uptime := time.Since(startTime)

	if !jsonFormat {
		return s.printStats(stats, health, startTime, uptime), nil
	}

	return struct {
		Version   string       `json:"version"`
		Health    string       `json:"health"`
		StartTime int64        `json:"start_time"`
		Topics    []TopicStats `json:"topics"`
	}{version.Binary, health, startTime.Unix(), stats}, nil
}

func (s *httpServer) printStats(stats []TopicStats, health string, startTime time.Time, uptime time.Duration) []byte {
	var buf bytes.Buffer
	w := &buf
	now := time.Now()
	io.WriteString(w, fmt.Sprintf("%s\n", version.String("nsqd")))
	io.WriteString(w, fmt.Sprintf("start_time %v\n", startTime.Format(time.RFC3339)))
	io.WriteString(w, fmt.Sprintf("uptime %s\n", uptime))
	if len(stats) == 0 {
		io.WriteString(w, "\nNO_TOPICS\n")
		return buf.Bytes()
	}
	io.WriteString(w, fmt.Sprintf("\nHealth: %s\n", health))
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
	return buf.Bytes()
}
