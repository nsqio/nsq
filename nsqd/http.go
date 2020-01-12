package nsqd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

var boolParams = map[string]bool{
	"true":  true,
	"1":     true,
	"false": false,
	"0":     false,
}

type httpServer struct {
	ctx         *context
	tlsEnabled  bool
	tlsRequired bool
	router      http.Handler
}

func newHTTPServer(ctx *context, tlsEnabled bool, tlsRequired bool) *httpServer {
	log := http_api.Log(ctx.nsqd.logf)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqd.logf)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqd.logf)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqd.logf)
	s := &httpServer{
		ctx:         ctx,
		tlsEnabled:  tlsEnabled,
		tlsRequired: tlsRequired,
		router:      router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))

	// v1 negotiate
	router.Handle("POST", "/pub", http_api.Decorate(s.doPUB, http_api.V1))
	router.Handle("POST", "/mpub", http_api.Decorate(s.doMPUB, http_api.V1))
	router.Handle("GET", "/stats", http_api.Decorate(s.doStats, log, http_api.V1))

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/topic/empty", http_api.Decorate(s.doEmptyTopic, log, http_api.V1))
	router.Handle("POST", "/topic/pause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))
	router.Handle("POST", "/topic/unpause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/channel/empty", http_api.Decorate(s.doEmptyChannel, log, http_api.V1))
	router.Handle("POST", "/channel/pause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("POST", "/channel/unpause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("GET", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))

	// debug
	router.HandlerFunc("GET", "/debug/pprof/", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handle("PUT", "/debug/setblockrate", http_api.Decorate(setBlockRateHandler, log, http_api.PlainText))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func setBlockRateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	rate, err := strconv.Atoi(req.FormValue("rate"))
	if err != nil {
		return nil, http_api.Err{http.StatusBadRequest, fmt.Sprintf("invalid block rate : %s", err.Error())}
	}
	runtime.SetBlockProfileRate(rate)
	return nil, nil
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !s.tlsEnabled && s.tlsRequired {
		resp := fmt.Sprintf(`{"message": "TLS_REQUIRED", "https_port": %d}`,
			s.ctx.nsqd.RealHTTPSAddr().Port)
		w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(403)
		io.WriteString(w, resp)
		return
	}
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	health := s.ctx.nsqd.GetHealth()
	if !s.ctx.nsqd.IsHealthy() {
		return nil, http_api.Err{500, health}
	}
	return health, nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	return struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
		StartTime        int64  `json:"start_time"`
	}{
		Version:          version.Binary,
		BroadcastAddress: s.ctx.nsqd.getOpts().BroadcastAddress,
		Hostname:         hostname,
		TCPPort:          s.ctx.nsqd.RealTCPAddr().Port,
		HTTPPort:         s.ctx.nsqd.RealHTTPAddr().Port,
		StartTime:        s.ctx.nsqd.GetStartTime().Unix(),
	}, nil
}

func (s *httpServer) getExistingTopicFromQuery(req *http.Request) (*http_api.ReqParams, *Topic, string, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
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
		s.ctx.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
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

func (s *httpServer) doPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	}

	// add 1 so that it's greater than our max when we test for it
	// (LimitReader returns a "fake" EOF)
	readMax := s.ctx.nsqd.getOpts().MaxMsgSize + 1
	body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil {
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	if int64(len(body)) == readMax {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	}
	if len(body) == 0 {
		return nil, http_api.Err{400, "MSG_EMPTY"}
	}

	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	var deferred time.Duration
	if ds, ok := reqParams["defer"]; ok {
		var di int64
		di, err = strconv.ParseInt(ds[0], 10, 64)
		if err != nil {
			return nil, http_api.Err{400, "INVALID_DEFER"}
		}
		deferred = time.Duration(di) * time.Millisecond
		if deferred < 0 || deferred > s.ctx.nsqd.getOpts().MaxReqTimeout {
			return nil, http_api.Err{400, "INVALID_DEFER"}
		}
	}

	msg := NewMessage(topic.GenerateID(), body)
	msg.deferred = deferred
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, http_api.Err{503, "EXITING"}
	}

	return "OK", nil
}

func (s *httpServer) doMPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var msgs []*Message
	var exit bool

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.ctx.nsqd.getOpts().MaxBodySize {
		return nil, http_api.Err{413, "BODY_TOO_BIG"}
	}

	reqParams, topic, err := s.getTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	// text mode is default, but unrecognized binary opt considered true
	binaryMode := false
	if vals, ok := reqParams["binary"]; ok {
		if binaryMode, ok = boolParams[vals[0]]; !ok {
			binaryMode = true
			s.ctx.nsqd.logf(LOG_WARN, "deprecated value '%s' used for /mpub binary param", vals[0])
		}
	}
	if binaryMode {
		tmp := make([]byte, 4)
		msgs, err = readMPUB(req.Body, tmp, topic,
			s.ctx.nsqd.getOpts().MaxMsgSize, s.ctx.nsqd.getOpts().MaxBodySize)
		if err != nil {
			return nil, http_api.Err{413, err.(*protocol.FatalClientErr).Code[2:]}
		}
	} else {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.ctx.nsqd.getOpts().MaxBodySize + 1
		rdr := bufio.NewReader(io.LimitReader(req.Body, readMax))
		total := 0
		for !exit {
			var block []byte
			block, err = rdr.ReadBytes('\n')
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

			if int64(len(block)) > s.ctx.nsqd.getOpts().MaxMsgSize {
				return nil, http_api.Err{413, "MSG_TOO_BIG"}
			}

			msg := NewMessage(topic.GenerateID(), block)
			msgs = append(msgs, msg)
		}
	}

	err = topic.PutMessages(msgs)
	if err != nil {
		return nil, http_api.Err{503, "EXITING"}
	}

	return "OK", nil
}

func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, _, err := s.getTopicFromQuery(req)
	return nil, err
}

func (s *httpServer) doEmptyTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
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

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
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

func (s *httpServer) doPauseTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
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
		s.ctx.nsqd.logf(LOG_ERROR, "failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// pro-actively persist metadata so in case of process failure
	// nsqd won't suddenly (un)pause a topic
	s.ctx.nsqd.Lock()
	s.ctx.nsqd.PersistMetadata()
	s.ctx.nsqd.Unlock()
	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}
	topic.GetChannel(channelName)
	return nil, nil
}

func (s *httpServer) doEmptyChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
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

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
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

func (s *httpServer) doPauseChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
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
		s.ctx.nsqd.logf(LOG_ERROR, "failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// pro-actively persist metadata so in case of process failure
	// nsqd won't suddenly (un)pause a channel
	s.ctx.nsqd.Lock()
	s.ctx.nsqd.PersistMetadata()
	s.ctx.nsqd.Unlock()
	return nil, nil
}

func (s *httpServer) doStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var producerStats []ClientStats

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.ctx.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	formatString, _ := reqParams.Get("format")
	topicName, _ := reqParams.Get("topic")
	channelName, _ := reqParams.Get("channel")
	includeClientsParam, _ := reqParams.Get("include_clients")
	includeMemParam, _ := reqParams.Get("include_mem")
	jsonFormat := formatString == "json"

	includeClients, ok := boolParams[includeClientsParam]
	if !ok {
		includeClients = true
	}
	includeMem, ok := boolParams[includeMemParam]
	if !ok {
		includeMem = true
	}
	if includeClients {
		producerStats = s.ctx.nsqd.GetProducerStats()
	}

	stats := s.ctx.nsqd.GetStats(topicName, channelName, includeClients)
	health := s.ctx.nsqd.GetHealth()
	startTime := s.ctx.nsqd.GetStartTime()
	uptime := time.Since(startTime)

	// filter by topic (if specified)
	if len(topicName) > 0 {
		for _, topicStats := range stats {
			if topicStats.TopicName == topicName {
				// filter by channel (if specified)
				if len(channelName) > 0 {
					for _, channelStats := range topicStats.Channels {
						if channelStats.ChannelName == channelName {
							topicStats.Channels = []ChannelStats{channelStats}
							break
						}
					}
				}
				stats = []TopicStats{topicStats}
				break
			}
		}

		filteredProducerStats := make([]ClientStats, 0)
		for _, clientStat := range producerStats {
			var found bool
			var count uint64
			for _, v := range clientStat.PubCounts {
				if v.Topic == topicName {
					count = v.Count
					found = true
					break
				}
			}
			if !found {
				continue
			}
			clientStat.PubCounts = []PubCount{PubCount{
				Topic: topicName,
				Count: count,
			}}
			filteredProducerStats = append(filteredProducerStats, clientStat)
		}
		producerStats = filteredProducerStats
	}

	var ms *memStats
	if includeMem {
		m := getMemStats()
		ms = &m
	}
	if !jsonFormat {
		return s.printStats(stats, producerStats, ms, health, startTime, uptime), nil
	}

	return struct {
		Version   string        `json:"version"`
		Health    string        `json:"health"`
		StartTime int64         `json:"start_time"`
		Topics    []TopicStats  `json:"topics"`
		Memory    *memStats     `json:"memory,omitempty"`
		Producers []ClientStats `json:"producers"`
	}{version.Binary, health, startTime.Unix(), stats, ms, producerStats}, nil
}

func (s *httpServer) printStats(stats []TopicStats, producerStats []ClientStats, ms *memStats, health string, startTime time.Time, uptime time.Duration) []byte {
	var buf bytes.Buffer
	w := &buf

	now := time.Now()

	fmt.Fprintf(w, "%s\n", version.String("nsqd"))
	fmt.Fprintf(w, "start_time %v\n", startTime.Format(time.RFC3339))
	fmt.Fprintf(w, "uptime %s\n", uptime)

	fmt.Fprintf(w, "\nHealth: %s\n", health)

	if ms != nil {
		fmt.Fprintf(w, "\nMemory:\n")
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_objects", ms.HeapObjects)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_idle_bytes", ms.HeapIdleBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_in_use_bytes", ms.HeapInUseBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_released_bytes", ms.HeapReleasedBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_100", ms.GCPauseUsec100)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_99", ms.GCPauseUsec99)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_95", ms.GCPauseUsec95)
		fmt.Fprintf(w, "   %-25s\t%d\n", "next_gc_bytes", ms.NextGCBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_total_runs", ms.GCTotalRuns)
	}

	if len(stats) == 0 {
		fmt.Fprintf(w, "\nTopics: None\n")
	} else {
		fmt.Fprintf(w, "\nTopics:")
	}

	for _, t := range stats {
		var pausedPrefix string
		if t.Paused {
			pausedPrefix = "*P "
		} else {
			pausedPrefix = "   "
		}
		fmt.Fprintf(w, "\n%s[%-15s] depth: %-5d be-depth: %-5d msgs: %-8d e2e%%: %s\n",
			pausedPrefix,
			t.TopicName,
			t.Depth,
			t.BackendDepth,
			t.MessageCount,
			t.E2eProcessingLatency,
		)
		for _, c := range t.Channels {
			if c.Paused {
				pausedPrefix = "   *P "
			} else {
				pausedPrefix = "      "
			}
			fmt.Fprintf(w, "%s[%-25s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d re-q: %-5d timeout: %-5d msgs: %-8d e2e%%: %s\n",
				pausedPrefix,
				c.ChannelName,
				c.Depth,
				c.BackendDepth,
				c.InFlightCount,
				c.DeferredCount,
				c.RequeueCount,
				c.TimeoutCount,
				c.MessageCount,
				c.E2eProcessingLatency,
			)
			for _, client := range c.Clients {
				connectTime := time.Unix(client.ConnectTime, 0)
				// truncate to the second
				duration := time.Duration(int64(now.Sub(connectTime).Seconds())) * time.Second
				fmt.Fprintf(w, "        [%s %-21s] state: %d inflt: %-4d rdy: %-4d fin: %-8d re-q: %-8d msgs: %-8d connected: %s\n",
					client.Version,
					client.ClientID,
					client.State,
					client.InFlightCount,
					client.ReadyCount,
					client.FinishCount,
					client.RequeueCount,
					client.MessageCount,
					duration,
				)
			}
		}
	}

	if len(producerStats) == 0 {
		fmt.Fprintf(w, "\nProducers: None\n")
	} else {
		fmt.Fprintf(w, "\nProducers:")
		for _, client := range producerStats {
			connectTime := time.Unix(client.ConnectTime, 0)
			// truncate to the second
			duration := time.Duration(int64(now.Sub(connectTime).Seconds())) * time.Second
			var totalPubCount uint64
			for _, v := range client.PubCounts {
				totalPubCount += v.Count
			}
			fmt.Fprintf(w, "\n   [%s %-21s] msgs: %-8d connected: %s\n",
				client.Version,
				client.ClientID,
				totalPubCount,
				duration,
			)
			for _, v := range client.PubCounts {
				fmt.Fprintf(w, "      [%-15s] msgs: %-8d\n",
					v.Topic,
					v.Count,
				)
			}
		}
	}

	return buf.Bytes()
}

func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	opt := ps.ByName("opt")

	if req.Method == "PUT" {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.ctx.nsqd.getOpts().MaxMsgSize + 1
		body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{413, "INVALID_VALUE"}
		}

		opts := *s.ctx.nsqd.getOpts()
		switch opt {
		case "nsqlookupd_tcp_addresses":
			err := json.Unmarshal(body, &opts.NSQLookupdTCPAddresses)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "log_level":
			logLevelStr := string(body)
			logLevel, err := lg.ParseLogLevel(logLevelStr)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			opts.LogLevel = logLevel
		default:
			return nil, http_api.Err{400, "INVALID_OPTION"}
		}
		s.ctx.nsqd.swapOpts(&opts)
		s.ctx.nsqd.triggerOptsNotification()
	}

	v, ok := getOptByCfgName(s.ctx.nsqd.getOpts(), opt)
	if !ok {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}

	return v, nil
}

func getOptByCfgName(opts interface{}, name string) (interface{}, bool) {
	val := reflect.ValueOf(opts).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		flagName := field.Tag.Get("flag")
		cfgName := field.Tag.Get("cfg")
		if flagName == "" {
			continue
		}
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}
		if name != cfgName {
			continue
		}
		return val.FieldByName(field.Name).Interface(), true
	}
	return nil, false
}
