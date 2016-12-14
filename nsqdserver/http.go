package nsqdserver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/internal/clusterinfo"
	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
	"github.com/julienschmidt/httprouter"
)

type httpServer struct {
	ctx         *context
	tlsEnabled  bool
	tlsRequired bool
	router      http.Handler
}

func newHTTPServer(ctx *context, tlsEnabled bool, tlsRequired bool) *httpServer {
	log := http_api.Log(nsqd.NsqLogger())

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(nsqd.NsqLogger())
	router.NotFound = http_api.LogNotFoundHandler(nsqd.NsqLogger())
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(nsqd.NsqLogger())
	s := &httpServer{
		ctx:         ctx,
		tlsEnabled:  tlsEnabled,
		tlsRequired: tlsRequired,
		router:      router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))
	router.Handle("POST", "/loglevel/set", http_api.Decorate(s.doSetLogLevel, log, http_api.V1))
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.NegotiateVersion))

	// v1 negotiate
	router.Handle("POST", "/pub", http_api.Decorate(s.doPUB, http_api.NegotiateVersion))
	router.Handle("POST", "/pubtrace", http_api.Decorate(s.doPUBTrace, http_api.V1))
	router.Handle("POST", "/mpub", http_api.Decorate(s.doMPUB, http_api.NegotiateVersion))
	router.Handle("GET", "/stats", http_api.Decorate(s.doStats, log, http_api.NegotiateVersion))
	router.Handle("GET", "/coordinator/stats", http_api.Decorate(s.doCoordStats, log, http_api.V1))
	router.Handle("GET", "/message/stats", http_api.Decorate(s.doMessageStats, log, http_api.V1))
	router.Handle("GET", "/message/historystats", http_api.Decorate(s.doMessageHistoryStats, log, http_api.V1))
	router.Handle("POST", "/message/trace/enable", http_api.Decorate(s.enableMessageTrace, log, http_api.V1))
	router.Handle("POST", "/message/trace/disable", http_api.Decorate(s.disableMessageTrace, log, http_api.V1))
	router.Handle("POST", "/channel/pause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("POST", "/channel/unpause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/channel/empty", http_api.Decorate(s.doEmptyChannel, log, http_api.V1))
	router.Handle("POST", "/channel/setoffset", http_api.Decorate(s.doSetChannelOffset, log, http_api.V1))
	router.Handle("POST", "/channel/setorder", http_api.Decorate(s.doSetChannelOrder, log, http_api.V1))
	router.Handle("GET", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))

	//router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, http_api.DeprecatedAPI, log, http_api.V1))

	// debug
	router.HandlerFunc("GET", "/debug/pprof/", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handle("PUT", "/debug/setblockrate", http_api.Decorate(setBlockRateHandler, log, http_api.V1))
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
		resp := fmt.Sprintf(`{"message": "TLS_REQUIRED"}`)
		http_api.Respond(w, 403, "", resp)
		return
	}
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	health := s.ctx.getHealth()
	if !s.ctx.isHealthy() {
		return nil, http_api.Err{500, health}
	}
	return health, nil
}

func (s *httpServer) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("loglevel")
	if levelStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, http_api.Err{400, "BAD_LEVEL_STRING"}
	}
	nsqd.NsqLogger().SetLevel(int32(level))
	consistence.SetCoordLogLevel(int32(level))
	return nil, nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	httpPort := 0
	if s.ctx.reverseProxyPort == "" {
		httpPort = s.ctx.realHTTPAddr().Port
	} else {
		httpPort, _ = strconv.Atoi(s.ctx.reverseProxyPort)
	}
	return struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
		StartTime        int64  `json:"start_time"`
		HASupport        bool   `json:"ha_support"`
	}{
		Version:          version.Binary,
		BroadcastAddress: s.ctx.getOpts().BroadcastAddress,
		Hostname:         hostname,
		TCPPort:          s.ctx.realTCPAddr().Port,
		HTTPPort:         httpPort,
		StartTime:        s.ctx.getStartTime().Unix(),
		HASupport:        s.ctx.nsqdCoord != nil,
	}, nil
}

func (s *httpServer) getExistingTopicChannelFromQuery(req *http.Request) (url.Values, *nsqd.Topic, string, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, nil, "", http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, topicPart, channelName, err := http_api.GetTopicPartitionChannelArgs(reqParams)
	if err != nil {
		return nil, nil, "", http_api.Err{400, err.Error()}
	}

	if topicPart == -1 {
		topicPart = s.ctx.getDefaultPartition(topicName)
	}

	topic, err := s.ctx.getExistingTopic(topicName, topicPart)
	if err != nil {
		nsqd.NsqLogger().Logf("topic not found - %s, %v", topicName, err)
		return nil, nil, "", http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	return reqParams, topic, channelName, err
}

func (s *httpServer) getExistingTopicFromQuery(req *http.Request) (url.Values, *nsqd.Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, topicPart, err := http_api.GetTopicPartitionArgs(reqParams)
	if err != nil {
		return nil, nil, http_api.Err{400, err.Error()}
	}

	if topicPart == -1 {
		topicPart = s.ctx.getDefaultPartition(topicName)
	}

	topic, err := s.ctx.getExistingTopic(topicName, topicPart)
	if err != nil {
		nsqd.NsqLogger().Logf("topic not found - %s-%v, %v", topicName, topicPart, err)
		return nil, nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	if topicPart != topic.GetTopicPart() {
		return nil, nil, http_api.Err{http.StatusNotFound, "Topic partition not exist"}
	}

	return reqParams, topic, nil
}

func (s *httpServer) doPUBTrace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return s.internalPUB(w, req, ps, true)
}
func (s *httpServer) doPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return s.internalPUB(w, req, ps, false)
}

func (s *httpServer) internalPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params, enableTrace bool) (interface{}, error) {
	startPub := time.Now().UnixNano()
	// do not support chunked for http pub, use tcp pub instead.
	if req.ContentLength > s.ctx.getOpts().MaxMsgSize {
		return nil, http_api.Err{413, "MSG_TOO_BIG"}
	} else if req.ContentLength <= 0 {
		return nil, http_api.Err{406, "MSG_EMPTY"}
	}

	// add 1 so that it's greater than our max when we test for it
	// (LimitReader returns a "fake" EOF)
	params, topic, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		nsqd.NsqLogger().Logf("get topic err: %v", err)
		return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	readMax := req.ContentLength + 1
	b := topic.BufferPoolGet(int(req.ContentLength))
	defer topic.BufferPoolPut(b)
	asyncAction := !enableTrace
	n, err := io.CopyN(b, io.LimitReader(req.Body, readMax), int64(req.ContentLength))
	body := b.Bytes()[:req.ContentLength]

	if err != nil {
		nsqd.NsqLogger().Logf("read request body error: %v", err)
		body = body[:n]
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// we ignore EOF, maybe the ContentLength is not match?
			nsqd.NsqLogger().LogWarningf("read request body eof: %v, ContentLength: %v,return length %v.",
				err, req.ContentLength, n)
		} else {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
	}
	if len(body) == 0 {
		return nil, http_api.Err{406, "MSG_EMPTY"}
	}

	if s.ctx.checkForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		var err error
		traceIDStr := params.Get("trace_id")
		traceID, err := strconv.ParseUint(traceIDStr, 10, 0)
		if enableTrace && err != nil {
			nsqd.NsqLogger().Logf("trace id invalid %v, %v",
				traceIDStr, err)
			return nil, http_api.Err{400, "INVALID_TRACE_ID"}
		}

		id := nsqd.MessageID(0)
		offset := nsqd.BackendOffset(0)
		rawSize := int32(0)
		if asyncAction {
			err = internalPubAsync(nil, b, topic)
		} else {
			id, offset, rawSize, _, err = s.ctx.PutMessage(topic, body, traceID)
		}
		if err != nil {
			nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)
			if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
				if !clusterErr.IsLocalErr() {
					return nil, http_api.Err{400, FailedOnNotWritable}
				}
			}
			return nil, http_api.Err{503, err.Error()}
		}

		cost := time.Now().UnixNano() - startPub
		topic.GetDetailStats().UpdateTopicMsgStats(int64(len(body)), cost/1000)
		if enableTrace {
			return struct {
				Status      string `json:"status"`
				ID          uint64 `json:"id"`
				TraceID     string `json:"trace_id"`
				QueueOffset uint64 `json:"queue_offset"`
				DataRawSize uint32 `json:"rawsize"`
			}{"OK", uint64(id), traceIDStr, uint64(offset), uint32(rawSize)}, nil
		} else {
			return "OK", nil
		}
	} else {
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		topic.DisableForSlave()
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
}

func (s *httpServer) doMPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	startPub := time.Now().UnixNano()
	if req.ContentLength > s.ctx.getOpts().MaxBodySize {
		return nil, http_api.Err{413, "BODY_TOO_BIG"}
	}

	reqParams, topic, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	var msgs []*nsqd.Message
	var buffers []*bytes.Buffer
	var exit bool

	_, ok := reqParams["binary"]
	if ok {
		tmp := make([]byte, 4)
		msgs, buffers, err = readMPUB(req.Body, tmp, topic,
			s.ctx.getOpts().MaxMsgSize, false)
		defer func() {
			for _, b := range buffers {
				topic.BufferPoolPut(b)
			}
		}()

		if err != nil {
			return nil, http_api.Err{413, err.(*protocol.FatalClientErr).Code[2:]}
		}
	} else {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.ctx.getOpts().MaxBodySize + 1
		rdr := nsqd.NewBufioReader(io.LimitReader(req.Body, readMax))
		defer nsqd.PutBufioReader(rdr)
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

			if int64(len(block)) > s.ctx.getOpts().MaxMsgSize {
				return nil, http_api.Err{413, "MSG_TOO_BIG"}
			}

			msg := nsqd.NewMessage(0, block)
			msgs = append(msgs, msg)
			topic.GetDetailStats().UpdateTopicMsgStats(int64(len(block)), 0)
		}
	}

	if s.ctx.checkForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		_, _, _, err := s.ctx.PutMessages(topic, msgs)
		//s.ctx.setHealth(err)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)
			if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
				if !clusterErr.IsLocalErr() {
					return nil, http_api.Err{400, FailedOnNotWritable}
				}
			}
			return nil, http_api.Err{503, err.Error()}
		}
	} else {
		//should we forward to master of topic?
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		topic.DisableForSlave()
		return nil, http_api.Err{400, FailedOnNotLeader}
	}

	cost := time.Now().UnixNano() - startPub
	topic.GetDetailStats().UpdateTopicMsgStats(0, cost/1000/int64(len(msgs)))
	return "OK", nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, topicPart, err := http_api.GetTopicPartitionArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if topicPart == -1 {
		topicPart = s.ctx.getDefaultPartition(topicName)
	}
	err = s.ctx.deleteExistingTopic(topicName, topicPart)
	if err != nil {
		return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}

	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}
	topic.GetChannel(channelName)
	return nil, nil
}

func (s *httpServer) doEmptyChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	if s.ctx.checkForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		var startFrom ConsumeOffset
		startFrom.OffsetType = offsetSpecialType
		startFrom.OffsetValue = -1
		queueOffset, cnt, err := s.ctx.SetChannelOffset(channel, &startFrom, true)
		if err != nil {
			return nil, http_api.Err{500, err.Error()}
		}
		nsqd.NsqLogger().Logf("empty the channel to end offset: %v:%v, by client:%v",
			queueOffset, cnt, req.RemoteAddr)
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	return nil, nil
}

func (s *httpServer) doSetChannelOrder(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	orderStr := reqParams.Get("order")
	if orderStr == "true" {
		channel.SetOrdered(true)
	} else if orderStr == "false" {
		channel.SetOrdered(false)
	} else {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}
	return nil, nil
}

func (s *httpServer) doSetChannelOffset(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}
	readMax := req.ContentLength + 1
	body := make([]byte, req.ContentLength)
	n, err := io.ReadFull(io.LimitReader(req.Body, readMax), body)
	if err != nil {
		nsqd.NsqLogger().Logf("read request body error: %v", err)
		body = body[:n]
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// we ignore EOF, maybe the ContentLength is not match?
			nsqd.NsqLogger().LogWarningf("read request body eof: %v, ContentLength: %v,return length %v.",
				err, req.ContentLength, n)
		} else {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
	}
	if len(body) == 0 {
		return nil, http_api.Err{406, "MSG_EMPTY"}
	}
	startFrom := &ConsumeOffset{}
	err = startFrom.FromBytes(body)
	if err != nil {
		nsqd.NsqLogger().Logf("offset %v error: %v", string(body), err)
		return nil, http_api.Err{400, err.Error()}
	}

	if s.ctx.checkForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		queueOffset, cnt, err := s.ctx.SetChannelOffset(channel, startFrom, true)
		if err != nil {
			return nil, http_api.Err{500, err.Error()}
		}
		nsqd.NsqLogger().Logf("set the channel offset: %v (actual set : %v:%v), by client:%v",
			startFrom, queueOffset, cnt, req.RemoteAddr)
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}
	return nil, nil
}

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
	if err != nil {
		return nil, err
	}

	if s.ctx.checkForMasterWrite(topic.GetTopicName(), topic.GetTopicPart()) {
		clusterErr := s.ctx.DeleteExistingChannel(topic, channelName)
		if clusterErr != nil {
			return nil, http_api.Err{500, clusterErr.Error()}
		}
		nsqd.NsqLogger().Logf("deleted the channel : %v, by client:%v",
			channelName, req.RemoteAddr)
	} else {
		nsqd.NsqLogger().LogDebugf("should request to master: %v, from %v",
			topic.GetFullName(), req.RemoteAddr)
		return nil, http_api.Err{400, FailedOnNotLeader}
	}

	return nil, nil
}

func (s *httpServer) doPauseChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicChannelFromQuery(req)
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
		nsqd.NsqLogger().LogErrorf("failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	// pro-actively persist metadata so in case of process failure
	s.ctx.persistMetadata()
	return nil, nil
}

func (s *httpServer) enableMessageTrace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	channelName := reqParams.Get("channel")

	parts := s.ctx.getPartitions(topicName)
	for _, t := range parts {
		if channelName != "" {
			ch, err := t.GetExistingChannel(channelName)
			if err != nil {
				continue
			}
			ch.SetTrace(true)
			nsqd.NsqLogger().Logf("channel %v trace enabled", ch.GetName())
		} else {
			t.SetTrace(true)
			nsqd.NsqLogger().Logf("topic %v trace enabled", t.GetFullName())
		}
	}
	return nil, nil
}

func (s *httpServer) disableMessageTrace(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	channelName := reqParams.Get("channel")
	parts := s.ctx.getPartitions(topicName)
	for _, t := range parts {
		if channelName != "" {
			ch, err := t.GetExistingChannel(channelName)
			if err != nil {
				continue
			}
			ch.SetTrace(false)
			nsqd.NsqLogger().Logf("channel %v trace disabled", ch.GetName())
		} else {
			t.SetTrace(false)
			nsqd.NsqLogger().Logf("topic %v trace disabled", t.GetFullName())
		}
	}
	return nil, nil
}

func (s *httpServer) doMessageHistoryStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	topicPartStr := reqParams.Get("partition")
	topicPart, err := strconv.Atoi(topicPartStr)
	if err != nil && topicName != "" && topicPartStr != "" {
		nsqd.NsqLogger().LogErrorf("failed to get partition - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	if topicName == "" && topicPartStr == "" {
		topicStats := s.ctx.getStats(true, "")
		var topicHourlyPubStatsList []*clusterinfo.NodeHourlyPubsize
		for _, topicStat := range topicStats {
			partitionNum, err := strconv.Atoi(topicStat.TopicPartition)
			if err != nil {
				nsqd.NsqLogger().LogErrorf("failed to parse partition string %s - %s", topicStat.TopicPartition, err)
				continue
			}
			topic, err := s.ctx.getExistingTopic(topicStat.TopicName, partitionNum)

			if err != nil {
				nsqd.NsqLogger().LogErrorf("failure to get topic  %s:%s - %s", topicStat.TopicName, topicStat.TopicPartition, err)
				continue
			}
			pubhs := topic.GetDetailStats().GetHourlyStats()
			cur := time.Now().Hour() + 2 + 21
			latestHourlyPubsize := pubhs[cur%len(pubhs)]
			aTopicHourlyPubStat := &clusterinfo.NodeHourlyPubsize{
				TopicName:      topicStat.TopicName,
				TopicPartition: topicStat.TopicPartition,
				HourlyPubSize:  latestHourlyPubsize,
			}
			topicHourlyPubStatsList = append(topicHourlyPubStatsList, aTopicHourlyPubStat)
		}

		return struct {
			NodehourlyPubsizeStats []*clusterinfo.NodeHourlyPubsize `json:"node_hourly_pub_size_stats"`
		}{topicHourlyPubStatsList}, nil
	} else {

		t, err := s.ctx.getExistingTopic(topicName, topicPart)
		if err != nil {
			return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
		}
		pubhs := t.GetDetailStats().GetHourlyStats()
		// since the newest 2 hours data maybe update during get, we ignore the newest 2 points
		cur := time.Now().Hour() + 2
		pubhsNew := make([]int64, 0, 22)
		for len(pubhsNew) < 22 {
			pubhsNew = append(pubhsNew, pubhs[cur%len(pubhs)])
			cur++
		}
		return struct {
			HourlyPubSize []int64 `json:"hourly_pub_size"`
		}{pubhsNew}, nil
	}
}

func (s *httpServer) doMessageStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	topicName := reqParams.Get("topic")
	topicPartStr := reqParams.Get("partition")
	topicPart, err := strconv.Atoi(topicPartStr)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to get partition - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	channelName := reqParams.Get("channel")

	t, err := s.ctx.getExistingTopic(topicName, topicPart)
	if err != nil {
		return nil, http_api.Err{404, E_TOPIC_NOT_EXIST}
	}
	statStr := t.GetTopicChannelDebugStat(channelName)

	return statStr, nil
}

func (s *httpServer) doCoordStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqdCoord != nil {
		reqParams, err := url.ParseQuery(req.URL.RawQuery)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
			return nil, http_api.Err{400, "INVALID_REQUEST"}
		}
		topicName := reqParams.Get("topic")
		topicPartStr := reqParams.Get("partition")
		topicPart := -1
		if topicPartStr != "" {
			topicPart, err = strconv.Atoi(topicPartStr)
			if err != nil {
				nsqd.NsqLogger().LogErrorf("invalid partition: %v - %s", topicPartStr, err)
				return nil, http_api.Err{400, "INVALID_REQUEST"}
			}
		}

		return s.ctx.nsqdCoord.Stats(topicName, topicPart), nil
	}
	return nil, http_api.Err{500, "Coordinator is disabled."}
}

func (s *httpServer) doStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to parse request params - %s", err)
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	formatString := reqParams.Get("format")
	topicName := reqParams.Get("topic")
	topicPart := reqParams.Get("partition")
	channelName := reqParams.Get("channel")
	leaderOnlyStr := reqParams.Get("leaderOnly")
	var leaderOnly bool
	leaderOnly, _ = strconv.ParseBool(leaderOnlyStr)

	jsonFormat := formatString == "json"

	stats := s.ctx.getStats(leaderOnly, topicName)
	health := s.ctx.getHealth()
	startTime := s.ctx.getStartTime()
	uptime := time.Since(startTime)

	// If we WERE given a channel-name, remove stats for all the other channels:
	// If we need the partition for topic, remove other partitions
	if len(channelName) > 0 || len(topicPart) > 0 {
		filteredStats := make([]nsqd.TopicStats, 0, len(stats))
		// Find the desired-topic-index:
		for _, topicStats := range stats {
			if len(topicPart) > 0 && topicStats.TopicPartition != topicPart {
				nsqd.NsqLogger().Logf("ignored stats topic partition mismatch - %v, %v", topicPart, topicStats.TopicPartition)
				continue
			}
			if len(channelName) > 0 {
				// Find the desired-channel:
				for _, channelStats := range topicStats.Channels {
					if channelStats.ChannelName == channelName {
						topicStats.Channels = []nsqd.ChannelStats{channelStats}
						// We've got the channel we were looking for:
						break
					}
				}
			}

			// We've got the topic we were looking for:
			// now only the mulit ordered topic can have several partitions on the same node
			filteredStats = append(filteredStats, topicStats)
		}
		stats = filteredStats
	}

	if !jsonFormat {
		return s.printStats(stats, health, startTime, uptime), nil
	}

	return struct {
		Version   string            `json:"version"`
		Health    string            `json:"health"`
		StartTime int64             `json:"start_time"`
		Topics    []nsqd.TopicStats `json:"topics"`
	}{version.Binary, health, startTime.Unix(), stats}, nil
}

func (s *httpServer) printStats(stats []nsqd.TopicStats, health string, startTime time.Time, uptime time.Duration) []byte {
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
		pausedPrefix = "   "
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

func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	opt := ps.ByName("opt")

	if req.Method == "PUT" {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.ctx.getOpts().MaxMsgSize + 1
		body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{413, "INVALID_VALUE"}
		}

		opts := *s.ctx.getOpts()
		switch opt {
		case "nsqlookupd_tcp_addresses":
			err := json.Unmarshal(body, &opts.NSQLookupdTCPAddresses)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "verbose":
			err := json.Unmarshal(body, &opts.Verbose)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		case "log_level":
			err := json.Unmarshal(body, &opts.LogLevel)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
			nsqd.NsqLogger().Logf("log level set to : %v", opts.LogLevel)
		default:
			return nil, http_api.Err{400, "INVALID_OPTION"}
		}
		s.ctx.swapOpts(&opts)
		s.ctx.triggerOptsNotification()
	}

	v, ok := getOptByCfgName(s.ctx.getOpts(), opt)
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
