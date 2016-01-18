package nsqlookupd

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"

	"errors"
	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
	"strconv"
)

const (
	MAX_PARTITION_NUM = 255
	MAX_REPLICATOR    = 5
)

func GetValidPartitionNum(numStr string) (int, error) {
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}
	if num > 0 && num <= MAX_PARTITION_NUM {
		return num, nil
	}
	return 0, errors.New("INVALID_PARTITION_NUM")
}

func GetValidPartitionID(numStr string) (int, error) {
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}
	if num >= 0 && num < MAX_PARTITION_NUM {
		return num, nil
	}
	return 0, errors.New("INVALID_PARTITION_ID")
}

func GetValidReplicator(r string) (int, error) {
	num, err := strconv.Atoi(r)
	if err != nil {
		return 0, err
	}
	if num > 0 && num <= MAX_REPLICATOR {
		return num, nil
	}
	return 0, errors.New("INVALID_REPLICATOR")
}

type httpServer struct {
	ctx    *Context
	router http.Handler
}

func newHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqlookupd.opts.Logger)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqlookupd.opts.Logger)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqlookupd.opts.Logger)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqlookupd.opts.Logger)
	s := &httpServer{
		ctx:    ctx,
		router: router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.NegotiateVersion))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.NegotiateVersion))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.NegotiateVersion))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.NegotiateVersion))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.NegotiateVersion))

	// only v1
	router.Handle("POST", "/loglevel/set", http_api.Decorate(s.doSetLogLevel, log, http_api.V1))
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// deprecated, v1 negotiate
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))

	// debug
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topics := s.ctx.nsqlookupd.DB.FindRegistrations("topic", "*", "", "*").Keys()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	topicPartition, err := reqParams.Get("partition")
	if err != nil {
		topicPartition = "*"
	}
	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*", topicPartition).SubKeys()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}

func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	topicPartition, err := reqParams.Get("partition")
	if err != nil {
		topicPartition = "*"
	}
	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "", topicPartition)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}
	partitionProducers := make(map[string]*PeerInfo)
	allProducers := make(map[string]*Producer, len(registrations))
	for _, r := range registrations {
		producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "", r.PartitionID)
		producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
			s.ctx.nsqlookupd.opts.TombstoneLifetime)
		if len(producers) == 0 {
			continue
		}
		// only for test
		if len(producers) == 1 && len(registrations) == 1 {
			allProducers[producers[0].peerInfo.Id] = producers[0]
			continue
		}
		// filter by leader
		var leaderProducer *Producer
		for _, p := range producers {
			pid, _ := strconv.Atoi(r.PartitionID)
			if s.ctx.nsqlookupd.coordinator.IsTopicLeader(topicName, pid, p.peerInfo.Id) {
				leaderProducer = p
				break
			}
		}
		if leaderProducer != nil {
			partitionProducers[r.PartitionID] = leaderProducer.peerInfo
			allProducers[leaderProducer.peerInfo.Id] = leaderProducer
		}
	}
	producers := make(Producers, 0, len(allProducers))
	for _, p := range allProducers {
		producers = append(producers, p)
	}

	// maybe channels should be under topic partitions?
	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*", topicPartition).SubKeys()
	return map[string]interface{}{
		"channels":   channels,
		"producers":  producers.PeerInfo(),
		"partitions": partitionProducers,
	}, nil
}

func (s *httpServer) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	levelStr, err := reqParams.Get("loglevel")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, http_api.Err{400, "BAD_LEVEL_STRING"}
	}
	nsqlookupLog.SetLevel(int32(level))
	return nil, nil
}

func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	pnumStr, err := reqParams.Get("partition_num")
	if err != nil {
		pnumStr = "3"
	}
	pnum, err := GetValidPartitionNum(pnumStr)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_PARTITION_NUM"}
	}
	replicatorStr, err := reqParams.Get("replicator")
	if err != nil {
		replicatorStr = "3"
	}
	replicator, err := GetValidReplicator(replicatorStr)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_REPLICATOR"}
	}
	nsqlookupLog.Logf("DB: adding topic(%s)", topicName)
	err = s.ctx.nsqlookupd.coordinator.CreateTopic(topicName, pnum, replicator)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	for i := 0; i < pnum; i++ {
		key := Registration{"topic", topicName, "", strconv.Itoa(i)}
		s.ctx.nsqlookupd.DB.AddRegistration(key)
	}

	return nil, nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// not support currently.
	return nil, http_api.Err{501, "DELETE not Implemented"}

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*", "*")
	for _, registration := range registrations {
		nsqlookupLog.Logf("DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	registrations = s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "", "*")
	for _, registration := range registrations {
		nsqlookupLog.Logf("DB: removing topic(%s)", topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	nsqlookupLog.Logf("DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "", "*")
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		if thisNode == node {
			p.Tombstone()
		}
	}

	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	tpid, err := reqParams.Get("partition")
	if err != nil {
		tpid = "*"
	}

	regs := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "", tpid)
	if len(regs) == 0 {
		return nil, http_api.Err{400, "TOPIC_NOT_FOUND"}
	}
	for _, reg := range regs {
		nsqlookupLog.Logf("DB: adding channel(%s) in topic(%s)-pid:%s", channelName, topicName, reg.PartitionID)
		key := Registration{"channel", topicName, channelName, reg.PartitionID}
		s.ctx.nsqlookupd.DB.AddRegistration(key)
	}
	err = s.ctx.nsqlookupd.coordinator.CreateChannelForAll(topicName, channelName)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}

	return nil, nil
}

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// not support currently.
	return nil, http_api.Err{501, "DELETE not Implemented"}

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	tpid, err := reqParams.Get("partition")
	if err != nil {
		tpid = "*"
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName, tpid)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	nsqlookupLog.Logf("DB: removing channel(%s) from topic(%s)-pid:%s", channelName, topicName, tpid)
	for _, registration := range registrations {
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

type node struct {
	RemoteAddress    string              `json:"remote_address"`
	Hostname         string              `json:"hostname"`
	BroadcastAddress string              `json:"broadcast_address"`
	TCPPort          int                 `json:"tcp_port"`
	HTTPPort         int                 `json:"http_port"`
	Version          string              `json:"version"`
	Tombstones       []bool              `json:"tombstones"`
	Topics           []string            `json:"topics"`
	Partitions       map[string][]string `json:"partitions"`
}

func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	for i, p := range producers {
		regs := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.Id).Filter("topic", "*", "", "*")
		topics := regs.Keys()
		partitions := make(map[string][]string)
		for _, r := range regs {
			partitions[r.Key] = append(partitions[r.Key], r.PartitionID)
		}

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "", "*")
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
				}
			}
		}

		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TCPPort:          p.peerInfo.TCPPort,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
			Partitions:       partitions,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.ctx.nsqlookupd.DB.RLock()
	defer s.ctx.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.ctx.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey + ":" + r.PartitionID
		for _, p := range producers {
			m := map[string]interface{}{
				"id":                p.peerInfo.Id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"tcp_port":          p.peerInfo.TCPPort,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			data[key] = append(data[key], m)
		}
	}

	return data, nil
}
