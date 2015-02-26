package nsqlookupd

import (
	"fmt"
	"io"
	"net/http"
	httpprof "net/http/pprof"
	"sync/atomic"

	"github.com/bitly/nsq/internal/http_api"
	"github.com/bitly/nsq/internal/protocol"
	"github.com/bitly/nsq/internal/version"
)

type httpServer struct {
	ctx *Context
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
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
		s.ctx.nsqlookupd.logf("ERROR: %s", err)
		http_api.Respond(w, 404, "NOT_FOUND", nil)
	}
}

func (s *httpServer) debugRouter(w http.ResponseWriter, req *http.Request) error {
	switch req.URL.Path {
	case "/debug":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doDebug(req) })
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
	case "/ping":
		s.pingHandler(w, req)

	case "/lookup":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doLookup(req) })
	case "/topics":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doTopics(req) })
	case "/channels":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doChannels(req) })
	case "/nodes":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doNodes(req) })

	case "/topic/create":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doCreateTopic(req) }))
	case "/topic/delete":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doDeleteTopic(req) }))
	case "/topic/tombstone":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doTombstoneTopicProducer(req) }))

	case "/channel/create":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doCreateChannel(req) }))
	case "/channel/delete":
		http_api.V1Wrapper(w, req, http_api.RequirePOST(req,
			func() (interface{}, error) { return s.doDeleteChannel(req) }))

	default:
		return fmt.Errorf("404 %s", req.URL.Path)
	}
	return nil
}

func (s *httpServer) deprecatedRouter(w http.ResponseWriter, req *http.Request) error {
	switch req.URL.Path {
	case "/info":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doInfo(req) })
	case "/delete_topic":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doDeleteTopic(req) })
	case "/delete_channel":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doDeleteChannel(req) })
	case "/tombstone_topic_producer":
		http_api.NegotiateVersionWrapper(w, req,
			func() (interface{}, error) { return s.doTombstoneTopicProducer(req) })
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
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) doInfo(req *http.Request) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

func (s *httpServer) doTopics(req *http.Request) (interface{}, error) {
	topics := s.ctx.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

func (s *httpServer) doChannels(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}

func (s *httpServer) doLookup(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registration := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}

func (s *httpServer) doCreateTopic(req *http.Request) (interface{}, error) {
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

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	key := Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteTopic(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	registrations = s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing topic(%s)", topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

func (s *httpServer) doTombstoneTopicProducer(req *http.Request) (interface{}, error) {
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

	s.ctx.nsqlookupd.logf("DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		if thisNode == node {
			p.Tombstone()
		}
	}

	return nil, nil
}

func (s *httpServer) doCreateChannel(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: adding channel(%s) in topic(%s)", channelName, topicName)
	key := Registration{"channel", topicName, channelName}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteChannel(req *http.Request) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", channelName, topicName)
	for _, registration := range registrations {
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

func (s *httpServer) doNodes(req *http.Request) (interface{}, error) {
	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	for i, p := range producers {
		topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
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
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func (s *httpServer) doDebug(req *http.Request) (interface{}, error) {
	s.ctx.nsqlookupd.DB.RLock()
	defer s.ctx.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.ctx.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		for _, p := range producers {
			m := map[string]interface{}{
				"id":                p.peerInfo.id,
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
