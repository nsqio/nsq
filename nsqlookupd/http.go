package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"io"
	"log"
	"net/http"
)

type httpServer struct {
	context *Context
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/ping":
		s.pingHandler(w, req)
	case "/info":
		s.infoHandler(w, req)
	case "/lookup":
		s.lookupHandler(w, req)
	case "/topics":
		s.topicsHandler(w, req)
	case "/channels":
		s.channelsHandler(w, req)
	case "/nodes":
		s.nodesHandler(w, req)
	case "/delete_topic":
		s.deleteTopicHandler(w, req)
	case "/delete_channel":
		s.deleteChannelHandler(w, req)
	case "/tombstone_topic_producer":
		s.tombstoneTopicProducerHandler(w, req)
	case "/create_topic":
		s.createTopicHandler(w, req)
	case "/create_channel":
		s.createChannelHandler(w, req)
	case "/debug":
		s.debugHandler(w, req)
	default:
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
	}
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (s *httpServer) topicsHandler(w http.ResponseWriter, req *http.Request) {
	topics := s.context.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()
	data := make(map[string]interface{})
	data["topics"] = topics
	util.ApiResponse(w, 200, "OK", data)
}

func (s *httpServer) channelsHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	channels := s.context.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	data := make(map[string]interface{})
	data["channels"] = channels
	util.ApiResponse(w, 200, "OK", data)
}

func (s *httpServer) lookupHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	registration := s.context.nsqlookupd.DB.FindRegistrations("topic", topicName, "")

	if len(registration) == 0 {
		util.ApiResponse(w, 500, "INVALID_ARG_TOPIC", nil)
		return
	}

	channels := s.context.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.context.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.context.nsqlookupd.inactiveProducerTimeout, s.context.nsqlookupd.tombstoneLifetime)
	data := make(map[string]interface{})
	data["channels"] = channels
	data["producers"] = producers.PeerInfo()

	util.ApiResponse(w, 200, "OK", data)
}

func (s *httpServer) createTopicHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
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

	log.Printf("DB: adding topic(%s)", topicName)
	key := Registration{"topic", topicName, ""}
	s.context.nsqlookupd.DB.AddRegistration(key)

	util.ApiResponse(w, 200, "OK", nil)
}

func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	registrations := s.context.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		log.Printf("DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.context.nsqlookupd.DB.RemoveRegistration(registration)
	}

	registrations = s.context.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		log.Printf("DB: removing topic(%s)", topicName)
		s.context.nsqlookupd.DB.RemoveRegistration(registration)
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func (s *httpServer) tombstoneTopicProducerHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	node, err := reqParams.Get("node")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_NODE", nil)
		return
	}

	log.Printf("DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	producers := s.context.nsqlookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HttpPort)
		if thisNode == node {
			p.Tombstone()
		}
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func (s *httpServer) createChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}

	log.Printf("DB: adding channel(%s) in topic(%s)", channelName, topicName)
	key := Registration{"channel", topicName, channelName}
	s.context.nsqlookupd.DB.AddRegistration(key)

	log.Printf("DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	s.context.nsqlookupd.DB.AddRegistration(key)

	util.ApiResponse(w, 200, "OK", nil)
}

func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		util.ApiResponse(w, 500, err.Error(), nil)
		return
	}

	registrations := s.context.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
		return
	}

	log.Printf("DB: removing channel(%s) from topic(%s)", channelName, topicName)
	for _, registration := range registrations {
		s.context.nsqlookupd.DB.RemoveRegistration(registration)
	}

	util.ApiResponse(w, 200, "OK", nil)
}

// note: we can't embed the *Producer here because embeded objects are ignored for json marshalling
type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TcpPort          int      `json:"tcp_port"`
	HttpPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

func (s *httpServer) nodesHandler(w http.ResponseWriter, req *http.Request) {
	// dont filter out tombstoned nodes
	producers := s.context.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(s.context.nsqlookupd.inactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	for i, p := range producers {
		topics := s.context.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			topicProducers := s.context.nsqlookupd.DB.FindProducers("topic", t, "")
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.context.nsqlookupd.tombstoneLifetime)
				}
			}
		}

		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TcpPort:          p.peerInfo.TcpPort,
			HttpPort:         p.peerInfo.HttpPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
		}
	}

	data := make(map[string]interface{})
	data["producers"] = nodes
	util.ApiResponse(w, 200, "OK", data)
}

func (s *httpServer) infoHandler(w http.ResponseWriter, req *http.Request) {
	util.ApiResponse(w, 200, "OK", struct {
		Version string `json:"version"`
	}{
		Version: util.BINARY_VERSION,
	})
}

func (s *httpServer) debugHandler(w http.ResponseWriter, req *http.Request) {
	s.context.nsqlookupd.DB.RLock()
	defer s.context.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.context.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		data[key] = make([]map[string]interface{}, 0)
		for _, p := range producers {
			m := make(map[string]interface{})
			m["id"] = p.peerInfo.id
			m["hostname"] = p.peerInfo.Hostname
			m["broadcast_address"] = p.peerInfo.BroadcastAddress
			m["tcp_port"] = p.peerInfo.TcpPort
			m["http_port"] = p.peerInfo.HttpPort
			m["version"] = p.peerInfo.Version
			m["last_update"] = p.peerInfo.lastUpdate.UnixNano()
			m["tombstoned"] = p.tombstoned
			m["tombstoned_at"] = p.tombstonedAt.UnixNano()
			data[key] = append(data[key], m)
		}
	}

	util.ApiResponse(w, 200, "OK", data)
}
