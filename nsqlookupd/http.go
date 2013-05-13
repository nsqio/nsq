package main

import (
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

func httpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/info", infoHandler)
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/lookup", lookupHandler)
	handler.HandleFunc("/topics", topicsHandler)
	handler.HandleFunc("/channels", channelsHandler)
	handler.HandleFunc("/nodes", nodesHandler)
	handler.HandleFunc("/delete_topic", deleteTopicHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)
	handler.HandleFunc("/tombstone_topic_producer", tombstoneTopicProducerHandler)
	handler.HandleFunc("/create_topic", createTopicHandler)
	handler.HandleFunc("/create_channel", createChannelHandler)
	handler.HandleFunc("/debug", debugHandler)

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

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func topicsHandler(w http.ResponseWriter, req *http.Request) {
	topics := lookupd.DB.FindRegistrations("topic", "*", "").Keys()
	data := make(map[string]interface{})
	data["topics"] = topics
	util.ApiResponse(w, 200, "OK", data)
}

func channelsHandler(w http.ResponseWriter, req *http.Request) {
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

	channels := lookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	data := make(map[string]interface{})
	data["channels"] = channels
	util.ApiResponse(w, 200, "OK", data)
}

func lookupHandler(w http.ResponseWriter, req *http.Request) {
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

	registration := lookupd.DB.FindRegistrations("topic", topicName, "")

	if len(registration) == 0 {
		util.ApiResponse(w, 500, "INVALID_ARG_TOPIC", nil)
		return
	}

	channels := lookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := lookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(lookupd.inactiveProducerTimeout, lookupd.tombstoneLifetime)
	data := make(map[string]interface{})
	data["channels"] = channels
	data["producers"] = producers.PeerInfo()

	util.ApiResponse(w, 200, "OK", data)
}

func createTopicHandler(w http.ResponseWriter, req *http.Request) {
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
	lookupd.DB.AddRegistration(key)

	util.ApiResponse(w, 200, "OK", nil)
}

func deleteTopicHandler(w http.ResponseWriter, req *http.Request) {
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

	registrations := lookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		log.Printf("DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		lookupd.DB.RemoveRegistration(*registration)
	}

	registrations = lookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		log.Printf("DB: removing topic(%s)", topicName)
		lookupd.DB.RemoveRegistration(*registration)
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func tombstoneTopicProducerHandler(w http.ResponseWriter, req *http.Request) {
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
	producers := lookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HttpPort)
		if thisNode == node {
			p.Tombstone()
		}
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func createChannelHandler(w http.ResponseWriter, req *http.Request) {
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
	lookupd.DB.AddRegistration(key)

	log.Printf("DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	lookupd.DB.AddRegistration(key)

	util.ApiResponse(w, 200, "OK", nil)
}

func deleteChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	registrations := lookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		util.ApiResponse(w, 404, "NOT_FOUND", nil)
		return
	}

	log.Printf("DB: removing channel(%s) from topic(%s)", channelName, topicName)
	for _, registration := range registrations {
		lookupd.DB.RemoveRegistration(*registration)
	}

	util.ApiResponse(w, 200, "OK", nil)
}

// note: we can't embed the *Producer here because embeded objects are ignored for json marshalling
type producerTopic struct {
	RemoteAddress    string   `json:"remote_address"`
	Address          string   `json:"address"` //TODO: drop for 1.0
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TcpPort          int      `json:"tcp_port"`
	HttpPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Topics           []string `json:"topics"`
}

func nodesHandler(w http.ResponseWriter, req *http.Request) {
	producers := lookupd.DB.FindProducers("client", "", "")
	producerTopics := make([]*producerTopic, len(producers))
	for i, p := range producers {
		producerTopics[i] = &producerTopic{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Address:          p.peerInfo.Address, //TODO: drop for 1.0
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TcpPort:          p.peerInfo.TcpPort,
			HttpPort:         p.peerInfo.HttpPort,
			Version:          p.peerInfo.Version,
			Topics:           lookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys(),
		}
	}

	data := make(map[string]interface{})
	data["producers"] = producerTopics
	util.ApiResponse(w, 200, "OK", data)
}

func infoHandler(w http.ResponseWriter, req *http.Request) {
	util.ApiResponse(w, 200, "OK", struct {
		Version string `json:"version"`
	}{
		Version: util.BINARY_VERSION,
	})
}

func debugHandler(w http.ResponseWriter, req *http.Request) {
	lookupd.DB.RLock()
	defer lookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range lookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		data[key] = make([]map[string]interface{}, 0)
		for _, p := range producers {
			m := make(map[string]interface{})
			m["id"] = p.peerInfo.id
			m["address"] = p.peerInfo.Address //TODO: remove for 1.0
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
