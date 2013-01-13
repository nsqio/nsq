package main

import (
	"../nsq"
	"../util"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

func httpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/lookup", lookupHandler)
	handler.HandleFunc("/topics", topicsHandler)
	handler.HandleFunc("/channels", channelsHandler)
	handler.HandleFunc("/nodes", nodesHandler)
	handler.HandleFunc("/delete_topic", deleteTopicHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)
	handler.HandleFunc("/info", infoHandler)
	handler.HandleFunc("/create_topic", createTopicHandler)
	handler.HandleFunc("/create_channel", createChannelHandler)

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
	producers := lookupd.DB.FindProducers("topic", topicName, "").FilterByActive()
	data := make(map[string]interface{})
	data["channels"] = channels
	data["producers"] = producers

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
	Address  string   `json:"address"`
	TcpPort  int      `json:"tcp_port"`
	HttpPort int      `json:"http_port"`
	Version  string   `json:"version"`
	Topics   []string `json:"topics"`
}

func nodesHandler(w http.ResponseWriter, req *http.Request) {
	producers := lookupd.DB.FindProducers("client", "", "")
	producerTopics := make([]*producerTopic, len(producers))
	for i, p := range producers {
		producerTopics[i] = &producerTopic{
			Address:  p.Address,
			TcpPort:  p.TcpPort,
			HttpPort: p.HttpPort,
			Version:  p.Version,
			Topics:   lookupd.DB.LookupRegistrations(p).Filter("topic", "*", "").Keys(),
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
