package main

import (
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
	handler.HandleFunc("/topics", TopicsHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)

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

func TopicsHandler(w http.ResponseWriter, req *http.Request) {
	topics := lookupd.DB.FindRegistrations("topic", "*", "").Keys()
	log.Printf("registrations topics %v", topics)
	data := make(map[string]interface{})
	data["topics"] = topics
	util.ApiResponse(w, 200, "OK", data)
}

func lookupHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Query("topic")
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
	producers := lookupd.DB.FindProducers("topic", topicName, "").CurrentProducers()
	data := make(map[string]interface{})
	data["channels"] = channels
	data["producers"] = producers
	util.ApiResponse(w, 200, "OK", data)
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

	log.Printf("Removing Topic:%s Channel:%s", topicName, channelName)
	for _, registration := range registrations {
		lookupd.DB.RemoveRegistration(*registration)
	}

	util.ApiResponse(w, 200, "OK", nil)
}

func shouldPreferLocal(conn net.Conn, addr string) bool {
	remoteAddr := conn.RemoteAddr().(*net.TCPAddr)
	addrHost, _, _ := net.SplitHostPort(addr)
	addrIP := net.ParseIP(addrHost)
	preferLocal := remoteAddr.IP.IsLoopback() && addrIP.IsLoopback()
	log.Printf("preferLocal: %v (%s and %s)", preferLocal, conn.RemoteAddr().String(), addr)
	return preferLocal
}
