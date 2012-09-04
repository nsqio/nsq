package main

import (
	"../util"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

func httpServer(listener net.Listener) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/lookup", lookupHandler)
	handler.HandleFunc("/topics", TopicsHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)

	server := &http.Server{
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
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
	lookupdb.RLock()
	defer lookupdb.RUnlock()

	topics := make([]string, 0)
	for topic, _ := range lookupdb.Topics {
		topics = append(topics, topic)
	}

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
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		util.ApiResponse(w, 500, "MISSING_ARG_TOPIC", nil)
		return
	}

	lookupdb.RLock()
	defer lookupdb.RUnlock()

	topic, ok := lookupdb.Topics[topicName]
	if !ok {
		util.ApiResponse(w, 500, "INVALID_ARG_TOPIC", nil)
		return
	}

	// hijack the request so we can access the connection directly (in order to get the local addr)
	hj, ok := w.(http.Hijacker)
	if !ok {
		util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		util.ApiResponse(w, 500, "INTERNAL_ERROR", nil)
		return
	}
	defer conn.Close()

	data := make(map[string]interface{})
	data["channels"] = topic.Channels

	// for each producer try to identify the optimal address based on the ones announced
	// to lookupd.  if none are optimal send the hostname (last entry)
	producers := make([]map[string]interface{}, 0)
	now := time.Now()
	for _, p := range topic.Producers {
		// TODO: make this a command line parameter
		if now.Sub(p.LastUpdate) > time.Duration(300)*time.Second {
			// it's a producer that has not checked in. Drop it
			continue
		}
		preferLocal := shouldPreferLocal(conn, p.ProducerId)
		chosenAddress, err := p.identifyBestAddress(preferLocal)
		if err != nil {
			w.Write([]byte(`{"status_code":500, "status_txt":"INTERNAL_ERROR", "data":null}`))
			return
		}

		producer := make(map[string]interface{})
		producer["address"] = chosenAddress
		producer["port"] = p.TCPPort // TODO: drop this field in a future rev (backwards compatible)
		producer["tcp_port"] = p.TCPPort
		producer["http_port"] = p.HTTPPort
		producers = append(producers, producer)
	}
	data["producers"] = producers

	util.ApiResponse(w, 200, "OK", data)
	output := make(map[string]interface{})
	output["data"] = data
	output["status_code"] = 200
	output["status_txt"] = "OK"
	response, err := json.Marshal(&output)
	if err != nil {
		response = []byte(`{"status_code":500, "status_txt":"INVALID_JSON", "data":null}`)
	}

	resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\nContent-Type: application/json; charset=utf-8\r\n\r\n%s", len(response), response)
	bufrw.WriteString(resp)
	bufrw.Flush()
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

	lookupdb.Lock()
	defer lookupdb.Unlock()

	topic, ok := lookupdb.Topics[topicName]
	if !ok {
		util.ApiResponse(w, 500, "INVALID_ARG_TOPIC", nil)
		return
	}

	log.Printf("Removing Topic:%s Channel:%s", topicName, channelName)
	delete(topic.Channels, channelName)
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
