package main

import (
	"../util"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var templates *template.Template

func httpServer(listener net.Listener) {
	var err error
	templates, err = template.ParseGlob(fmt.Sprintf("%s/*.html", *templateDir))
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
	}

	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/", indexHandler)
	handler.HandleFunc("/topic/", topicHandler)
	handler.HandleFunc("/delete_channel", removeChannelHandler)
	handler.HandleFunc("/empty_channel", emptyChannelHandler)

	server := &http.Server{
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	err = server.Serve(listener)
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

func indexHandler(w http.ResponseWriter, req *http.Request) {
	topics, _ := getLookupdTopics(lookupdAddresses)
	p := struct {
		Topics  []string
		Version string
	}{
		Topics:  topics,
		Version: VERSION,
	}
	err := templates.ExecuteTemplate(w, "index.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func topicHandler(w http.ResponseWriter, req *http.Request) {
	var urlRegex = regexp.MustCompile(`^/topic/([a-zA-Z0-9_-]+)(/([-_a-zA-Z0-9]+(#ephemeral)?))?$`)
	matches := urlRegex.FindStringSubmatch(req.URL.Path)
	if len(matches) == 0 {
		http.NotFound(w, req)
		return
	}
	topic := matches[1]
	var selectedChannel string
	if len(matches) >= 4 {
		selectedChannel = matches[3]
	}

	log.Printf("Topic:%s Channel:%s", topic, selectedChannel)

	producers, _ := getLookupdTopicProducers(topic, lookupdAddresses)
	hostStats, channelStats, _ := getNSQDStats(producers, topic)
	var selectedChannelInfo ChannelStats
	var ok bool
	if len(selectedChannel) != 0 {
		selectedChannelInfo, ok = channelStats[selectedChannel]
		if !ok {
			http.NotFound(w, req)
			return
		}
		selectedChannelInfo.Selected = true
		channelStats[selectedChannel] = selectedChannelInfo
	}

	p := struct {
		Version             string
		Topic               string
		SelectedChannel     string
		TopicProducers      []string
		HostStats           []HostStats
		ChannelStats        map[string]ChannelStats
		SelectedChannelInfo ChannelStats
	}{
		Version:             VERSION,
		Topic:               topic,
		SelectedChannel:     selectedChannel,
		TopicProducers:      producers,
		HostStats:           hostStats,
		ChannelStats:        channelStats,
		SelectedChannelInfo: selectedChannelInfo,
	}
	err := templates.ExecuteTemplate(w, "topic.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func removeChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	for _, addr := range lookupdAddresses {
		endpoint := fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("LOOKUPD: querying %s", endpoint)

		_, err := makeReq(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
	}

	producers, _ := getLookupdTopicProducers(topicName, lookupdAddresses)
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("NSQD: calling %s", endpoint)
		_, err := makeReq(endpoint)
		if err != nil {
			log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
			continue
		}
	}

	http.Redirect(w, req, fmt.Sprintf("/topic/%s", url.QueryEscape(topicName)), 302)

}

func emptyChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	topicName, channelName, err := util.GetTopicChannelArgs(reqParams)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	producers, _ := getLookupdTopicProducers(topicName, lookupdAddresses)
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s/empty_channel?topic=%s&channel=%s", addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("NSQD: calling %s", endpoint)

		_, err := makeReq(endpoint)
		if err != nil {
			log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
			continue
		}
	}

	http.Redirect(w, req, fmt.Sprintf("/topic/%s", url.QueryEscape(topicName)), 302)

}
