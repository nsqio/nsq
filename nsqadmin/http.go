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
	"sort"
	"strings"
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
		Handler: handler,
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
	sort.Strings(topics)
	p := struct {
		Title   string
		Topics  []string
		Version string
	}{
		Title:   "",
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
	if len(matches) >= 4 && len(matches[3]) > 0 {
		channel := matches[3]
		channelHandler(w, req, topic, channel)
		return
	}

	producers, _ := getLookupdTopicProducers(topic, lookupdAddresses)
	topicHostStats, channelStats, _ := getNSQDStats(producers, topic)

	p := struct {
		Title          string
		Version        string
		Topic          string
		TopicProducers []string
		TopicHostStats []TopicHostStats
		ChannelStats   map[string]*ChannelStats
	}{
		Title:          fmt.Sprintf("NSQ %s", topic),
		Version:        VERSION,
		Topic:          topic,
		TopicProducers: producers,
		TopicHostStats: topicHostStats,
		ChannelStats:   channelStats,
	}
	err := templates.ExecuteTemplate(w, "topic.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func channelHandler(w http.ResponseWriter, req *http.Request, topic string, channel string) {
	producers, _ := getLookupdTopicProducers(topic, lookupdAddresses)
	topicHostStats, allChannelStats, _ := getNSQDStats(producers, topic)
	channelStats := allChannelStats[channel]

	p := struct {
		Title          string
		Version        string
		Topic          string
		Channel        string
		TopicProducers []string
		TopicHostStats []TopicHostStats
		ChannelStats   *ChannelStats
	}{
		Title:          fmt.Sprintf("NSQ %s / %s", topic, channel),
		Version:        VERSION,
		Topic:          topic,
		Channel:        channel,
		TopicProducers: producers,
		TopicHostStats: topicHostStats,
		ChannelStats:   channelStats,
	}

	err := templates.ExecuteTemplate(w, "channel.html", p)
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

		_, err := util.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
	}

	producers, _ := getLookupdTopicProducers(topicName, lookupdAddresses)
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("NSQD: calling %s", endpoint)
		_, err := util.ApiRequest(endpoint)
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

		_, err := util.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
			continue
		}
	}

	http.Redirect(w, req, fmt.Sprintf("/topic/%s", url.QueryEscape(topicName)), 302)

}
