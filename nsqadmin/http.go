package main

import (
	"../nsq"
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

func loadTemplates() {
	var err error
	t := template.New("nsqadmin").Funcs(template.FuncMap{
		"commafy": util.Commafy,
	})
	templates, err = t.ParseGlob(fmt.Sprintf("%s/*.html", *templateDir))
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
	}
}

func httpServer(listener net.Listener) {
	loadTemplates()
	log.Printf("HTTP: listening on %s", listener.Addr().String())
	globalCounters = make(map[string]counterData)
	handler := http.NewServeMux()
	handler.HandleFunc("/favicon.ico", faviconHandler)
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/", indexHandler)
	handler.HandleFunc("/nodes", nodesHandler)
	handler.HandleFunc("/topic/", topicHandler)
	handler.HandleFunc("/delete_topic", deleteTopicHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)
	handler.HandleFunc("/empty_channel", emptyChannelHandler)
	handler.HandleFunc("/pause_channel", pauseChannelHandler)
	handler.HandleFunc("/unpause_channel", pauseChannelHandler)
	handler.HandleFunc("/counter/data", counterDataHandler)
	handler.HandleFunc("/counter", counterHandler)

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

func indexHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	var topics []string
	if len(lookupdHTTPAddrs) != 0 {
		topics, _ = getLookupdTopics(lookupdHTTPAddrs)
	} else {
		topics, _ = getNSQDTopics(nsqdHTTPAddrs)
	}

	p := struct {
		Title        string
		GraphOptions *GraphOptions
		Topics       Topics
		Version      string
	}{
		Title:        "NSQ",
		GraphOptions: NewGraphOptions(w, req, reqParams),
		Topics:       TopicsForStrings(topics),
		Version:      util.BINARY_VERSION,
	}
	err = templates.ExecuteTemplate(w, "index.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func topicHandler(w http.ResponseWriter, req *http.Request) {
	var urlRegex = regexp.MustCompile(`^/topic/(.*)$`)
	matches := urlRegex.FindStringSubmatch(req.URL.Path)
	if len(matches) == 0 {
		http.Error(w, "INVALID_TOPIC", 500)
		return
	}
	parts := strings.Split(matches[1], "/")
	topic := parts[0]
	if !nsq.IsValidTopicName(topic) {
		http.Error(w, "INVALID_TOPIC", 500)
		return
	}
	if len(parts) == 2 {
		channel := parts[1]
		if !nsq.IsValidChannelName(channel) {
			http.Error(w, "INVALID_CHANNEL", 500)
		} else {
			channelHandler(w, req, topic, channel)
		}
		return
	}

	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topic, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topic, nsqdHTTPAddrs)
	}
	topicHostStats, channelStats, _ := getNSQDStats(producers, topic)

	globalTopicStats := &TopicHostStats{HostAddress: "Total"}
	for _, t := range topicHostStats {
		globalTopicStats.AddHostStats(t)
	}

	p := struct {
		Title            string
		GraphOptions     *GraphOptions
		Version          string
		Topic            string
		TopicProducers   []string
		TopicHostStats   []*TopicHostStats
		GlobalTopicStats *TopicHostStats
		ChannelStats     map[string]*ChannelStats
	}{
		Title:            fmt.Sprintf("NSQ %s", topic),
		GraphOptions:     NewGraphOptions(w, req, reqParams),
		Version:          util.BINARY_VERSION,
		Topic:            topic,
		TopicProducers:   producers,
		TopicHostStats:   topicHostStats,
		GlobalTopicStats: globalTopicStats,
		ChannelStats:     channelStats,
	}
	err = templates.ExecuteTemplate(w, "topic.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func channelHandler(w http.ResponseWriter, req *http.Request, topic string, channel string) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topic, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topic, nsqdHTTPAddrs)
	}
	_, allChannelStats, _ := getNSQDStats(producers, topic)
	channelStats := allChannelStats[channel]

	p := struct {
		Title          string
		GraphOptions   *GraphOptions
		Version        string
		Topic          string
		Channel        string
		TopicProducers []string
		ChannelStats   *ChannelStats
	}{
		Title:          fmt.Sprintf("NSQ %s / %s", topic, channel),
		GraphOptions:   NewGraphOptions(w, req, reqParams),
		Version:        util.BINARY_VERSION,
		Topic:          topic,
		Channel:        channel,
		TopicProducers: producers,
		ChannelStats:   channelStats,
	}

	err = templates.ExecuteTemplate(w, "channel.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func deleteTopicHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		http.Error(w, "MISSING_ARG_TOPIC", 500)
		return
	}

	// for topic removal, you need to get all the producers *first*
	producers, _ := getLookupdTopicProducers(topicName, lookupdHTTPAddrs)

	// remove the topic from all the lookupds
	for _, addr := range lookupdHTTPAddrs {
		endpoint := fmt.Sprintf("http://%s/delete_topic?topic=%s", addr, url.QueryEscape(topicName))
		log.Printf("LOOKUPD: querying %s", endpoint)

		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
	}

	// now remove the topic from all the producers
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s/delete_topic?topic=%s", addr, url.QueryEscape(topicName))
		log.Printf("NSQD: querying %s", endpoint)
		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
			continue
		}
	}

	http.Redirect(w, req, "/", 302)
}

func deleteChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	for _, addr := range lookupdHTTPAddrs {
		endpoint := fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s",
			addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("LOOKUPD: querying %s", endpoint)

		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
	}

	producers, _ := getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s",
			addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("NSQD: querying %s", endpoint)
		_, err := nsq.ApiRequest(endpoint)
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

	producers, _ := getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s/empty_channel?topic=%s&channel=%s",
			addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("NSQD: calling %s", endpoint)

		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
			continue
		}
	}

	http.Redirect(w, req, fmt.Sprintf("/topic/%s", url.QueryEscape(topicName)), 302)
}

func pauseChannelHandler(w http.ResponseWriter, req *http.Request) {
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

	producers, _ := getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s%s?topic=%s&channel=%s",
			addr, req.URL.Path, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("NSQD: calling %s", endpoint)

		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
			continue
		}
	}

	http.Redirect(w, req, fmt.Sprintf("/topic/%s/%s", url.QueryEscape(topicName), url.QueryEscape(channelName)), 302)
}

func nodesHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}
	producers, _ := getLookupdProducers(lookupdHTTPAddrs)

	p := struct {
		Title        string
		Version      string
		GraphOptions *GraphOptions
		Producers    []*Producer
	}{
		Title:        "NSQD Hosts",
		Version:      util.BINARY_VERSION,
		GraphOptions: NewGraphOptions(w, req, reqParams),
		Producers:    producers,
	}
	err = templates.ExecuteTemplate(w, "nodes.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func counterHandler(w http.ResponseWriter, req *http.Request) {
	p := struct {
		Title   string
		Version string
	}{
		Title:   fmt.Sprintf("NSQ Message Counts"),
		Version: util.BINARY_VERSION,
	}
	err := templates.ExecuteTemplate(w, "counter.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

type counterData map[string]int64

var globalCounters map[string]counterData

// this endpoint works by giving out an ID that maps to a stats dictionary
// The initial request is the number of messages processed since each nsqd started up.
// Subsequent requsts pass that ID and get an updated delta based on each individual channel/nsqd message count
// That ID must be re-requested or it will be expired.
func counterDataHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	statsID, _ := reqParams.Get("id")
	now := time.Now()
	if statsID == "" {
		// make a new one
		statsID = fmt.Sprintf("%d.%d", now.Unix(), now.UnixNano())
	}

	stats, ok := globalCounters[statsID]
	if !ok {
		stats = make(map[string]int64)
	}
	newStats := make(map[string]int64)
	newStats["time"] = now.Unix()

	producers, _ := getLookupdProducers(lookupdHTTPAddrs)
	addresses := make([]string, len(producers))
	for i, p := range producers {
		addresses[i] = p.HTTPAddress()
	}
	_, channelStats, _ := getNSQDStats(addresses, "")

	var newMessages int64
	var totalMessages int64
	for _, channel := range channelStats {
		for _, channelHostStats := range channel.HostStats {
			key := fmt.Sprintf("%s:%s:%s", channel.Topic, channel.ChannelName, channelHostStats.HostAddress)
			d, ok := stats[key]
			if ok && d <= channelHostStats.MessageCount {
				newMessages += (channelHostStats.MessageCount - d)
			}
			totalMessages += channelHostStats.MessageCount
			newStats[key] = channelHostStats.MessageCount
		}
	}
	globalCounters[statsID] = newStats

	data := make(map[string]interface{})
	data["new_messages"] = newMessages
	data["total_messages"] = totalMessages
	data["id"] = statsID
	util.ApiResponse(w, 200, "OK", data)
}

func faviconHandler(w http.ResponseWriter, req *http.Request) {
	http.NotFound(w, req)
}
