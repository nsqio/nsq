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
	"net/http/httputil"
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

// this is similar to httputil.NewSingleHostReverseProxy except it passes along basic auth
func NewSingleHostReverseProxy(target *url.URL, timeout time.Duration) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		if target.User != nil {
			passwd, _ := target.User.Password()
			req.SetBasicAuth(target.User.Username(), passwd)
		}
	}
	return &httputil.ReverseProxy{
		Director:  director,
		Transport: nsq.NewDeadlineTransport(timeout),
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
	handler.HandleFunc("/lookup", lookupHandler)
	handler.HandleFunc("/create_topic_channel", createTopicChannelHandler)
	if *proxyGraphite {
		url, err := url.Parse(*graphiteUrl)
		if err != nil {
			log.Printf("%s - %v", err.Error(), *graphiteUrl)
		} else {
			proxy := NewSingleHostReverseProxy(url, 20*time.Second)
			handler.Handle("/render", proxy)
		}
	}

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
	topicName := parts[0]
	if !nsq.IsValidTopicName(topicName) {
		http.Error(w, "INVALID_TOPIC", 500)
		return
	}
	if len(parts) == 2 {
		channelName := parts[1]
		if !nsq.IsValidChannelName(channelName) {
			http.Error(w, "INVALID_CHANNEL", 500)
		} else {
			channelHandler(w, req, topicName, channelName)
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
		producers, _ = getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	}
	topicHostStats, channelStats, _ := getNSQDStats(producers, topicName)

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
		Title:            fmt.Sprintf("NSQ %s", topicName),
		GraphOptions:     NewGraphOptions(w, req, reqParams),
		Version:          util.BINARY_VERSION,
		Topic:            topicName,
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

func channelHandler(w http.ResponseWriter, req *http.Request, topicName string, channelName string) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	}
	_, allChannelStats, _ := getNSQDStats(producers, topicName)
	channelStats := allChannelStats[channelName]

	p := struct {
		Title          string
		GraphOptions   *GraphOptions
		Version        string
		Topic          string
		Channel        string
		TopicProducers []string
		ChannelStats   *ChannelStats
	}{
		Title:          fmt.Sprintf("NSQ %s / %s", topicName, channelName),
		GraphOptions:   NewGraphOptions(w, req, reqParams),
		Version:        util.BINARY_VERSION,
		Topic:          topicName,
		Channel:        channelName,
		TopicProducers: producers,
		ChannelStats:   channelStats,
	}

	err = templates.ExecuteTemplate(w, "channel.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func lookupHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	channels := make(map[string][]string)
	allTopics, _ := getLookupdTopics(lookupdHTTPAddrs)
	for _, topicName := range allTopics {
		var producers []string
		producers, _ = getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
		if len(producers) == 0 {
			topicChannels, _ := getLookupdTopicChannels(topicName, lookupdHTTPAddrs)
			channels[topicName] = topicChannels
		}
	}

	p := struct {
		Title        string
		GraphOptions *GraphOptions
		TopicMap     map[string][]string
		Lookupd      []string
		Version      string
	}{
		Title:        "NSQ Lookup",
		GraphOptions: NewGraphOptions(w, req, reqParams),
		TopicMap:     channels,
		Lookupd:      lookupdHTTPAddrs,
		Version:      util.BINARY_VERSION,
	}
	err = templates.ExecuteTemplate(w, "lookup.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func createTopicChannelHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}

	topicName, err := reqParams.Get("topic")
	if err != nil || !nsq.IsValidTopicName(topicName) {
		http.Error(w, "INVALID_TOPIC", 500)
		return
	}

	channelName, err := reqParams.Get("channel")
	if err != nil || (len(channelName) > 0 && !nsq.IsValidChannelName(channelName)) {
		http.Error(w, "INVALID_CHANNEL", 500)
		return
	}

	for _, addr := range lookupdHTTPAddrs {
		endpoint := fmt.Sprintf("http://%s/create_topic?topic=%s", addr, url.QueryEscape(topicName))
		log.Printf("LOOKUPD: querying %s", endpoint)
		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
	}

	NotifyAdminAction("create_topic", topicName, "", req)

	if len(channelName) > 0 {
		for _, addr := range lookupdHTTPAddrs {
			endpoint := fmt.Sprintf("http://%s/create_channel?topic=%s&channel=%s",
				addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
			log.Printf("LOOKUPD: querying %s", endpoint)
			_, err := nsq.ApiRequest(endpoint)
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				continue
			}
		}

		// TODO: we can remove this when we push new channel information from nsqlookupd -> nsqd
		producers, _ := getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
		for _, addr := range producers {
			endpoint := fmt.Sprintf("http://%s/create_channel?topic=%s&channel=%s",
				addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
			log.Printf("NSQD: querying %s", endpoint)
			_, err := nsq.ApiRequest(endpoint)
			if err != nil {
				log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
				continue
			}
		}
		NotifyAdminAction("create_channel", topicName, channelName, req)
	}

	http.Redirect(w, req, "/lookup", 302)
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

	var rd string
	rd, err = reqParams.Get("rd")
	if err != nil {
		rd = "/"
	}

	// for topic removal, you need to get all the producers *first*
	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	}

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

	NotifyAdminAction("delete_topic", topicName, "", req)

	http.Redirect(w, req, rd, 302)
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

	var rd string
	rd, err = reqParams.Get("rd")
	if err != nil {
		rd = fmt.Sprintf("/topic/%s", url.QueryEscape(topicName))
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

	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	}

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

	NotifyAdminAction("delete_channel", topicName, channelName, req)

	http.Redirect(w, req, rd, 302)
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

	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	}

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

	NotifyAdminAction("empty_channel", topicName, channelName, req)

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

	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	} else {
		producers, _ = getNSQDTopicProducers(topicName, nsqdHTTPAddrs)
	}

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

	NotifyAdminAction(strings.TrimLeft(req.URL.Path, "/"), topicName, channelName, req)

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
		Title:        "NSQ Nodes",
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
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		http.Error(w, "INVALID_REQUEST", 500)
		return
	}
	p := struct {
		Title        string
		Version      string
		GraphOptions *GraphOptions
	}{
		Title:        "NSQ Message Counts",
		Version:      util.BINARY_VERSION,
		GraphOptions: NewGraphOptions(w, req, reqParams),
	}
	err = templates.ExecuteTemplate(w, "counter.html", p)
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
