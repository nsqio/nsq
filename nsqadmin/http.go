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
)

var templates *template.Template

func commafy(i interface{}) string {
	var n int64
	switch i.(type) {
	case int:
		n = int64(i.(int))
	case int64:
		n = i.(int64)
	case int32:
		n = int64(i.(int32))
	}
	if n > 1000 {
		r := n % 1000
		n = n / 1000
		return fmt.Sprintf("%s,%03d", commafy(n), r)
	}
	return fmt.Sprintf("%d", n)
}

func httpServer(listener net.Listener) {
	var err error
	t := template.New("nsqadmin").Funcs(template.FuncMap{
		"commafy": commafy,
	})
	templates, err = t.ParseGlob(fmt.Sprintf("%s/*.html", *templateDir))
	if err != nil {
		log.Printf("ERROR: %s", err.Error())
	}

	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/", indexHandler)
	handler.HandleFunc("/nodes", nodesHandler)
	handler.HandleFunc("/topic/", topicHandler)
	handler.HandleFunc("/delete_topic", deleteTopicHandler)
	handler.HandleFunc("/delete_channel", deleteChannelHandler)
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
	var topics []string
	if len(lookupdHTTPAddrs) != 0 {
		topics, _ = getLookupdTopics(lookupdHTTPAddrs)
	} else {
		topics, _ = getNSQDTopics(nsqdHTTPAddrs)
	}
	p := struct {
		Title   string
		Topics  []string
		Version string
	}{
		Title:   "NSQ",
		Topics:  topics,
		Version: util.BINARY_VERSION,
	}
	err := templates.ExecuteTemplate(w, "index.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func topicHandler(w http.ResponseWriter, req *http.Request) {
	var urlRegex = regexp.MustCompile(`^/topic/([\.a-zA-Z0-9_-]+)(/([\.-_a-zA-Z0-9]+(#ephemeral)?))?$`)
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

	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topic, lookupdHTTPAddrs)
	} else {
		producers, _ = getNsqdTopicProducers(topic, nsqdHTTPAddrs)
	}
	topicHostStats, channelStats, _ := getNSQDStats(producers, topic)

	globalTopicStats := &TopicHostStats{HostAddress: "Total"}
	for _, t := range topicHostStats {
		globalTopicStats.AddHostStats(t)
	}

	p := struct {
		Title            string
		Version          string
		Topic            string
		TopicProducers   []string
		TopicHostStats   []*TopicHostStats
		GlobalTopicStats *TopicHostStats
		ChannelStats     map[string]*ChannelStats
	}{
		Title:            fmt.Sprintf("NSQ %s", topic),
		Version:          util.BINARY_VERSION,
		Topic:            topic,
		TopicProducers:   producers,
		TopicHostStats:   topicHostStats,
		GlobalTopicStats: globalTopicStats,
		ChannelStats:     channelStats,
	}
	err := templates.ExecuteTemplate(w, "topic.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}

func channelHandler(w http.ResponseWriter, req *http.Request, topic string, channel string) {
	var producers []string
	if len(lookupdHTTPAddrs) != 0 {
		producers, _ = getLookupdTopicProducers(topic, lookupdHTTPAddrs)
	} else {
		producers, _ = getNsqdTopicProducers(topic, nsqdHTTPAddrs)
	}
	_, allChannelStats, _ := getNSQDStats(producers, topic)
	channelStats := allChannelStats[channel]

	p := struct {
		Title          string
		Version        string
		Topic          string
		Channel        string
		TopicProducers []string
		ChannelStats   *ChannelStats
	}{
		Title:          fmt.Sprintf("NSQ %s / %s", topic, channel),
		Version:        util.BINARY_VERSION,
		Topic:          topic,
		Channel:        channel,
		TopicProducers: producers,
		ChannelStats:   channelStats,
	}

	err := templates.ExecuteTemplate(w, "channel.html", p)
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

	topicName, err := reqParams.Query("topic")
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
		endpoint := fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("LOOKUPD: querying %s", endpoint)

		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
	}

	producers, _ := getLookupdTopicProducers(topicName, lookupdHTTPAddrs)
	for _, addr := range producers {
		endpoint := fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
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
		endpoint := fmt.Sprintf("http://%s/empty_channel?topic=%s&channel=%s", addr, url.QueryEscape(topicName), url.QueryEscape(channelName))
		log.Printf("NSQD: calling %s", endpoint)

		_, err := nsq.ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: nsqd %s - %s", endpoint, err.Error())
			continue
		}
	}

	http.Redirect(w, req, fmt.Sprintf("/topic/%s", url.QueryEscape(topicName)), 302)
}

func nodesHandler(w http.ResponseWriter, req *http.Request) {
	producers, _ := getLookupdProducers(lookupdHTTPAddrs)

	p := struct {
		Title     string
		Version   string
		Producers []*Producer
	}{
		Title:     "NSQD Hosts",
		Version:   util.BINARY_VERSION,
		Producers: producers,
	}
	err := templates.ExecuteTemplate(w, "nodes.html", p)
	if err != nil {
		log.Printf("Template Error %s", err.Error())
		http.Error(w, "Template Error", 500)
	}
}
