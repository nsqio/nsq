package nsqadmin

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"reflect"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

func maybeWarnMsg(msgs []string) string {
	if len(msgs) > 0 {
		return "WARNING: " + strings.Join(msgs, "; ")
	}
	return ""
}

// this is similar to httputil.NewSingleHostReverseProxy except it passes along basic auth
func NewSingleHostReverseProxy(target *url.URL, connectTimeout time.Duration, requestTimeout time.Duration) *httputil.ReverseProxy {
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
		Transport: http_api.NewDeadlineTransport(connectTimeout, requestTimeout),
	}
}

type httpServer struct {
	ctx    *Context
	router http.Handler
	client *http_api.Client
	ci     *clusterinfo.ClusterInfo
}

func NewHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqadmin.getOpts().Logger)

	client := http_api.NewClient(ctx.nsqadmin.httpClientTLSConfig, ctx.nsqadmin.getOpts().HTTPClientConnectTimeout,
		ctx.nsqadmin.getOpts().HTTPClientRequestTimeout)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqadmin.getOpts().Logger)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqadmin.getOpts().Logger)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqadmin.getOpts().Logger)
	s := &httpServer{
		ctx:    ctx,
		router: router,
		client: client,
		ci:     clusterinfo.New(ctx.nsqadmin.getOpts().Logger, client),
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	router.Handle("GET", "/", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/topics", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/topics/:topic", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/topics/:topic/:channel", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/nodes", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/nodes/:node", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/counter", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/lookup", http_api.Decorate(s.indexHandler, log))

	router.Handle("GET", "/static/:asset", http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	router.Handle("GET", "/fonts/:asset", http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	if s.ctx.nsqadmin.getOpts().ProxyGraphite {
		proxy := NewSingleHostReverseProxy(ctx.nsqadmin.graphiteURL, ctx.nsqadmin.getOpts().HTTPClientConnectTimeout,
			ctx.nsqadmin.getOpts().HTTPClientRequestTimeout)
		router.Handler("GET", "/render", proxy)
	}

	// v1 endpoints
	router.Handle("GET", "/api/topics", http_api.Decorate(s.topicsHandler, log, http_api.V1))
	router.Handle("GET", "/api/topics/:topic", http_api.Decorate(s.topicHandler, log, http_api.V1))
	router.Handle("GET", "/api/topics/:topic/:channel", http_api.Decorate(s.channelHandler, log, http_api.V1))
	router.Handle("GET", "/api/nodes", http_api.Decorate(s.nodesHandler, log, http_api.V1))
	router.Handle("GET", "/api/nodes/:node", http_api.Decorate(s.nodeHandler, log, http_api.V1))
	router.Handle("POST", "/api/topics", http_api.Decorate(s.createTopicChannelHandler, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic", http_api.Decorate(s.topicActionHandler, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic/:channel", http_api.Decorate(s.channelActionHandler, log, http_api.V1))
	router.Handle("DELETE", "/api/nodes/:node", http_api.Decorate(s.tombstoneNodeForTopicHandler, log, http_api.V1))
	router.Handle("DELETE", "/api/topics/:topic", http_api.Decorate(s.deleteTopicHandler, log, http_api.V1))
	router.Handle("DELETE", "/api/topics/:topic/:channel", http_api.Decorate(s.deleteChannelHandler, log, http_api.V1))
	router.Handle("GET", "/api/counter", http_api.Decorate(s.counterHandler, log, http_api.V1))
	router.Handle("GET", "/api/graphite", http_api.Decorate(s.graphiteHandler, log, http_api.V1))
	router.Handle("GET", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))
	router.Handle("PUT", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) indexHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	asset, _ := Asset("index.html")
	t, _ := template.New("index").Parse(string(asset))

	w.Header().Set("Content-Type", "text/html")
	t.Execute(w, struct {
		Version             string
		ProxyGraphite       bool
		GraphEnabled        bool
		GraphiteURL         string
		StatsdInterval      int
		UseStatsdPrefixes   bool
		StatsdCounterFormat string
		StatsdGaugeFormat   string
		StatsdPrefix        string
		NSQLookupd          []string
	}{
		Version:             version.Binary,
		ProxyGraphite:       s.ctx.nsqadmin.getOpts().ProxyGraphite,
		GraphEnabled:        s.ctx.nsqadmin.getOpts().GraphiteURL != "",
		GraphiteURL:         s.ctx.nsqadmin.getOpts().GraphiteURL,
		StatsdInterval:      int(s.ctx.nsqadmin.getOpts().StatsdInterval / time.Second),
		UseStatsdPrefixes:   s.ctx.nsqadmin.getOpts().UseStatsdPrefixes,
		StatsdCounterFormat: s.ctx.nsqadmin.getOpts().StatsdCounterFormat,
		StatsdGaugeFormat:   s.ctx.nsqadmin.getOpts().StatsdGaugeFormat,
		StatsdPrefix:        s.ctx.nsqadmin.getOpts().StatsdPrefix,
		NSQLookupd:          s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
	})

	return nil, nil
}

func (s *httpServer) staticAssetHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	assetName := ps.ByName("asset")

	asset, err := Asset(assetName)
	if err != nil {
		return nil, http_api.Err{404, "NOT_FOUND"}
	}

	ext := path.Ext(assetName)
	ct := mime.TypeByExtension(ext)
	if ct == "" {
		switch ext {
		case ".svg":
			ct = "image/svg+xml"
		case ".woff":
			ct = "application/font-woff"
		case ".ttf":
			ct = "application/font-sfnt"
		case ".eot":
			ct = "application/vnd.ms-fontobject"
		case ".woff2":
			ct = "application/font-woff2"
		}
	}
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}

	return string(asset), nil
}

func (s *httpServer) topicsHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	var topics []string
	if len(s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses) != 0 {
		topics, err = s.ci.GetLookupdTopics(s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	} else {
		topics, err = s.ci.GetNSQDTopics(s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	}
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topics - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	inactive, _ := reqParams.Get("inactive")
	if inactive == "true" {
		topicChannelMap := make(map[string][]string)
		if len(s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses) == 0 {
			goto respond
		}
		for _, topicName := range topics {
			producers, _ := s.ci.GetLookupdTopicProducers(
				topicName, s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
			if len(producers) == 0 {
				topicChannels, _ := s.ci.GetLookupdTopicChannels(
					topicName, s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
				topicChannelMap[topicName] = topicChannels
			}
		}
	respond:
		return struct {
			Topics  map[string][]string `json:"topics"`
			Message string              `json:"message"`
		}{topicChannelMap, maybeWarnMsg(messages)}, nil
	}

	return struct {
		Topics  []string `json:"topics"`
		Message string   `json:"message"`
	}{topics, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) topicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")

	producers, err := s.ci.GetTopicProducers(topicName,
		s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	topicStats, _, err := s.ci.GetNSQDStats(producers, topicName)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topic metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	allNodesTopicStats := &clusterinfo.TopicStats{TopicName: topicName}
	for _, t := range topicStats {
		allNodesTopicStats.Add(t)
	}

	return struct {
		*clusterinfo.TopicStats
		Message string `json:"message"`
	}{allNodesTopicStats, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) channelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	producers, err := s.ci.GetTopicProducers(topicName,
		s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get topic producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	_, allChannelStats, err := s.ci.GetNSQDStats(producers, topicName)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get channel metadata - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		*clusterinfo.ChannelStats
		Message string `json:"message"`
	}{allChannelStats[channelName], maybeWarnMsg(messages)}, nil
}

func (s *httpServer) nodesHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	producers, err := s.ci.GetProducers(s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get nodes - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Nodes   clusterinfo.Producers `json:"nodes"`
		Message string                `json:"message"`
	}{producers, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) nodeHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	node := ps.ByName("node")

	producers, err := s.ci.GetProducers(s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get producers - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	producer := producers.Search(node)
	if producer == nil {
		return nil, http_api.Err{404, "NODE_NOT_FOUND"}
	}

	topicStats, _, err := s.ci.GetNSQDStats(clusterinfo.Producers{producer}, "")
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: failed to get nsqd stats - %s", err)
		return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
	}

	var totalClients int64
	var totalMessages int64
	for _, ts := range topicStats {
		for _, cs := range ts.Channels {
			totalClients += int64(len(cs.Clients))
		}
		totalMessages += ts.MessageCount
	}

	return struct {
		Node          string                    `json:"node"`
		TopicStats    []*clusterinfo.TopicStats `json:"topics"`
		TotalMessages int64                     `json:"total_messages"`
		TotalClients  int64                     `json:"total_clients"`
		Message       string                    `json:"message"`
	}{
		Node:          node,
		TopicStats:    topicStats,
		TotalMessages: totalMessages,
		TotalClients:  totalClients,
		Message:       maybeWarnMsg(messages),
	}, nil
}

func (s *httpServer) tombstoneNodeForTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	node := ps.ByName("node")

	var body struct {
		Topic string `json:"topic"`
	}
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_BODY"}
	}

	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	err = s.ci.TombstoneNodeForTopic(body.Topic, node,
		s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to tombstone node for topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("tombstone_topic_producer", body.Topic, "", node, req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) createTopicChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	var body struct {
		Topic   string `json:"topic"`
		Channel string `json:"channel"`
	}
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	if !protocol.IsValidTopicName(body.Topic) {
		return nil, http_api.Err{400, "INVALID_TOPIC"}
	}

	if len(body.Channel) > 0 && !protocol.IsValidChannelName(body.Channel) {
		return nil, http_api.Err{400, "INVALID_CHANNEL"}
	}

	err = s.ci.CreateTopicChannel(body.Topic, body.Channel,
		s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to create topic/channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("create_topic", body.Topic, "", "", req)
	if len(body.Channel) > 0 {
		s.notifyAdminAction("create_channel", body.Topic, body.Channel, "", req)
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")

	err := s.ci.DeleteTopic(topicName,
		s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to delete topic - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("delete_topic", topicName, "", "", req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string

	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	err := s.ci.DeleteChannel(topicName, channelName,
		s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
		s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to delete channel - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	s.notifyAdminAction("delete_channel", topicName, channelName, "", req)

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

func (s *httpServer) topicActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	return s.topicChannelAction(req, topicName, "")
}

func (s *httpServer) channelActionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")
	return s.topicChannelAction(req, topicName, channelName)
}

func (s *httpServer) topicChannelAction(req *http.Request, topicName string, channelName string) (interface{}, error) {
	var messages []string

	var body struct {
		Action string `json:"action"`
	}
	err := json.NewDecoder(req.Body).Decode(&body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	switch body.Action {
	case "pause":
		if channelName != "" {
			err = s.ci.PauseChannel(topicName, channelName,
				s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("pause_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.PauseTopic(topicName,
				s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("pause_topic", topicName, "", "", req)
		}
	case "unpause":
		if channelName != "" {
			err = s.ci.UnPauseChannel(topicName, channelName,
				s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("unpause_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.UnPauseTopic(topicName,
				s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("unpause_topic", topicName, "", "", req)
		}
	case "empty":
		if channelName != "" {
			err = s.ci.EmptyChannel(topicName, channelName,
				s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("empty_channel", topicName, channelName, "", req)
		} else {
			err = s.ci.EmptyTopic(topicName,
				s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses,
				s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)

			s.notifyAdminAction("empty_topic", topicName, "", "", req)
		}
	default:
		return nil, http_api.Err{400, "INVALID_ACTION"}
	}

	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to %s topic/channel - %s", body.Action, err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	return struct {
		Message string `json:"message"`
	}{maybeWarnMsg(messages)}, nil
}

type counterStats struct {
	Node         string `json:"node"`
	TopicName    string `json:"topic_name"`
	ChannelName  string `json:"channel_name"`
	MessageCount int64  `json:"message_count"`
}

func (s *httpServer) counterHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var messages []string
	stats := make(map[string]*counterStats)

	producers, err := s.ci.GetProducers(s.ctx.nsqadmin.getOpts().NSQLookupdHTTPAddresses, s.ctx.nsqadmin.getOpts().NSQDHTTPAddresses)
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get counter producer list - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}
	_, channelStats, err := s.ci.GetNSQDStats(producers, "")
	if err != nil {
		pe, ok := err.(clusterinfo.PartialErr)
		if !ok {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqd stats - %s", err)
			return nil, http_api.Err{502, fmt.Sprintf("UPSTREAM_ERROR: %s", err)}
		}
		s.ctx.nsqadmin.logf("WARNING: %s", err)
		messages = append(messages, pe.Error())
	}

	for _, channelStats := range channelStats {
		for _, hostChannelStats := range channelStats.NodeStats {
			key := fmt.Sprintf("%s:%s:%s", channelStats.TopicName, channelStats.ChannelName, hostChannelStats.Node)
			s, ok := stats[key]
			if !ok {
				s = &counterStats{
					Node:        hostChannelStats.Node,
					TopicName:   channelStats.TopicName,
					ChannelName: channelStats.ChannelName,
				}
				stats[key] = s
			}
			s.MessageCount += hostChannelStats.MessageCount
		}
	}

	return struct {
		Stats   map[string]*counterStats `json:"stats"`
		Message string                   `json:"message"`
	}{stats, maybeWarnMsg(messages)}, nil
}

func (s *httpServer) graphiteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	metric, err := reqParams.Get("metric")
	if err != nil || metric != "rate" {
		return nil, http_api.Err{400, "INVALID_ARG_METRIC"}
	}

	target, err := reqParams.Get("target")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TARGET"}
	}

	params := url.Values{}
	params.Set("from", fmt.Sprintf("-%dsec", s.ctx.nsqadmin.getOpts().StatsdInterval*2/time.Second))
	params.Set("until", fmt.Sprintf("-%dsec", s.ctx.nsqadmin.getOpts().StatsdInterval/time.Second))
	params.Set("format", "json")
	params.Set("target", target)
	query := fmt.Sprintf("/render?%s", params.Encode())
	url := s.ctx.nsqadmin.getOpts().GraphiteURL + query

	s.ctx.nsqadmin.logf("GRAPHITE: %s", url)

	var response []struct {
		Target     string       `json:"target"`
		DataPoints [][]*float64 `json:"datapoints"`
	}
	err = s.client.GETV1(url, &response)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: graphite request failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	var rateStr string
	rate := *response[0].DataPoints[0][0]
	if rate < 0 {
		rateStr = "N/A"
	} else {
		rateDivisor := s.ctx.nsqadmin.getOpts().StatsdInterval / time.Second
		rateStr = fmt.Sprintf("%.2f", rate/float64(rateDivisor))
	}
	return struct {
		Rate string `json:"rate"`
	}{rateStr}, nil
}

func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	opt := ps.ByName("opt")

	if req.Method == "PUT" {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := int64(1024*1024 + 1)
		body, err := ioutil.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			return nil, http_api.Err{500, "INTERNAL_ERROR"}
		}
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{413, "INVALID_VALUE"}
		}

		opts := *s.ctx.nsqadmin.getOpts()
		switch opt {
		case "nsqlookupd_http_addresses":
			err := json.Unmarshal(body, &opts.NSQLookupdHTTPAddresses)
			if err != nil {
				return nil, http_api.Err{400, "INVALID_VALUE"}
			}
		default:
			return nil, http_api.Err{400, "INVALID_OPTION"}
		}
		s.ctx.nsqadmin.swapOpts(&opts)
	}

	v, ok := getOptByCfgName(s.ctx.nsqadmin.getOpts(), opt)
	if !ok {
		return nil, http_api.Err{400, "INVALID_OPTION"}
	}

	return v, nil
}

func getOptByCfgName(opts interface{}, name string) (interface{}, bool) {
	val := reflect.ValueOf(opts).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		flagName := field.Tag.Get("flag")
		cfgName := field.Tag.Get("cfg")
		if flagName == "" {
			continue
		}
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}
		if name != cfgName {
			continue
		}
		return val.FieldByName(field.Name).Interface(), true
	}
	return nil, false
}
