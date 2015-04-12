package nsqadmin

import (
	"errors"
	"fmt"
	"html/template"
	"io/ioutil"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/blang/semver"
	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

var v1EndpointVersion semver.Version

func init() {
	v1EndpointVersion, _ = semver.Parse("0.2.29-alpha")
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
		Transport: http_api.NewDeadlineTransport(timeout),
	}
}

type httpServer struct {
	ctx    *Context
	router http.Handler
	client *http_api.Client
	ci     *clusterinfo.ClusterInfo
}

func NewHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqadmin.opts.Logger)

	client := http_api.NewClient(ctx.nsqadmin.httpClientTLSConfig)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqadmin.opts.Logger)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqadmin.opts.Logger)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqadmin.opts.Logger)
	s := &httpServer{
		ctx:    ctx,
		router: router,
		client: client,
		ci:     clusterinfo.New(ctx.nsqadmin.opts.Logger, client),
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
	if s.ctx.nsqadmin.opts.ProxyGraphite {
		proxy := NewSingleHostReverseProxy(ctx.nsqadmin.graphiteURL, 20*time.Second)
		router.Handler("GET", "/render", proxy)
	}

	// v1 endpoints
	router.Handle("GET", "/api/topics", http_api.Decorate(s.doTopics, log, http_api.V1))
	router.Handle("GET", "/api/topics/:topic", http_api.Decorate(s.doTopic, log, http_api.V1))
	router.Handle("GET", "/api/topics/:topic/:channel", http_api.Decorate(s.doChannel, log, http_api.V1))
	router.Handle("GET", "/api/nodes", http_api.Decorate(s.doNodes, log, http_api.V1))
	router.Handle("GET", "/api/nodes/:node", http_api.Decorate(s.doNode, log, http_api.V1))
	router.Handle("POST", "/api/topics", http_api.Decorate(s.doCreateTopicChannel, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic", http_api.Decorate(s.doTopicAction, log, http_api.V1))
	router.Handle("POST", "/api/topics/:topic/:channel", http_api.Decorate(s.doChannelAction, log, http_api.V1))
	router.Handle("DELETE", "/api/nodes/:node", http_api.Decorate(s.doTombstoneTopicNode, log, http_api.V1))
	router.Handle("DELETE", "/api/topics/:topic", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("DELETE", "/api/topics/:topic/:channel", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("GET", "/api/counter", http_api.Decorate(s.counterHandler, log, http_api.V1))
	router.Handle("GET", "/api/graphite", http_api.Decorate(s.graphiteHandler, log, http_api.V1))

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
		Version        string
		ProxyGraphite  bool
		GraphEnabled   bool
		GraphiteURL    string
		StatsdInterval int
		StatsdPrefix   string
		NSQLookupd     []string
	}{
		Version:        version.Binary,
		ProxyGraphite:  s.ctx.nsqadmin.opts.ProxyGraphite,
		GraphEnabled:   s.ctx.nsqadmin.opts.GraphiteURL != "",
		GraphiteURL:    s.ctx.nsqadmin.opts.GraphiteURL,
		StatsdInterval: int(s.ctx.nsqadmin.opts.StatsdInterval / time.Second),
		StatsdPrefix:   s.ctx.nsqadmin.opts.StatsdPrefix,
		NSQLookupd:     s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
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

func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	var topics []string
	if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses) != 0 {
		topics, _ = s.ci.GetLookupdTopics(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	} else {
		topics, _ = s.ci.GetNSQDTopics(s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	}

	inactive, _ := reqParams.Get("inactive")
	if inactive == "true" {
		topicChannelMap := make(map[string][]string)
		if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses) == 0 {
			goto respond
		}
		for _, topicName := range topics {
			producerList, _ := s.ci.GetLookupdTopicProducers(
				topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
			if len(producerList) == 0 {
				topicChannels, _ := s.ci.GetLookupdTopicChannels(
					topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
				topicChannelMap[topicName] = topicChannels
			}
		}
	respond:
		return struct {
			Topics map[string][]string `json:"topics"`
		}{topicChannelMap}, nil
	}

	return struct {
		Topics []string `json:"topics"`
	}{topics}, nil
}

func (s *httpServer) doTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")

	producerList := s.getTopicProducerList(topicName)
	topicStats, _, err := s.ci.GetNSQDStats(producerList, topicName)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	allNodesTopicStats := &clusterinfo.TopicStats{TopicName: topicName}
	for _, t := range topicStats {
		allNodesTopicStats.Add(t)
	}

	return allNodesTopicStats, nil
}

func (s *httpServer) doChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	producerList := s.getTopicProducerList(topicName)
	_, allChannelStats, _ := s.ci.GetNSQDStats(producerList, topicName)
	channelStats := allChannelStats[channelName]

	return channelStats, nil
}

func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	producerList := s.getProducerList()
	return struct {
		Nodes clusterinfo.ProducerList `json:"nodes"`
	}{producerList}, nil
}

func (s *httpServer) doTombstoneTopicNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	node := ps.ByName("node")

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	topicName := js.Get("topic").MustString()

	err = s.tombstoneTopicNode(req, topicName, node)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	node := ps.ByName("node")

	producerList := s.getProducerList()
	producer := producerList.Search(node)
	if producer == nil {
		return nil, http_api.Err{404, "NODE_NOT_FOUND"}
	}

	topicStats, _, _ := s.ci.GetNSQDStats(clusterinfo.ProducerList{producer}, "")

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
	}{
		Node:          node,
		TopicStats:    topicStats,
		TotalMessages: totalMessages,
		TotalClients:  totalClients,
	}, nil
}

func (s *httpServer) doCreateTopicChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	topicName := js.Get("topic").MustString()
	channelName := js.Get("channel").MustString()

	err = s.createTopicChannel(req, topicName, channelName)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) createTopicChannel(req *http.Request, topicName string, channelName string) error {
	if !protocol.IsValidTopicName(topicName) {
		return errors.New("INVALID_TOPIC")
	}

	if len(channelName) > 0 && !protocol.IsValidChannelName(channelName) {
		return errors.New("INVALID_CHANNEL")
	}

	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := s.ci.GetVersion(addr)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqlookupd %s version - %s", addr, err)
		}

		uri := "create_topic"
		if !nsqlookupdVersion.LT(v1EndpointVersion) {
			uri = "topic/create"
		}

		endpoint := fmt.Sprintf("http://%s/%s?topic=%s", addr,
			uri, url.QueryEscape(topicName))
		s.ctx.nsqadmin.logf("LOOKUPD: querying %s", endpoint)
		err = s.client.POSTV1(endpoint)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
			continue
		}

		if len(channelName) > 0 {
			uri := "create_channel"
			if !nsqlookupdVersion.LT(v1EndpointVersion) {
				uri = "channel/create"
			}
			endpoint := fmt.Sprintf("http://%s/%s?topic=%s&channel=%s",
				addr, uri,
				url.QueryEscape(topicName),
				url.QueryEscape(channelName))
			s.ctx.nsqadmin.logf("LOOKUPD: querying %s", endpoint)
			err := s.client.POSTV1(endpoint)
			if err != nil {
				s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
				continue
			}
		}
	}

	s.notifyAdminAction("create_topic", topicName, "", "", req)

	if len(channelName) > 0 {
		// TODO: we can remove this when we push new channel information from nsqlookupd -> nsqd
		producerList, _ := s.ci.GetLookupdTopicProducers(topicName,
			s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)

		s.performVersionNegotiatedRequestsToNSQD(
			s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
			producerList.HTTPAddrs(),
			"create_channel",
			"channel/create",
			fmt.Sprintf("topic=%s&channel=%s",
				url.QueryEscape(topicName), url.QueryEscape(channelName)))

		s.notifyAdminAction("create_channel", topicName, channelName, "", req)
	}

	return nil
}

func (s *httpServer) tombstoneTopicNode(req *http.Request, topicName string, node string) error {
	if !protocol.IsValidTopicName(topicName) {
		return errors.New("INVALID_TOPIC")
	}

	if node == "" {
		return errors.New("INVALID_NODE")
	}

	// tombstone the topic on all the lookupds
	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := s.ci.GetVersion(addr)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqlookupd %s version - %s", addr, err)
		}

		uri := "tombstone_topic_producer"
		if !nsqlookupdVersion.LT(v1EndpointVersion) {
			uri = "topic/tombstone"
		}

		endpoint := fmt.Sprintf("http://%s/%s?topic=%s&node=%s",
			addr, uri,
			url.QueryEscape(topicName), url.QueryEscape(node))
		s.ctx.nsqadmin.logf("LOOKUPD: querying %s", endpoint)
		err = s.client.POSTV1(endpoint)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
		}
	}

	nsqdVersion, err := s.ci.GetVersion(node)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: failed to get nsqd %s version - %s", node, err)
	}

	uri := "delete_topic"
	if !nsqdVersion.LT(v1EndpointVersion) {
		uri = "topic/delete"
	}

	// delete the topic on the producer
	endpoint := fmt.Sprintf("http://%s/%s?topic=%s", node,
		uri, url.QueryEscape(topicName))
	s.ctx.nsqadmin.logf("NSQD: querying %s", endpoint)
	err = s.client.POSTV1(endpoint)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: nsqd %s - %s", endpoint, err)
	}

	s.notifyAdminAction("tombstone_topic_producer", topicName, "", node, req)

	return nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	err := s.deleteTopic(req, topicName)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) deleteTopic(req *http.Request, topicName string) error {
	// for topic removal, you need to get all the producers *first*
	producerList := s.getTopicProducerList(topicName)

	// remove the topic from all the lookupds
	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := s.ci.GetVersion(addr)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqlookupd %s version - %s", addr, err)
		}

		uri := "delete_topic"
		if !nsqlookupdVersion.LT(v1EndpointVersion) {
			uri = "topic/delete"
		}

		endpoint := fmt.Sprintf("http://%s/%s?topic=%s", addr, uri, topicName)
		s.ctx.nsqadmin.logf("LOOKUPD: querying %s", endpoint)
		err = s.client.POSTV1(endpoint)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
			continue
		}
	}

	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerList.HTTPAddrs(),
		"delete_topic",
		"topic/delete",
		fmt.Sprintf("topic=%s", url.QueryEscape(topicName)))

	s.notifyAdminAction("delete_topic", topicName, "", "", req)

	return nil
}

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")
	err := s.deleteChannel(req, topicName, channelName)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) deleteChannel(req *http.Request, topicName string, channelName string) error {
	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := s.ci.GetVersion(addr)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqlookupd %s version - %s", addr, err)
		}

		uri := "delete_channel"
		if !nsqlookupdVersion.LT(v1EndpointVersion) {
			uri = "channel/delete"
		}

		endpoint := fmt.Sprintf("http://%s/%s?topic=%s&channel=%s",
			addr, uri,
			url.QueryEscape(topicName),
			url.QueryEscape(channelName))
		s.ctx.nsqadmin.logf("LOOKUPD: querying %s", endpoint)
		err = s.client.POSTV1(endpoint)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
			continue
		}
	}

	producerList := s.getTopicProducerList(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerList.HTTPAddrs(),
		"delete_channel",
		"channel/delete",
		fmt.Sprintf("topic=%s&channel=%s",
			url.QueryEscape(topicName), url.QueryEscape(channelName)))

	s.notifyAdminAction("delete_channel", topicName, channelName, "", req)

	return nil
}

func (s *httpServer) doTopicAction(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	return nil, s.topicChannelAction(req, topicName, "")
}

func (s *httpServer) doChannelAction(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")
	return nil, s.topicChannelAction(req, topicName, channelName)
}

func (s *httpServer) topicChannelAction(req *http.Request, topicName string, channelName string) error {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return http_api.Err{400, err.Error()}
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		return http_api.Err{400, err.Error()}
	}

	action := js.Get("action").MustString()

	switch action {
	case "pause", "unpause":
		if channelName != "" {
			err = s.pauseChannel(req, topicName, channelName, action)
		} else {
			err = s.pauseTopic(req, topicName, action)
		}
	case "empty":
		if channelName != "" {
			err = s.emptyChannel(req, topicName, channelName)
		} else {
			err = s.emptyTopic(req, topicName)
		}
	default:
		return http_api.Err{400, "INVALID_ACTION"}
	}

	if err != nil {
		return http_api.Err{500, err.Error()}
	}
	return nil
}

func (s *httpServer) pauseTopic(req *http.Request, topicName string, action string) error {
	producerList := s.getTopicProducerList(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerList.HTTPAddrs(),
		action+"_topic",
		"topic/"+action,
		fmt.Sprintf("topic=%s",
			url.QueryEscape(topicName)))
	s.notifyAdminAction(action+"_topic", topicName, "", "", req)
	return nil
}

func (s *httpServer) pauseChannel(req *http.Request, topicName string, channelName string, action string) error {
	producerList := s.getTopicProducerList(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerList.HTTPAddrs(),
		action+"_channel",
		"channel/"+action,
		fmt.Sprintf("topic=%s&channel=%s",
			url.QueryEscape(topicName), url.QueryEscape(channelName)))
	s.notifyAdminAction(action+"_channel", topicName, channelName, "", req)
	return nil
}

func (s *httpServer) emptyTopic(req *http.Request, topicName string) error {
	producerList := s.getTopicProducerList(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerList.HTTPAddrs(),
		"empty_topic",
		"topic/empty",
		fmt.Sprintf("topic=%s",
			url.QueryEscape(topicName)))
	s.notifyAdminAction("empty_topic", topicName, "", "", req)
	return nil
}

func (s *httpServer) emptyChannel(req *http.Request, topicName string, channelName string) error {
	producerList := s.getTopicProducerList(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerList.HTTPAddrs(),
		"empty_channel",
		"channel/empty",
		fmt.Sprintf("topic=%s&channel=%s",
			url.QueryEscape(topicName), url.QueryEscape(channelName)))
	s.notifyAdminAction("empty_channel", topicName, channelName, "", req)
	return nil
}

type counterStats struct {
	Node         string `json:"node"`
	TopicName    string `json:"topic_name"`
	ChannelName  string `json:"channel_name"`
	MessageCount int64  `json:"message_count"`
}

func (s *httpServer) counterHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	stats := make(map[string]*counterStats)
	producerList := s.getProducerList()
	_, channelStats, _ := s.ci.GetNSQDStats(producerList, "")
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
	return stats, nil
}

func (s *httpServer) graphiteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	metric, err := reqParams.Get("metric")
	if err != nil || metric != "rate" {
		return nil, http_api.Err{404, "INVALID_ARG_METRIC"}
	}

	target, err := reqParams.Get("target")
	if err != nil {
		return nil, http_api.Err{404, "INVALID_ARG_TARGET"}
	}

	params := url.Values{}
	params.Set("from", fmt.Sprintf("-%dsec", s.ctx.nsqadmin.opts.StatsdInterval*2/time.Second))
	params.Set("until", fmt.Sprintf("-%dsec", s.ctx.nsqadmin.opts.StatsdInterval/time.Second))
	params.Set("format", "json")
	params.Set("target", target)
	query := fmt.Sprintf("/render?%s", params.Encode())
	url := s.ctx.nsqadmin.opts.GraphiteURL + query

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
		rateDivisor := s.ctx.nsqadmin.opts.StatsdInterval / time.Second
		rateStr = fmt.Sprintf("%.2f", rate/float64(rateDivisor))
	}
	return struct {
		Rate string `json:"rate"`
	}{rateStr}, nil
}

func (s *httpServer) getProducerList() clusterinfo.ProducerList {
	var producerList clusterinfo.ProducerList
	if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses) != 0 {
		producerList, _ = s.ci.GetLookupdProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	} else {
		producerList, _ = s.ci.GetNSQDProducers(s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	}
	return producerList
}

func (s *httpServer) getTopicProducerList(topicName string) clusterinfo.ProducerList {
	var producerList clusterinfo.ProducerList
	if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses) != 0 {
		producerList, _ = s.ci.GetLookupdTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	} else {
		producerList, _ = s.ci.GetNSQDTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	}
	return producerList
}

func (s *httpServer) performVersionNegotiatedRequestsToNSQD(
	nsqlookupdAddrs []string, nsqdAddrs []string,
	deprecatedURI string, v1URI string, queryString string) {
	var err error
	// get producer structs in one set of up-front requests
	// so we can negotiate versions
	//
	// (this returns an empty list if there are no nsqlookupd configured)
	producerList, _ := s.ci.GetLookupdProducers(nsqlookupdAddrs)
	for _, addr := range nsqdAddrs {
		var nodeVer semver.Version

		uri := deprecatedURI
		producer := producerList.Search(addr)
		if producer != nil {
			nodeVer = producer.VersionObj
		} else {
			// we couldn't find the node in our list
			// so ask it for a version directly
			nodeVer, err = s.ci.GetVersion(addr)
			if err != nil {
				s.ctx.nsqadmin.logf("ERROR: failed to get nsqd %s version - %s", addr, err)
			}
		}

		if nodeVer.NE(semver.Version{}) && nodeVer.GTE(v1EndpointVersion) {
			uri = v1URI
		}

		endpoint := fmt.Sprintf("http://%s/%s?%s", addr, uri, queryString)
		s.ctx.nsqadmin.logf("NSQD: querying %s", endpoint)
		err := s.client.POSTV1(endpoint)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: nsqd %s - %s", endpoint, err)
			continue
		}
	}
}
