package nsqadmin

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/internal/clusterinfo"
	"github.com/bitly/nsq/internal/http_api"
	"github.com/bitly/nsq/internal/protocol"
	"github.com/blang/semver"
	"github.com/julienschmidt/httprouter"
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
	ctx      *Context
	counters map[string]map[string]int64
	proxy    *httputil.ReverseProxy
	router   http.Handler
	ci       *clusterinfo.ClusterInfo
}

func NewHTTPServer(ctx *Context) *httpServer {
	var proxy *httputil.ReverseProxy

	if ctx.nsqadmin.opts.ProxyGraphite {
		proxy = NewSingleHostReverseProxy(ctx.nsqadmin.graphiteURL, 20*time.Second)
	}

	log := http_api.Log(ctx.nsqadmin.opts.Logger)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqadmin.opts.Logger)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqadmin.opts.Logger)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqadmin.opts.Logger)
	s := &httpServer{
		ctx:      ctx,
		counters: make(map[string]map[string]int64),
		proxy:    proxy,
		router:   router,
		ci:       clusterinfo.New(ctx.nsqadmin.opts.Logger),
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	// v1 endpoints
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.V1))
	router.Handle("GET", "/topics/:topic", http_api.Decorate(s.doTopic, log, http_api.V1))
	router.Handle("GET", "/topics/:topic/:channel", http_api.Decorate(s.doChannel, log, http_api.V1))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.V1))
	router.Handle("GET", "/nodes/:node", http_api.Decorate(s.doNode, log, http_api.V1))
	router.Handle("POST", "/topics", http_api.Decorate(s.doCreateTopicChannel, log, http_api.V1))
	router.Handle("POST", "/topics/:topic", http_api.Decorate(s.doTopicAction, log, http_api.V1))
	router.Handle("POST", "/topics/:topic/:channel", http_api.Decorate(s.doChannelAction, log, http_api.V1))
	router.Handle("DELETE", "/nodes/:node", http_api.Decorate(s.doTombstoneTopicNode, log, http_api.V1))
	router.Handle("DELETE", "/topics/:topic", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("DELETE", "/topics/:topic/:channel", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("GET", "/counter", http_api.Decorate(s.counterHandler, log, http_api.V1))
	router.Handle("GET", "/graphite", http_api.Decorate(s.graphiteHandler, log, http_api.V1))

	// deprecated endpoints
	router.Handle("GET", "/", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/static/:asset", http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))

	if s.ctx.nsqadmin.opts.ProxyGraphite {
		router.Handler("GET", "/render", s.proxy)
	}

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
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
			var producers []string
			producers, _ = s.ci.GetLookupdTopicProducers(
				topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
			if len(producers) == 0 {
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

	producers := s.getProducers(topicName)
	topicStats, _, _ := s.ci.GetNSQDStats(producers, topicName)

	allNodesTopicStats := &clusterinfo.TopicStats{TopicName: topicName}
	for _, t := range topicStats {
		allNodesTopicStats.Add(t)
	}

	return allNodesTopicStats, nil
}

func (s *httpServer) doChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	producers := s.getProducers(topicName)
	_, allChannelStats, _ := s.ci.GetNSQDStats(producers, topicName)
	channelStats := allChannelStats[channelName]

	return channelStats, nil
}

func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	producers, _ := s.ci.GetLookupdProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	addresses := make([]string, len(producers))
	for i, p := range producers {
		addresses[i] = p.HTTPAddress()
	}
	// _, channelStats, _ := s.ci.GetNSQDStats(addresses, "")
	return struct {
		Nodes []*clusterinfo.Producer `json:"nodes"`
	}{producers}, nil
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

	found := false
	for _, n := range s.ctx.nsqadmin.opts.NSQDHTTPAddresses {
		if node == n {
			found = true
			break
		}
	}
	producers, _ := s.ci.GetLookupdProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	for _, p := range producers {
		if node == fmt.Sprintf("%s:%d", p.BroadcastAddress, p.HTTPPort) {
			found = true
			break
		}
	}

	if !found {
		return nil, http_api.Err{404, "NODE_NOT_FOUND"}
	}

	topicStats, _, _ := s.ci.GetNSQDStats([]string{node}, "")

	var totalClients int64
	var totalMessages int64
	for _, ts := range topicStats {
		for _, cs := range ts.Channels {
			totalClients += int64(len(cs.Clients))
		}
		totalMessages += ts.MessageCount
	}

	return struct {
		Node          Node                      `json:"node"`
		TopicStats    []*clusterinfo.TopicStats `json:"topics"`
		TotalMessages int64                     `json:"total_messages"`
		TotalClients  int64                     `json:"total_clients"`
	}{
		Node:          Node(node),
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
		_, err = http_api.NegotiateV1("POST", endpoint, nil)
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
			_, err := http_api.NegotiateV1("POST", endpoint, nil)
			if err != nil {
				s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
				continue
			}
		}
	}

	s.notifyAdminAction("create_topic", topicName, "", "", req)

	if len(channelName) > 0 {
		// TODO: we can remove this when we push new channel information from nsqlookupd -> nsqd
		producerAddrs, _ := s.ci.GetLookupdTopicProducers(topicName,
			s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)

		s.performVersionNegotiatedRequestsToNSQD(
			s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
			producerAddrs,
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
		_, err = http_api.NegotiateV1("POST", endpoint, nil)
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
	_, err = http_api.NegotiateV1("POST", endpoint, nil)
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
	producerAddrs := s.getProducers(topicName)

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
		_, err = http_api.NegotiateV1("POST", endpoint, nil)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
			continue
		}
	}

	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
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
		_, err = http_api.NegotiateV1("POST", endpoint, nil)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: lookupd %s - %s", endpoint, err)
			continue
		}
	}

	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
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
	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
		action+"_topic",
		"topic/"+action,
		fmt.Sprintf("topic=%s",
			url.QueryEscape(topicName)))
	s.notifyAdminAction(action+"_topic", topicName, "", "", req)
	return nil
}

func (s *httpServer) pauseChannel(req *http.Request, topicName string, channelName string, action string) error {
	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
		action+"_channel",
		"channel/"+action,
		fmt.Sprintf("topic=%s&channel=%s",
			url.QueryEscape(topicName), url.QueryEscape(channelName)))
	s.notifyAdminAction(action+"_channel", topicName, channelName, "", req)
	return nil
}

func (s *httpServer) emptyTopic(req *http.Request, topicName string) error {
	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
		"empty_topic",
		"topic/empty",
		fmt.Sprintf("topic=%s",
			url.QueryEscape(topicName)))
	s.notifyAdminAction("empty_topic", topicName, "", "", req)
	return nil
}

func (s *httpServer) emptyChannel(req *http.Request, topicName string, channelName string) error {
	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
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
	producers, _ := s.ci.GetLookupdProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	addresses := make([]string, len(producers))
	for i, p := range producers {
		addresses[i] = p.HTTPAddress()
	}
	_, channelStats, _ := s.ci.GetNSQDStats(addresses, "")

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

	query := rateQuery(target, s.ctx.nsqadmin.opts.StatsdInterval)
	url := s.ctx.nsqadmin.opts.GraphiteURL + query
	s.ctx.nsqadmin.logf("GRAPHITE: %s", url)
	response, err := graphiteGet(url)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: graphite request failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	resp, err := parseRateResponse(response, s.ctx.nsqadmin.opts.StatsdInterval)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: response formatting failed - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}

	return resp, nil
}

func graphiteGet(url string) ([]byte, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return contents, nil
}

func (s *httpServer) getProducers(topicName string) []string {
	var producers []string
	if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses) != 0 {
		producers, _ = s.ci.GetLookupdTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	} else {
		producers, _ = s.ci.GetNSQDTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	}
	return producers
}

func producerSearch(producers []*clusterinfo.Producer, needle string) *clusterinfo.Producer {
	for _, producer := range producers {
		addr := net.JoinHostPort(producer.BroadcastAddress, strconv.Itoa(producer.HTTPPort))
		if needle == addr {
			return producer
		}
	}
	return nil
}

func (s *httpServer) performVersionNegotiatedRequestsToNSQD(
	nsqlookupdAddrs []string, nsqdAddrs []string,
	deprecatedURI string, v1URI string, queryString string) {
	var err error
	// get producer structs in one set of up-front requests
	// so we can negotiate versions
	//
	// (this returns an empty list if there are no nsqlookupd configured)
	producers, _ := s.ci.GetLookupdProducers(nsqlookupdAddrs)
	for _, addr := range nsqdAddrs {
		var nodeVer semver.Version

		uri := deprecatedURI
		producer := producerSearch(producers, addr)
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
		_, err := http_api.NegotiateV1("POST", endpoint, nil)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: nsqd %s - %s", endpoint, err)
			continue
		}
	}
}
