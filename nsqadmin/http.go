package nsqadmin

import (
	"fmt"
	"html/template"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bitly/nsq/internal/http_api"
	"github.com/bitly/nsq/internal/lookupd"
	"github.com/bitly/nsq/internal/protocol"
	"github.com/bitly/nsq/internal/stringy"
	"github.com/bitly/nsq/internal/version"
	"github.com/bitly/nsq/nsqadmin/templates"
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
}

func NewHTTPServer(ctx *Context) *httpServer {
	var proxy *httputil.ReverseProxy

	templates.T.Funcs(template.FuncMap{
		"commafy":        stringy.Commafy,
		"nanotohuman":    stringy.NanoSecondToHuman,
		"floatToPercent": stringy.FloatToPercent,
		"percSuffix":     stringy.PercSuffix,
		"getNodeConsistencyClass": func(node *lookupd.Producer) string {
			if node.IsInconsistent(len(ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)) {
				return "btn-warning"
			}
			return ""
		},
	})

	templates.Parse()

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
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	router.Handle("GET", "/", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/nodes", http_api.Decorate(s.nodesHandler, log))
	router.Handle("GET", "/node/:node", http_api.Decorate(s.nodeHandler, log))
	router.Handle("GET", "/topic/:topic", http_api.Decorate(s.topicHandler, log))
	router.Handle("GET", "/topic/:topic/:channel", http_api.Decorate(s.channelHandler, log))
	router.Handle("GET", "/static/:asset", http_api.Decorate(s.embeddedAssetHandler, log))
	router.Handle("GET", "/counter", http_api.Decorate(s.counterHandler, log))
	router.Handle("GET", "/counter/data", http_api.Decorate(s.counterDataHandler, log))
	router.Handle("GET", "/lookup", http_api.Decorate(s.lookupHandler, log))
	router.Handle("GET", "/graphite_data", http_api.Decorate(s.graphiteDataHandler, log))

	router.Handle("POST", "/tombstone_topic_producer", http_api.Decorate(s.tombstoneTopicProducerHandler, log))
	router.Handle("POST", "/empty_topic", http_api.Decorate(s.emptyTopicHandler, log))
	router.Handle("POST", "/delete_topic", http_api.Decorate(s.deleteTopicHandler, log))
	router.Handle("POST", "/pause_topic", http_api.Decorate(s.pauseTopicHandler, log))
	router.Handle("POST", "/unpause_topic", http_api.Decorate(s.pauseTopicHandler, log))
	router.Handle("POST", "/empty_channel", http_api.Decorate(s.emptyChannelHandler, log))
	router.Handle("POST", "/delete_channel", http_api.Decorate(s.deleteChannelHandler, log))
	router.Handle("POST", "/pause_channel", http_api.Decorate(s.pauseChannelHandler, log))
	router.Handle("POST", "/unpause_channel", http_api.Decorate(s.pauseChannelHandler, log))
	router.Handle("POST", "/create_topic_channel", http_api.Decorate(s.createTopicChannelHandler, log))

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

func (s *httpServer) embeddedAssetHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	assetName := ps.ByName("asset")

	asset, error := templates.Asset(assetName)
	if error != nil {
		return nil, http_api.Err{404, "NOT_FOUND"}
	}

	if strings.HasSuffix(assetName, ".js") {
		w.Header().Set("Content-Type", "text/javascript")
	} else if strings.HasSuffix(assetName, ".css") {
		w.Header().Set("Content-Type", "text/css")
	}

	return asset, nil
}

func (s *httpServer) indexHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	var topics []string
	if len(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses) != 0 {
		topics, _ = lookupd.GetLookupdTopics(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	} else {
		topics, _ = lookupd.GetNSQDTopics(s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	}

	p := struct {
		Title        string
		GraphOptions *GraphOptions
		Topics       Topics
		Version      string
	}{
		Title:        "NSQ",
		GraphOptions: NewGraphOptions(w, req, reqParams, s.ctx),
		Topics:       TopicsFromStrings(topics),
		Version:      version.Binary,
	}
	err = templates.T.ExecuteTemplate(w, "index.html", p)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: executing template - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	return nil, nil
}

func (s *httpServer) topicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	producers := s.getProducers(topicName)
	topicStats, channelStats, _ := lookupd.GetNSQDStats(producers, topicName)

	globalTopicStats := &lookupd.TopicStats{HostAddress: "Total"}
	for _, t := range topicStats {
		globalTopicStats.Add(t)
	}

	hasE2eLatency := globalTopicStats.E2eProcessingLatency != nil &&
		len(globalTopicStats.E2eProcessingLatency.Percentiles) > 0

	var firstTopic *lookupd.TopicStats
	if len(topicStats) > 0 {
		firstTopic = topicStats[0]
	}

	p := struct {
		Title            string
		GraphOptions     *GraphOptions
		Version          string
		Topic            string
		TopicProducers   []string
		TopicStats       []*lookupd.TopicStats
		FirstTopic       *lookupd.TopicStats
		GlobalTopicStats *lookupd.TopicStats
		ChannelStats     map[string]*lookupd.ChannelStats
		HasE2eLatency    bool
	}{
		Title:            fmt.Sprintf("NSQ %s", topicName),
		GraphOptions:     NewGraphOptions(w, req, reqParams, s.ctx),
		Version:          version.Binary,
		Topic:            topicName,
		TopicProducers:   producers,
		TopicStats:       topicStats,
		FirstTopic:       firstTopic,
		GlobalTopicStats: globalTopicStats,
		ChannelStats:     channelStats,
		HasE2eLatency:    hasE2eLatency,
	}
	err = templates.T.ExecuteTemplate(w, "topic.html", p)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: executing template - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	return nil, nil
}

func (s *httpServer) channelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topicName := ps.ByName("topic")
	channelName := ps.ByName("channel")

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	producers := s.getProducers(topicName)
	_, allChannelStats, _ := lookupd.GetNSQDStats(producers, topicName)
	channelStats, ok := allChannelStats[channelName]
	if !ok {
		return nil, http_api.Err{404, "NOT_FOUND"}
	}

	hasE2eLatency := channelStats.E2eProcessingLatency != nil &&
		len(channelStats.E2eProcessingLatency.Percentiles) > 0

	var firstHost *lookupd.ChannelStats
	if len(channelStats.HostStats) > 0 {
		firstHost = channelStats.HostStats[0]
	}

	p := struct {
		Title          string
		GraphOptions   *GraphOptions
		Version        string
		Topic          string
		Channel        string
		TopicProducers []string
		ChannelStats   *lookupd.ChannelStats
		FirstHost      *lookupd.ChannelStats
		HasE2eLatency  bool
	}{
		Title:          fmt.Sprintf("NSQ %s / %s", topicName, channelName),
		GraphOptions:   NewGraphOptions(w, req, reqParams, s.ctx),
		Version:        version.Binary,
		Topic:          topicName,
		Channel:        channelName,
		TopicProducers: producers,
		ChannelStats:   channelStats,
		FirstHost:      firstHost,
		HasE2eLatency:  hasE2eLatency,
	}

	err = templates.T.ExecuteTemplate(w, "channel.html", p)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: executing template - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	return nil, nil
}

func (s *httpServer) lookupHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	channels := make(map[string][]string)
	allTopics, _ := lookupd.GetLookupdTopics(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	for _, topicName := range allTopics {
		var producers []string
		producers, _ = lookupd.GetLookupdTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
		if len(producers) == 0 {
			topicChannels, _ := lookupd.GetLookupdTopicChannels(topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
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
		GraphOptions: NewGraphOptions(w, req, reqParams, s.ctx),
		TopicMap:     channels,
		Lookupd:      s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		Version:      version.Binary,
	}
	err = templates.T.ExecuteTemplate(w, "lookup.html", p)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: executing template - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	return nil, nil
}

func (s *httpServer) createTopicChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, err := reqParams.Get("topic")
	if err != nil || !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	channelName, err := reqParams.Get("channel")
	if err != nil || (len(channelName) > 0 && !protocol.IsValidChannelName(channelName)) {
		return nil, http_api.Err{400, "INVALID_ARG_CHANNEL"}
	}

	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := lookupd.GetVersion(addr)
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
		producerAddrs, _ := lookupd.GetLookupdTopicProducers(topicName,
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

	http.Redirect(w, req, "/lookup", 302)
	return nil, nil
}

func (s *httpServer) tombstoneTopicProducerHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_NODE"}
	}

	rd, _ := reqParams.Get("rd")
	if !strings.HasPrefix(rd, "/") {
		rd = "/"
	}

	// tombstone the topic on all the lookupds
	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := lookupd.GetVersion(addr)
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

	nsqdVersion, err := lookupd.GetVersion(node)
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

	http.Redirect(w, req, rd, 302)
	return nil, nil
}

func (s *httpServer) deleteTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	rd, _ := reqParams.Get("rd")
	if !strings.HasPrefix(rd, "/") {
		rd = "/"
	}

	// for topic removal, you need to get all the producers *first*
	producerAddrs := s.getProducers(topicName)

	// remove the topic from all the lookupds
	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := lookupd.GetVersion(addr)
		if err != nil {
			s.ctx.nsqadmin.logf("ERROR: failed to get nsqlookupd %s version - %s", addr, err)
		}

		uri := "delete_topic"
		if !nsqlookupdVersion.LT(v1EndpointVersion) {
			uri = "topic/delete"
		}

		endpoint := fmt.Sprintf("http://%s/%s?topic=%s", addr, uri, url.QueryEscape(topicName))
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

	http.Redirect(w, req, rd, 302)
	return nil, nil
}

func (s *httpServer) deleteChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	rd, _ := reqParams.Get("rd")
	if !strings.HasPrefix(rd, "/") {
		rd = fmt.Sprintf("/topic/%s", url.QueryEscape(topicName))
	}

	for _, addr := range s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses {
		nsqlookupdVersion, err := lookupd.GetVersion(addr)
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

	http.Redirect(w, req, rd, 302)
	return nil, nil
}

func (s *httpServer) emptyTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
		"empty_topic",
		"topic/empty",
		fmt.Sprintf("topic=%s",
			url.QueryEscape(topicName)))

	s.notifyAdminAction("empty_topic", topicName, "", "", req)

	http.Redirect(w, req, fmt.Sprintf("/topic/%s", url.QueryEscape(topicName)), 302)
	return nil, nil
}

func (s *httpServer) pauseTopicHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	verb := "pause"
	if strings.Contains(req.URL.Path, "unpause") {
		verb = "unpause"
	}

	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
		verb+"_topic",
		"topic/"+verb,
		fmt.Sprintf("topic=%s",
			url.QueryEscape(topicName)))

	s.notifyAdminAction(verb+"_topic", topicName, "", "", req)

	http.Redirect(w, req, fmt.Sprintf("/topic/%s", url.QueryEscape(topicName)), 302)
	return nil, nil
}

func (s *httpServer) emptyChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
		"empty_channel",
		"channel/empty",
		fmt.Sprintf("topic=%s&channel=%s",
			url.QueryEscape(topicName), url.QueryEscape(channelName)))

	s.notifyAdminAction("empty_channel", topicName, channelName, "", req)

	http.Redirect(w, req, fmt.Sprintf("/topic/%s/%s",
		url.QueryEscape(topicName), url.QueryEscape(channelName)), 302)
	return nil, nil
}

func (s *httpServer) pauseChannelHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams := &http_api.PostParams{req}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	verb := "pause"
	if strings.Contains(req.URL.Path, "unpause") {
		verb = "unpause"
	}

	producerAddrs := s.getProducers(topicName)
	s.performVersionNegotiatedRequestsToNSQD(
		s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
		producerAddrs,
		verb+"_channel",
		"channel/"+verb,
		fmt.Sprintf("topic=%s&channel=%s",
			url.QueryEscape(topicName), url.QueryEscape(channelName)))

	s.notifyAdminAction(verb+"_channel", topicName, channelName, "", req)

	http.Redirect(w, req, fmt.Sprintf("/topic/%s/%s", url.QueryEscape(topicName), url.QueryEscape(channelName)), 302)
	return nil, nil
}

func (s *httpServer) nodeHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	node := ps.ByName("node")

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	found := false
	for _, n := range s.ctx.nsqadmin.opts.NSQDHTTPAddresses {
		if node == n {
			found = true
			break
		}
	}
	producers, _ := lookupd.GetLookupdProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	for _, p := range producers {
		if node == fmt.Sprintf("%s:%d", p.BroadcastAddress, p.HTTPPort) {
			found = true
			break
		}
	}
	if !found {
		return nil, http_api.Err{404, "NOT_FOUND"}
	}

	topicStats, channelStats, _ := lookupd.GetNSQDStats([]string{node}, "")

	numClients := int64(0)
	numMessages := int64(0)
	for _, ts := range topicStats {
		for _, cs := range ts.Channels {
			numClients += int64(len(cs.Clients))
		}
		numMessages += ts.MessageCount
	}

	p := struct {
		Title        string
		Version      string
		GraphOptions *GraphOptions
		Node         Node
		TopicStats   []*lookupd.TopicStats
		ChannelStats map[string]*lookupd.ChannelStats
		NumMessages  int64
		NumClients   int64
	}{
		Title:        "NSQ Node - " + node,
		Version:      version.Binary,
		GraphOptions: NewGraphOptions(w, req, reqParams, s.ctx),
		Node:         Node(node),
		TopicStats:   topicStats,
		ChannelStats: channelStats,
		NumMessages:  numMessages,
		NumClients:   numClients,
	}
	err = templates.T.ExecuteTemplate(w, "node.html", p)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: executing template - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	return nil, nil
}

func (s *httpServer) nodesHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	producers, _ := lookupd.GetLookupdProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)

	p := struct {
		Title        string
		Version      string
		GraphOptions *GraphOptions
		Producers    []*lookupd.Producer
		Lookupd      []string
	}{
		Title:        "NSQ Nodes",
		Version:      version.Binary,
		GraphOptions: NewGraphOptions(w, req, reqParams, s.ctx),
		Producers:    producers,
		Lookupd:      s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses,
	}
	err = templates.T.ExecuteTemplate(w, "nodes.html", p)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: executing template - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	return nil, nil
}

type counterTarget struct{}

func (c counterTarget) Target(key string) ([]string, string) {
	return []string{fmt.Sprintf("sumSeries(%%stopic.*.channel.*.%s)", key)}, "green"
}

func (c counterTarget) Host() string {
	return "*"
}

func (s *httpServer) counterHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	p := struct {
		Title        string
		Version      string
		GraphOptions *GraphOptions
		Target       counterTarget
	}{
		Title:        "NSQ Message Counts",
		Version:      version.Binary,
		GraphOptions: NewGraphOptions(w, req, reqParams, s.ctx),
		Target:       counterTarget{},
	}
	err = templates.T.ExecuteTemplate(w, "counter.html", p)
	if err != nil {
		s.ctx.nsqadmin.logf("ERROR: executing template - %s", err)
		return nil, http_api.Err{500, "INTERNAL_ERROR"}
	}
	return nil, nil
}

// this endpoint works by giving out an ID that maps to a stats dictionary
// The initial request is the number of messages processed since each nsqd started up.
// Subsequent requsts pass that ID and get an updated delta based on each individual channel/nsqd message count
// That ID must be re-requested or it will be expired.
func (s *httpServer) counterDataHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	statsID, _ := reqParams.Get("id")
	now := time.Now()
	if statsID == "" {
		// make a new one
		statsID = fmt.Sprintf("%d.%d", now.Unix(), now.UnixNano())
	}

	stats, ok := s.counters[statsID]
	if !ok {
		stats = make(map[string]int64)
	}
	newStats := make(map[string]int64)
	newStats["time"] = now.Unix()

	producers, _ := lookupd.GetLookupdProducers(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	addresses := make([]string, len(producers))
	for i, p := range producers {
		addresses[i] = p.HTTPAddress()
	}
	_, channelStats, _ := lookupd.GetNSQDStats(addresses, "")

	var newMessages int64
	var totalMessages int64
	for _, channelStats := range channelStats {
		for _, hostChannelStats := range channelStats.HostStats {
			key := fmt.Sprintf("%s:%s:%s", channelStats.TopicName, channelStats.ChannelName, hostChannelStats.HostAddress)
			d, ok := stats[key]
			if ok && d <= hostChannelStats.MessageCount {
				newMessages += (hostChannelStats.MessageCount - d)
			}
			totalMessages += hostChannelStats.MessageCount
			newStats[key] = hostChannelStats.MessageCount
		}
	}
	s.counters[statsID] = newStats

	return struct {
		NewMessages   int64  `json:"new_messages"`
		TotalMessages int64  `json:"total_messages"`
		ID            string `json:"id"`
	}{newMessages, totalMessages, statsID}, nil
}

func (s *httpServer) graphiteDataHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
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

	w.Header().Set("Content-Type", "application/json")
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
		producers, _ = lookupd.GetLookupdTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	} else {
		producers, _ = lookupd.GetNSQDTopicProducers(topicName, s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	}
	return producers
}

func producerSearch(producers []*lookupd.Producer, needle string) *lookupd.Producer {
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
	producers, _ := lookupd.GetLookupdProducers(nsqlookupdAddrs)
	for _, addr := range nsqdAddrs {
		var nodeVer semver.Version

		uri := deprecatedURI
		producer := producerSearch(producers, addr)
		if producer != nil {
			nodeVer = producer.VersionObj
		} else {
			// we couldn't find the node in our list
			// so ask it for a version directly
			nodeVer, err = lookupd.GetVersion(addr)
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
