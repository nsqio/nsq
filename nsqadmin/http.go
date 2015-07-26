package nsqadmin

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/bitly/nsq/internal/clusterinfo"
	"github.com/bitly/nsq/internal/http_api"
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

	// deprecated endpoints
	router.Handle("GET", "/", http_api.Decorate(s.indexHandler, log))
	router.Handle("GET", "/static/:asset", http_api.Decorate(s.staticAssetHandler, log, http_api.PlainText))
	router.Handle("GET", "/graphite_data", http_api.Decorate(s.graphiteDataHandler, log, http_api.PlainText))

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
