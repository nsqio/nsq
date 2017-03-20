package nsqlookupd

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"net/url"
	"sync/atomic"

	"errors"
	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/julienschmidt/httprouter"
	"runtime"
	"strconv"
)

const (
	MAX_PARTITION_NUM = 255
	MAX_REPLICATOR    = 5
	MAX_LOAD_FACTOR   = 10000
)

func GetValidPartitionNum(numStr string) (int, error) {
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}
	if num > 0 && num <= MAX_PARTITION_NUM {
		return num, nil
	}
	return 0, errors.New("INVALID_PARTITION_NUM")
}

func GetValidPartitionID(numStr string) (int, error) {
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, err
	}
	if num >= 0 && num < MAX_PARTITION_NUM {
		return num, nil
	}
	if num == OLD_VERSION_PID {
		return num, nil
	}
	return 0, errors.New("INVALID_PARTITION_ID")
}

func GetValidReplicator(r string) (int, error) {
	num, err := strconv.Atoi(r)
	if err != nil {
		return 0, err
	}
	if num > 0 && num <= MAX_REPLICATOR {
		return num, nil
	}
	return 0, errors.New("INVALID_REPLICATOR")
}

func GetValidSuggestLF(r string) (int, error) {
	num, err := strconv.Atoi(r)
	if err != nil {
		return 0, err
	}
	if num >= 0 && num <= MAX_LOAD_FACTOR {
		return num, nil
	}
	return 0, errors.New("INVALID_SUGGEST_LOADFACTOR")
}

type httpServer struct {
	ctx    *Context
	router http.Handler
}

func newHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(nsqlookupLog)
	// debug log only print when error or the level is larger than debug.
	debugLog := http_api.DebugLog(nsqlookupLog)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(nsqlookupLog)
	router.NotFound = http_api.LogNotFoundHandler(nsqlookupLog)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(nsqlookupLog)
	s := &httpServer{
		ctx:    ctx,
		router: router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.NegotiateVersion))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, debugLog, http_api.NegotiateVersion))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.NegotiateVersion))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.NegotiateVersion))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.NegotiateVersion))
	router.Handle("GET", "/listlookup", http_api.Decorate(s.doListLookup, debugLog, http_api.NegotiateVersion))
	router.Handle("GET", "/cluster/stats", http_api.Decorate(s.doClusterStats, debugLog, http_api.V1))
	router.Handle("POST", "/cluster/node/remove", http_api.Decorate(s.doRemoveClusterDataNode, log, http_api.V1))
	router.Handle("POST", "/cluster/upgrade/begin", http_api.Decorate(s.doClusterBeginUpgrade, log, http_api.V1))
	router.Handle("POST", "/cluster/upgrade/done", http_api.Decorate(s.doClusterFinishUpgrade, log, http_api.V1))
	router.Handle("POST", "/cluster/lookupd/tombstone", http_api.Decorate(s.doClusterTombstoneLookupd, log, http_api.V1))

	// only v1
	router.Handle("POST", "/loglevel/set", http_api.Decorate(s.doSetLogLevel, log, http_api.V1))
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("PUT", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/topic/partition/expand", http_api.Decorate(s.doChangeTopicPartitionNum, log, http_api.V1))
	router.Handle("POST", "/topic/partition/move", http_api.Decorate(s.doMoveTopicParition, log, http_api.V1))
	router.Handle("POST", "/topic/meta/update", http_api.Decorate(s.doChangeTopicDynamicParam, log, http_api.V1))
	//router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	//router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.NegotiateVersion))
	// debug
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.HandlerFunc("GET", "/debug/pprof/trace", pprof.Trace)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handle("PUT", "/debug/setblockrate", http_api.Decorate(HandleBlockRate, log, http_api.PlainText))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func HandleBlockRate(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	rate, err := strconv.Atoi(req.FormValue("rate"))
	if err != nil {
		return nil, http_api.Err{http.StatusBadRequest, fmt.Sprintf("invalid block rate : %s", err.Error())}
	}
	runtime.SetBlockProfileRate(rate)
	return nil, nil
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version   string `json:"version"`
		HASupport bool   `json:"ha_support"`
	}{
		Version:   version.Binary,
		HASupport: s.ctx.nsqlookupd.coordinator != nil,
	}, nil
}

func (s *httpServer) doClusterStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	stable := false
	nodeStatMap := make(map[string]*NodeStat)
	if s.ctx.nsqlookupd.coordinator != nil {
		if !s.ctx.nsqlookupd.coordinator.IsMineLeader() {
			nsqlookupLog.Logf("request from remote %v should request to leader", req.RemoteAddr)
			return nil, http_api.Err{400, consistence.ErrFailedOnNotLeader}
		}

		stable = s.ctx.nsqlookupd.coordinator.IsClusterStable()
		leaderLFs, nodeLFs := s.ctx.nsqlookupd.coordinator.GetClusterNodeLoadFactor()
		for nid, lf := range leaderLFs {
			p := s.ctx.nsqlookupd.DB.SearchPeerClientByClusterID(nid)
			if p == nil {
				nsqlookupLog.Logf("node not found in peer: %v", nid)
				continue
			}
			stat := &NodeStat{}
			nodeStatMap[nid] = stat
			stat.TCPPort = p.TCPPort
			stat.HTTPPort = p.HTTPPort
			stat.Hostname = p.Hostname
			stat.BroadcastAddress = p.BroadcastAddress
			stat.LeaderLoadFactor = lf
		}
		for nid, lf := range nodeLFs {
			p := s.ctx.nsqlookupd.DB.SearchPeerClientByClusterID(nid)
			if p == nil {
				nsqlookupLog.Logf("node not found in peer: %v", nid)
				continue
			}

			stat, ok := nodeStatMap[nid]
			if !ok {
				stat = &NodeStat{}
				nodeStatMap[nid] = stat
			}

			stat.TCPPort = p.TCPPort
			stat.HTTPPort = p.HTTPPort
			stat.Hostname = p.Hostname
			stat.BroadcastAddress = p.BroadcastAddress
			stat.NodeLoadFactor = lf
		}
		nsqlookupLog.Logf("node stats map: %v", nodeStatMap)
	}
	nodeStatList := make([]*NodeStat, 0, len(nodeStatMap))
	for _, v := range nodeStatMap {
		nodeStatList = append(nodeStatList, v)
	}
	return struct {
		Stable       bool        `json:"stable"`
		NodeStatList []*NodeStat `json:"node_stat_list"`
	}{
		Stable:       stable,
		NodeStatList: nodeStatList,
	}, nil
}

func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topics := s.ctx.nsqlookupd.DB.FindTopics()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	topicPartition := reqParams.Get("partition")
	if topicPartition == "" {
		topicPartition = "*"
	}
	channels := s.ctx.nsqlookupd.DB.FindChannelRegs(topicName, topicPartition).Channels()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}

func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		nsqlookupLog.Logf("lookup topic param error : %v", err.Error())
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	topicPartition := reqParams.Get("partition")
	if topicPartition == "" {
		topicPartition = "*"
	}
	// access mode will be used for disable some write method (pub) to allow
	// removing the topic from some node without affecting the consumer.
	// if a node is setting read only, then with access mode "w", this node
	// will be filtered before return to client.
	// The access mode "r" will return all nodes (that have the topic) without any filter.
	accessMode := reqParams.Get("access")
	if accessMode == "" {
		accessMode = "r"
	}
	if accessMode != "w" && accessMode != "r" {
		return nil, http_api.Err{400, "INVALID_ACCESS_MODE"}
	}
	// check consistent level
	// The reported info in the register db may not consistent,
	// if the client need a strong consistent result, we check the db result with
	// the leadership info from etcd.
	checkConsistent := reqParams.Get("consistent")

	registrations := s.ctx.nsqlookupd.DB.FindTopicProducers(topicName, topicPartition)
	isFoundInRegister := len(registrations) > 0
	if len(registrations) == 0 {
		nsqlookupLog.LogDebugf("lookup topic %v-%v not found", topicName, topicPartition)
		// try to find in cluster info
		if accessMode == "w" {
			clusterNodes, clusterErr := s.ctx.nsqlookupd.coordinator.GetTopicLeaderNodes(topicName)
			if clusterErr != nil {
				if clusterErr == consistence.ErrKeyNotFound {
					return nil, http_api.Err{404, clusterErr.Error()}
				}
				return nil, http_api.Err{500, clusterErr.Error()}
			}
			if topicPartition == "*" {
				for pid, nodeID := range clusterNodes {
					peerInfo := s.ctx.nsqlookupd.DB.SearchPeerClientByClusterID(nodeID)
					if peerInfo != nil {
						var reg TopicProducerReg
						reg.PartitionID = pid
						reg.ProducerNode = &Producer{peerInfo: peerInfo}
						registrations = append(registrations, reg)
					}
				}
			} else {
				nodeID, ok := clusterNodes[topicPartition]
				if ok {
					peerInfo := s.ctx.nsqlookupd.DB.SearchPeerClientByClusterID(nodeID)
					if peerInfo != nil {
						var reg TopicProducerReg
						reg.PartitionID = topicPartition
						reg.ProducerNode = &Producer{peerInfo: peerInfo}
						registrations = append(registrations, reg)
					}
				}
			}
			if len(registrations) > 0 {
				nsqlookupLog.Logf("no topic producers found in memory, found in cluster: %v, %v", len(clusterNodes), len(registrations))
			}
		}
	}
	partitionProducers := make(map[string]*PeerInfo)
	allProducers := make(map[string]*Producer, len(registrations))
	// note: tombstone has been changed : the tomb is used for producer.
	// tombstone node is filter so that any new data will not be put to this node,
	// but the consumer can still consume the old data until no data to avoid the data lost
	// while put some node offline.
	filterTomb := true
	if accessMode == "r" {
		filterTomb = false
	}
	registrations = registrations.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		filterTomb)

	emptyChanFiltered := false
	for _, r := range registrations {
		var leaderProducer *Producer
		pid, _ := strconv.Atoi(r.PartitionID)
		if checkConsistent != "" {
			// check leader only the client need consistent
			if s.ctx.nsqlookupd.coordinator.IsTopicLeader(topicName, pid, r.ProducerNode.peerInfo.DistributedID) {
				leaderProducer = r.ProducerNode
			}
		} else {
			leaderProducer = r.ProducerNode
		}
		if leaderProducer != nil {
			if accessMode == "w" {
				// check if any channel on the specific topic producer node
				channels := s.ctx.nsqlookupd.DB.FindChannelRegs(topicName, r.PartitionID)
				if len(channels) == 0 {
					nsqlookupLog.Logf("no channels under this partition node: %v, %v", topicName, r)
					emptyChanFiltered = true
					continue
				}
			}
			if pid >= 0 {
				// old node should be filtered since no any partition info
				partitionProducers[r.PartitionID] = leaderProducer.peerInfo
			}
			allProducers[leaderProducer.peerInfo.Id] = leaderProducer
		}
	}
	producers := make(Producers, 0, len(allProducers))
	for _, p := range allProducers {
		producers = append(producers, p)
	}

	peers := producers.PeerInfo()
	if isFoundInRegister && emptyChanFiltered &&
		len(partitionProducers) == 0 && len(peers) == 0 {
		return nil, http_api.Err{404, "Topic has no channel, should init at least one for the new topic"}
	}
	// maybe channels should be under topic partitions?
	channels := s.ctx.nsqlookupd.DB.FindChannelRegs(topicName, topicPartition).Channels()
	needMeta := reqParams.Get("metainfo")
	if needMeta != "" {
		meta, err := s.ctx.nsqlookupd.coordinator.GetTopicMetaInfo(topicName)
		if err != nil {
			// maybe topic on old nsqd
			if err != consistence.ErrKeyNotFound {
				return nil, http_api.Err{500, err.Error()}
			}
		}
		return map[string]interface{}{
			"channels": channels,
			"meta": map[string]interface{}{
				"partition_num": meta.PartitionNum,
				"replica":       meta.Replica,
			},
			"producers":  peers,
			"partitions": partitionProducers,
		}, nil
	}
	return map[string]interface{}{
		"channels":   channels,
		"producers":  peers,
		"partitions": partitionProducers,
	}, nil
}

func (s *httpServer) doSetLogLevel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	levelStr := reqParams.Get("loglevel")
	if levelStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_LEVEL"}
	}
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return nil, http_api.Err{400, "BAD_LEVEL_STRING"}
	}
	nsqlookupLog.SetLevel(int32(level))
	consistence.SetCoordLogLevel(int32(level))
	return nil, nil
}

func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	pnumStr := reqParams.Get("partition_num")
	if pnumStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC_PARTITION_NUM"}
	}
	pnum, err := GetValidPartitionNum(pnumStr)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_PARTITION_NUM"}
	}
	replicatorStr := reqParams.Get("replicator")
	if replicatorStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC_REPLICATOR"}
	}
	replicator, err := GetValidReplicator(replicatorStr)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_REPLICATOR"}
	}

	suggestLFStr := reqParams.Get("suggestload")
	if suggestLFStr == "" {
		suggestLFStr = "0"
	}
	suggestLF, err := GetValidSuggestLF(suggestLFStr)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_LOAD_FACTOR"}
	}
	syncEveryStr := reqParams.Get("syncdisk")
	if syncEveryStr == "" {
		syncEveryStr = "0"
	}
	syncEvery, err := strconv.Atoi(syncEveryStr)
	if err != nil {
		nsqlookupLog.Logf("error sync disk param: %v, %v", syncEvery, err)
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_SYNC_DISK"}
	}
	retentionDaysStr := reqParams.Get("retention")
	if retentionDaysStr == "" {
		retentionDaysStr = "0"
	}
	retentionDays, err := strconv.Atoi(retentionDaysStr)
	if err != nil {
		nsqlookupLog.Logf("error retention param: %v, %v", retentionDaysStr, err)
		return nil, http_api.Err{400, err.Error()}
	}
	allowMultiOrdered := reqParams.Get("orderedmulti")

	if !s.ctx.nsqlookupd.coordinator.IsMineLeader() {
		nsqlookupLog.LogDebugf("create topic (%s) from remote %v should request to leader", topicName, req.RemoteAddr)
		return nil, http_api.Err{400, consistence.ErrFailedOnNotLeader}
	}

	nsqlookupLog.Logf("creating topic(%s) with partition %v replicator: %v load: %v", topicName, pnum, replicator, suggestLF)

	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	meta := consistence.TopicMetaInfo{}
	meta.PartitionNum = pnum
	meta.Replica = replicator
	meta.SuggestLF = suggestLF
	meta.SyncEvery = syncEvery
	meta.RetentionDay = int32(retentionDays)
	if allowMultiOrdered == "true" {
		meta.OrderedMulti = true
	}
	err = s.ctx.nsqlookupd.coordinator.CreateTopic(topicName, meta)
	if err != nil {
		nsqlookupLog.LogErrorf("DB: adding topic(%s) failed: %v", topicName, err)
		return nil, http_api.Err{400, err.Error()}
	}

	return nil, nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	partStr := reqParams.Get("partition")
	if partStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC_PARTITION"}
	} else if partStr == "**" {
		nsqlookupLog.LogWarningf("removing all the partitions of topic: %v", topicName)
	} else {
		return nil, http_api.Err{400, "REMOVE_SINGLE_PARTITION_NOT_ALLOWED"}
	}
	force := reqParams.Get("force")

	nsqlookupLog.Logf("deleting topic(%s) with partition %v ", topicName, partStr)
	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	err = s.ctx.nsqlookupd.coordinator.DeleteTopic(topicName, partStr)
	if err != nil {
		nsqlookupLog.Logf("deleting topic(%s) with partition %v failed : %v", topicName, partStr, err)
		if force == "true" {
			err = s.ctx.nsqlookupd.coordinator.DeleteTopicForce(topicName, partStr)
			if err == nil {
				return nil, nil
			}
		}
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doChangeTopicPartitionNum(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	pnumStr := reqParams.Get("partition_num")
	if pnumStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC_PARTITION_NUM"}
	}
	pnum, err := GetValidPartitionNum(pnumStr)
	if err != nil {
		nsqlookupLog.Logf("invalid partition num: %v, %v", pnumStr, err)
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_PARTITION_NUM"}
	}

	err = s.ctx.nsqlookupd.coordinator.ExpandTopicPartition(topicName, pnum)
	if err != nil {
		return nil, http_api.Err{500, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doChangeTopicDynamicParam(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	replicatorStr := reqParams.Get("replicator")
	replicator := -1
	if replicatorStr != "" {
		replicator, err = GetValidReplicator(replicatorStr)
		if err != nil {
			return nil, http_api.Err{400, "INVALID_ARG_TOPIC_REPLICATOR"}
		}
	}

	syncEveryStr := reqParams.Get("syncdisk")
	syncEvery := -1
	if syncEveryStr != "" {
		syncEvery, err = strconv.Atoi(syncEveryStr)
		if err != nil {
			nsqlookupLog.Logf("error sync disk param: %v, %v", syncEvery, err)
			return nil, http_api.Err{400, "INVALID_ARG_TOPIC_SYNC_DISK"}
		}
	}
	retentionDaysStr := reqParams.Get("retention")
	retentionDays := -1
	if retentionDaysStr != "" {
		retentionDays, err = strconv.Atoi(retentionDaysStr)
		if err != nil {
			nsqlookupLog.Logf("error retention param: %v, %v", retentionDaysStr, err)
			return nil, http_api.Err{400, err.Error()}
		}
	}

	err = s.ctx.nsqlookupd.coordinator.ChangeTopicMetaParam(topicName, syncEvery, retentionDays, replicator)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	return nil, nil
}

func (s *httpServer) doMoveTopicParition(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	pStr := reqParams.Get("partition")
	if pStr == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC_PARTITION"}
	}
	pid, err := GetValidPartitionID(pStr)
	if err != nil {
		nsqlookupLog.Logf("invalid partition num: %v, %v", pStr, err)
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC_PARTITION_NUM"}
	}

	moveLeader := reqParams.Get("move_leader") == "true"
	fromNode := reqParams.Get("move_from")
	toNode := reqParams.Get("move_to")

	err = s.ctx.nsqlookupd.coordinator.MoveTopicPartitionDataByManual(topicName, pid, moveLeader, fromNode, toNode)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doClusterBeginUpgrade(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	err := s.ctx.nsqlookupd.coordinator.SetClusterUpgradeState(true)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doClusterFinishUpgrade(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	err := s.ctx.nsqlookupd.coordinator.SetClusterUpgradeState(false)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doRemoveClusterDataNode(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqlookupd.coordinator == nil {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	nid := reqParams.Get("remove_node")

	err = s.ctx.nsqlookupd.coordinator.MarkNodeAsRemoving(nid)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	return nil, nil
}

func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName := reqParams.Get("topic")
	if topicName == "" {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	node := reqParams.Get("node")
	if node == "" {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	restore := reqParams.Get("restore")
	nsqlookupLog.Logf("DB: setting tombstone for producer@%s of topic(%s), restore param: %v", node, topicName, restore)
	producerRegs := s.ctx.nsqlookupd.DB.FindTopicProducers(topicName, "*")
	for _, reg := range producerRegs {
		p := reg.ProducerNode
		if p.peerInfo == nil {
			continue
		}
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		if thisNode == node {
			if restore == "true" {
				nsqlookupLog.Logf("DB: undo tombstone producer %v, topic: %v:%v", p, topicName, reg.PartitionID)
				p.UndoTombstone()
			} else {
				nsqlookupLog.Logf("DB: setting tombstone  producer %v, topic: %v:%v", p, topicName, reg.PartitionID)
				p.Tombstone()
			}
		}
	}

	return nil, nil
}

type NodeStat struct {
	Hostname         string  `json:"hostname"`
	BroadcastAddress string  `json:"broadcast_address"`
	TCPPort          int     `json:"tcp_port"`
	HTTPPort         int     `json:"http_port"`
	LeaderLoadFactor float64 `json:"leader_load_factor"`
	NodeLoadFactor   float64 `json:"node_load_factor"`
}

type node struct {
	RemoteAddress    string              `json:"remote_address"`
	Hostname         string              `json:"hostname"`
	BroadcastAddress string              `json:"broadcast_address"`
	TCPPort          int                 `json:"tcp_port"`
	HTTPPort         int                 `json:"http_port"`
	Version          string              `json:"version"`
	Tombstones       []bool              `json:"tombstones"`
	Topics           []string            `json:"topics"`
	Partitions       map[string][]string `json:"partitions"`
}

// return all lookup nodes that registered on etcd, and mark the master/slave info
func (s *httpServer) doListLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	if s.ctx.nsqlookupd.coordinator != nil {
		nodes, err := s.ctx.nsqlookupd.coordinator.GetAllLookupdNodes()
		if err != nil {
			nsqlookupLog.Infof("list lookup error: %v", err)
			return nil, http_api.Err{500, err.Error()}
		}
		filteredNodes := nodes[:0]
		for _, n := range nodes {
			if !s.ctx.nsqlookupd.DB.IsTombstoneLookupdNode(n.GetID()) {
				filteredNodes = append(filteredNodes, n)
			}
		}
		leader := s.ctx.nsqlookupd.coordinator.GetLookupLeader()
		return map[string]interface{}{
			"lookupdnodes":  filteredNodes,
			"lookupdleader": leader,
		}, nil
	}
	return nil, nil
}

func (s *httpServer) doClusterTombstoneLookupd(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	node := reqParams.Get("node")
	if node == "" {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}
	restore := reqParams.Get("restore")
	if restore != "" {
		deleted := s.ctx.nsqlookupd.DB.DelTombstoneLookupdNode(node)
		if deleted {
			return nil, nil
		} else {
			return nil, http_api.Err{404, "lookup node id not found"}
		}
	}
	if s.ctx.nsqlookupd.coordinator != nil {
		nodes, err := s.ctx.nsqlookupd.coordinator.GetAllLookupdNodes()
		if err != nil {
			return nil, http_api.Err{500, err.Error()}
		}
		var peer PeerInfo
		for _, n := range nodes {
			if n.GetID() == node {
				peer.DistributedID = n.GetID()
				peer.BroadcastAddress = n.NodeIP
				break
			}
		}
		if peer.DistributedID == "" {
			return nil, http_api.Err{404, "lookup node id not found"}
		} else {
			s.ctx.nsqlookupd.DB.TombstoneLookupdNode(peer.DistributedID, peer)
		}
	} else {
		return nil, http_api.Err{500, "MISSING_COORDINATOR"}
	}
	return nil, nil
}

func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.GetAllPeerClients().FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout)
	nodes := make([]*node, len(producers))
	for i, p := range producers {
		regMap := s.ctx.nsqlookupd.DB.FindPeerTopics(p.Id)
		topics := make([]string, 0, len(regMap))
		partitions := make(map[string][]string)
		tombstones := make([]bool, len(regMap))
		j := 0
		for t, regs := range regMap {
			topics = append(topics, t)
			for _, reg := range regs {
				partitions[t] = append(partitions[t], reg.PartitionID)
				// for each topic find the producer that matches this peer
				// to add tombstone information
				if reg.ProducerNode.peerInfo.Id == p.Id {
					tombstones[j] = reg.ProducerNode.IsTombstoned()
				}
			}
			j++
		}

		nodes[i] = &node{
			RemoteAddress:    p.RemoteAddress,
			Hostname:         p.Hostname,
			BroadcastAddress: p.BroadcastAddress,
			TCPPort:          p.TCPPort,
			HTTPPort:         p.HTTPPort,
			Version:          p.Version,
			Tombstones:       tombstones,
			Topics:           topics,
			Partitions:       partitions,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.ctx.nsqlookupd.DB.RLock()
	defer s.ctx.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for topic, topicRegs := range s.ctx.nsqlookupd.DB.registrationTopicMap {
		key := "topic" + ":" + topic
		for _, reg := range topicRegs {
			p := reg.ProducerNode
			m := map[string]interface{}{
				"partitionID":       reg.PartitionID,
				"id":                p.peerInfo.Id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"tcp_port":          p.peerInfo.TCPPort,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			data[key] = append(data[key], m)
		}
	}

	for topic, regs := range s.ctx.nsqlookupd.DB.registrationChannelMap {
		for _, reg := range regs {
			key := "channel" + ":" + topic + ":" + reg.PartitionID
			m := map[string]interface{}{
				"partitionID": reg.PartitionID,
				"channelName": reg.Channel,
				"peerId":      reg.PeerId,
			}
			data[key] = append(data[key], m)
		}
	}

	for id, p := range s.ctx.nsqlookupd.DB.registrationNodeMap {
		key := "peerInfo:" + id
		m := map[string]interface{}{
			"id":                p.Id,
			"hostname":          p.Hostname,
			"broadcast_address": p.BroadcastAddress,
			"tcp_port":          p.TCPPort,
			"http_port":         p.HTTPPort,
			"version":           p.Version,
			"last_update":       atomic.LoadInt64(&p.lastUpdate),
		}
		data[key] = append(data[key], m)
	}
	return data, nil
}
