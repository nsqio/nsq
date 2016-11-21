package consistence

import (
	"errors"
	"github.com/cenkalti/backoff"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrAlreadyExist         = errors.New("already exist")
	ErrTopicNotCreated      = errors.New("topic is not created")
	ErrWaitingLeaderRelease = errors.New("leader session is still alive")
	ErrNotNsqLookupLeader   = errors.New("Not nsqlookup leader")
	ErrClusterUnstable      = errors.New("the cluster is unstable")

	ErrLeaderNodeLost           = NewCoordErr("leader node is lost", CoordTmpErr)
	ErrNodeNotFound             = NewCoordErr("node not found", CoordCommonErr)
	ErrLeaderElectionFail       = NewCoordErr("Leader election failed.", CoordElectionTmpErr)
	ErrNoLeaderCanBeElected     = NewCoordErr("No leader can be elected", CoordElectionTmpErr)
	ErrNodeUnavailable          = NewCoordErr("No node is available for topic", CoordTmpErr)
	ErrJoinISRInvalid           = NewCoordErr("Join ISR failed", CoordCommonErr)
	ErrJoinISRTimeout           = NewCoordErr("Join ISR timeout", CoordCommonErr)
	ErrWaitingJoinISR           = NewCoordErr("The topic is waiting node to join isr", CoordCommonErr)
	ErrLeaderSessionNotReleased = NewCoordErr("The topic leader session is not released", CoordElectionTmpErr)
	ErrTopicISRCatchupEnough    = NewCoordErr("the topic isr and catchup nodes are enough", CoordTmpErr)
	ErrClusterNodeRemoving      = NewCoordErr("the node is mark as removed", CoordTmpErr)
	ErrTopicNodeConflict        = NewCoordErr("the topic node info is conflicted", CoordElectionErr)
)

const (
	waitMigrateInterval = time.Minute * 10
)

type JoinISRState struct {
	sync.Mutex
	waitingJoin      bool
	waitingSession   string
	waitingStart     time.Time
	readyNodes       map[string]struct{}
	doneChan         chan struct{}
	isLeadershipWait bool
}

type RpcFailedInfo struct {
	nodeID    string
	topic     string
	partition int
	failTime  time.Time
}

func getOthersExceptLeader(topicInfo *TopicPartitionMetaInfo) []string {
	others := make([]string, 0, len(topicInfo.ISR)+len(topicInfo.CatchupList)-1)
	for _, n := range topicInfo.ISR {
		if n == topicInfo.Leader {
			continue
		}
		others = append(others, n)
	}
	others = append(others, topicInfo.CatchupList...)
	return others
}

type TopicNameInfo struct {
	TopicName      string
	TopicPartition int
}

type Options struct {
	BalanceStart int
	BalanceEnd   int
}

type NsqLookupCoordinator struct {
	clusterKey         string
	myNode             NsqLookupdNodeInfo
	leaderNode         NsqLookupdNodeInfo
	leadership         NSQLookupdLeadership
	nodesMutex         sync.RWMutex
	nsqdNodes          map[string]NsqdNodeInfo
	removingNodes      map[string]string
	nodesEpoch         int64
	rpcMutex           sync.RWMutex
	nsqdRpcClients     map[string]*NsqdRpcClient
	checkTopicFailChan chan TopicNameInfo
	stopChan           chan struct{}
	joinStateMutex     sync.Mutex
	joinISRState       map[string]*JoinISRState
	failedRpcMutex     sync.Mutex
	failedRpcList      []RpcFailedInfo
	nsqlookupRpcServer *NsqLookupCoordRpcServer
	wg                 sync.WaitGroup
	nsqdMonitorChan    chan struct{}
	isClusterUnstable  int32
	dpm                *DataPlacement
	balanceWaiting     int32
}

func NewNsqLookupCoordinator(cluster string, n *NsqLookupdNodeInfo, opts *Options) *NsqLookupCoordinator {
	coord := &NsqLookupCoordinator{
		clusterKey:         cluster,
		myNode:             *n,
		leadership:         nil,
		nsqdNodes:          make(map[string]NsqdNodeInfo),
		removingNodes:      make(map[string]string),
		nsqdRpcClients:     make(map[string]*NsqdRpcClient),
		checkTopicFailChan: make(chan TopicNameInfo, 3),
		stopChan:           make(chan struct{}),
		joinISRState:       make(map[string]*JoinISRState),
		failedRpcList:      make([]RpcFailedInfo, 0),
		nsqdMonitorChan:    make(chan struct{}),
	}
	if coord.leadership != nil {
		coord.leadership.InitClusterID(coord.clusterKey)
	}
	coord.nsqlookupRpcServer = NewNsqLookupCoordRpcServer(coord)
	coord.dpm = NewDataPlacement(coord)
	if opts != nil {
		coord.dpm.SetBalanceInterval(opts.BalanceStart, opts.BalanceEnd)
	}
	return coord
}

func (self *NsqLookupCoordinator) SetLeadershipMgr(l NSQLookupdLeadership) {
	self.leadership = l
	if self.leadership != nil {
		self.leadership.InitClusterID(self.clusterKey)
	}
}

func RetryWithTimeout(fn func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = time.Second * 30
	bo.MaxInterval = time.Second * 5
	return backoff.Retry(fn, bo)
}

// init and register to leader server
func (self *NsqLookupCoordinator) Start() error {
	if self.leadership != nil {
		err := self.leadership.Register(&self.myNode)
		if err != nil {
			coordLog.Warningf("failed to register nsqlookup coordinator: %v", err)
			return err
		}
	}
	self.wg.Add(1)
	go self.handleLeadership()
	go self.nsqlookupRpcServer.start(self.myNode.NodeIP, self.myNode.RpcPort)
	self.notifyNodesLookup()
	return nil
}

func (self *NsqLookupCoordinator) Stop() {
	close(self.stopChan)
	self.leadership.Unregister(&self.myNode)
	self.leadership.Stop()
	// TODO: exit should avoid while test.
	self.nsqlookupRpcServer.stop()
	for _, c := range self.nsqdRpcClients {
		c.Close()
	}
	self.wg.Wait()
	coordLog.Infof("nsqlookup coordinator stopped.")
}

func (self *NsqLookupCoordinator) notifyNodesLookup() {
	nodes, err := self.leadership.GetNsqdNodes()
	if err != nil {
		return
	}

	for _, node := range nodes {
		client, err := NewNsqdRpcClient(net.JoinHostPort(node.NodeIP, node.RpcPort), RPC_TIMEOUT_FOR_LOOKUP)
		if err != nil {
			coordLog.Infof("rpc node %v client init failed : %v", node, err)
			continue
		}
		client.TriggerLookupChanged()
		client.Close()
	}
}

func (self *NsqLookupCoordinator) handleLeadership() {
	defer self.wg.Done()
	lookupdLeaderChan := make(chan *NsqLookupdNodeInfo)
	if self.leadership != nil {
		go self.leadership.AcquireAndWatchLeader(lookupdLeaderChan, self.stopChan)
	}
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			buf = buf[0:n]
			coordLog.Errorf("panic %s:%v", buf, e)
		}

		coordLog.Warningf("leadership watch exit.")
		time.Sleep(time.Second)
		if self.nsqdMonitorChan != nil {
			close(self.nsqdMonitorChan)
			self.nsqdMonitorChan = nil
		}
	}()
	for {
		select {
		case l, ok := <-lookupdLeaderChan:
			if !ok {
				coordLog.Warningf("leader chan closed.")
				return
			}
			if l == nil {
				coordLog.Warningln("leader is lost.")
				continue
			}
			if l.GetID() != self.leaderNode.GetID() ||
				l.Epoch != self.leaderNode.Epoch {
				coordLog.Infof("lookup leader changed from %v to %v", self.leaderNode, *l)
				self.leaderNode = *l
				if self.leaderNode.GetID() != self.myNode.GetID() {
					// remove watchers.
					if self.nsqdMonitorChan != nil {
						close(self.nsqdMonitorChan)
					}
					self.nsqdMonitorChan = make(chan struct{})
				}
				self.notifyLeaderChanged(self.nsqdMonitorChan)
			}
			if self.leaderNode.GetID() == "" {
				coordLog.Warningln("leader is missing.")
			}
		}
	}
}

func (self *NsqLookupCoordinator) notifyLeaderChanged(monitorChan chan struct{}) {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("I am slave (%v). Leader is: %v", self.myNode, self.leaderNode)
		self.nodesMutex.Lock()
		self.removingNodes = make(map[string]string)
		self.nodesMutex.Unlock()
		return
	}
	coordLog.Infof("I am master now.")
	// reload topic information
	if self.leadership != nil {
		newTopics, err := self.leadership.ScanTopics()
		if err != nil {
			coordLog.Errorf("load topic info failed: %v", err)
		} else {
			coordLog.Infof("topic loaded : %v", len(newTopics))
			self.notifyTopicsToAllNsqdForReload(newTopics)
		}
	}

	// we do not need to watch each topic leader,
	// we can make sure the leader on the alive node is alive.
	// so we bind all the topic leader session on the alive node session
	//self.wg.Add(1)
	//go func() {
	//	defer self.wg.Done()
	//	self.watchTopicLeaderSession(monitorChan)
	//}()

	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.handleNsqdNodes(monitorChan)
	}()
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.checkTopics(monitorChan)
	}()
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.rpcFailRetryFunc(monitorChan)
	}()
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.dpm.DoBalance(monitorChan)
	}()
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		self.handleRemovingNodes(monitorChan)
	}()
}

// for the nsqd node that temporally lost, we need send the related topics to
// it .
func (self *NsqLookupCoordinator) notifyTopicsToSingleNsqdForReload(topics []TopicPartitionMetaInfo, nodeID string) {
	coordLog.Infof("reload topics for node: %v", nodeID)
	for _, v := range topics {
		select {
		case <-self.stopChan:
			return
		default:
		}
		if FindSlice(v.ISR, nodeID) != -1 || FindSlice(v.CatchupList, nodeID) != -1 {
			self.notifySingleNsqdForTopicReload(v, nodeID)
		}
	}
}

func (self *NsqLookupCoordinator) notifyTopicsToAllNsqdForReload(topics []TopicPartitionMetaInfo) {
	for _, v := range topics {
		select {
		case <-self.stopChan:
			return
		default:
		}

		self.notifyAllNsqdsForTopicReload(v)
	}
}

func (self *NsqLookupCoordinator) getCurrentNodes() map[string]NsqdNodeInfo {
	self.nodesMutex.RLock()
	currentNodes := self.nsqdNodes
	if len(self.removingNodes) > 0 {
		currentNodes = make(map[string]NsqdNodeInfo)
		for nid, n := range self.nsqdNodes {
			if _, ok := self.removingNodes[nid]; ok {
				continue
			}
			currentNodes[nid] = n
		}
	}
	self.nodesMutex.RUnlock()
	return currentNodes
}

func (self *NsqLookupCoordinator) getCurrentNodesWithRemoving() (map[string]NsqdNodeInfo, int64) {
	self.nodesMutex.RLock()
	currentNodes := self.nsqdNodes
	currentNodesEpoch := atomic.LoadInt64(&self.nodesEpoch)
	self.nodesMutex.RUnlock()
	return currentNodes, currentNodesEpoch
}

func (self *NsqLookupCoordinator) getCurrentNodesWithEpoch() (map[string]NsqdNodeInfo, int64) {
	self.nodesMutex.RLock()
	currentNodes := self.nsqdNodes
	if len(self.removingNodes) > 0 {
		currentNodes = make(map[string]NsqdNodeInfo)
		for nid, n := range self.nsqdNodes {
			if _, ok := self.removingNodes[nid]; ok {
				continue
			}
			currentNodes[nid] = n
		}
	}
	currentNodesEpoch := atomic.LoadInt64(&self.nodesEpoch)
	self.nodesMutex.RUnlock()
	return currentNodes, currentNodesEpoch
}

func (self *NsqLookupCoordinator) handleNsqdNodes(monitorChan chan struct{}) {
	nsqdNodesChan := make(chan []NsqdNodeInfo)
	if self.leadership != nil {
		go self.leadership.WatchNsqdNodes(nsqdNodesChan, monitorChan)
	}
	coordLog.Debugf("start watch the nsqd nodes.")
	defer func() {
		coordLog.Infof("stop watch the nsqd nodes.")
	}()
	for {
		select {
		case nodes, ok := <-nsqdNodesChan:
			if !ok {
				return
			}
			// check if any nsqd node changed.
			coordLog.Debugf("Current nsqd nodes: %v", len(nodes))
			oldNodes := self.nsqdNodes
			newNodes := make(map[string]NsqdNodeInfo)
			for _, v := range nodes {
				//coordLog.Infof("nsqd node %v : %v", v.GetID(), v)
				newNodes[v.GetID()] = v
			}
			self.nodesMutex.Lock()
			self.nsqdNodes = newNodes
			check := false
			for oldID, oldNode := range oldNodes {
				if _, ok := newNodes[oldID]; !ok {
					coordLog.Warningf("nsqd node failed: %v, %v", oldID, oldNode)
					// if node is missing we need check election immediately.
					check = true
				}
			}
			// failed need be protected by lock so we can avoid contention.
			if check {
				atomic.AddInt64(&self.nodesEpoch, 1)
			}
			self.nodesMutex.Unlock()

			if self.leadership == nil {
				continue
			}
			topics, scanErr := self.leadership.ScanTopics()
			if scanErr != nil {
				coordLog.Infof("scan topics failed: %v", scanErr)
			}
			for newID, newNode := range newNodes {
				if _, ok := oldNodes[newID]; !ok {
					coordLog.Infof("new nsqd node joined: %v, %v", newID, newNode)
					// notify the nsqd node to recheck topic info.(for
					// temp lost)
					if scanErr == nil {
						self.notifyTopicsToSingleNsqdForReload(topics, newID)
					}
					check = true
				}
			}
			if check {
				atomic.AddInt64(&self.nodesEpoch, 1)
				atomic.StoreInt32(&self.isClusterUnstable, 1)
				self.triggerCheckTopics("", 0, time.Millisecond*10)
			}
		}
	}
}

func (self *NsqLookupCoordinator) handleRemovingNodes(monitorChan chan struct{}) {
	coordLog.Debugf("start handle the removing nsqd nodes.")
	defer func() {
		coordLog.Infof("stop handle the removing nsqd nodes.")
	}()
	ticker := time.NewTicker(time.Second * 30)
	nodeTopicStats := make([]NodeTopicStats, 0, 10)
	defer ticker.Stop()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			//
			anyStateChanged := false
			self.nodesMutex.RLock()
			removingNodes := make(map[string]string)
			for nid, removeState := range self.removingNodes {
				removingNodes[nid] = removeState
			}
			self.nodesMutex.RUnlock()
			// remove state: marked -> pending -> data_transfered -> done
			if len(removingNodes) == 0 {
				continue
			}
			currentNodes := self.getCurrentNodes()
			allTopics, err := self.leadership.ScanTopics()
			if err != nil {
				continue
			}
			nodeTopicStats = nodeTopicStats[:0]
			for nodeID, nodeInfo := range currentNodes {
				topicStat, err := self.getNsqdTopicStat(nodeInfo)
				if err != nil {
					coordLog.Infof("failed to get node topic status : %v", nodeID)
					continue
				}
				nodeTopicStats = append(nodeTopicStats, *topicStat)
			}
			leaderSort := func(l, r *NodeTopicStats) bool {
				return l.LeaderLessLoader(r)
			}
			By(leaderSort).Sort(nodeTopicStats)

			for nid, _ := range removingNodes {
				anyPending := false
				coordLog.Infof("handle the removing node %v ", nid)
				// only check the topic with one replica left
				// because the doCheckTopics will check the others
				// we add a new replica for the removing node
				for _, topicInfo := range allTopics {
					if FindSlice(topicInfo.ISR, nid) == -1 {
						continue
					}
					if len(topicInfo.ISR) <= topicInfo.Replica {
						anyPending = true
						// find new catchup and wait isr ready
						removingNodes[nid] = "pending"
						err := self.dpm.addToCatchupAndWaitISRReady(monitorChan, topicInfo.Name, topicInfo.Partition,
							"", nodeTopicStats, true)
						if err != nil {
							coordLog.Infof("topic %v data on node %v transfered failed, waiting next time", topicInfo.GetTopicDesp(), nid)
							continue
						}
						coordLog.Infof("topic %v data on node %v transfered success", topicInfo.GetTopicDesp(), nid)
						anyStateChanged = true
					}
					if topicInfo.Leader == nid {
						self.handleMoveTopic(true, topicInfo.Name, topicInfo.Partition, nid)
					} else {
						self.handleMoveTopic(false, topicInfo.Name, topicInfo.Partition, nid)
					}
				}
				if !anyPending {
					anyStateChanged = true
					coordLog.Infof("node %v data has been transfered, it can be removed from cluster: state: %v", nid, removingNodes[nid])
					if removingNodes[nid] != "data_transfered" && removingNodes[nid] != "done" {
						removingNodes[nid] = "data_transfered"
					} else {
						if removingNodes[nid] == "data_transfered" {
							removingNodes[nid] = "done"
						} else if removingNodes[nid] == "done" {
							self.nodesMutex.Lock()
							_, ok := self.nsqdNodes[nid]
							if !ok {
								delete(removingNodes, nid)
								coordLog.Infof("the node %v is removed finally since not alive in cluster", nid)
							}
							self.nodesMutex.Unlock()
						}
					}
				}
			}

			if anyStateChanged {
				self.nodesMutex.Lock()
				self.removingNodes = removingNodes
				self.nodesMutex.Unlock()
			}
		}
	}
}

func (self *NsqLookupCoordinator) watchTopicLeaderSession(monitorChan chan struct{}) {
	leaderChan := make(chan *TopicLeaderSession, 1)
	// close monitor channel should cause the leaderChan closed, so we can quit normally
	if self.leadership != nil {
		go self.leadership.WatchTopicLeader(leaderChan, monitorChan)
	}
	defer func() {
		coordLog.Infof("stop watch the topic leader session.")
	}()

	coordLog.Infof("begin watching leader session")
	for {
		select {
		case n, ok := <-leaderChan:
			if !ok {
				return
			}
			if n == nil {
				coordLog.Warningf("got nil topic leader session")
			} else if n.LeaderNode == nil {
				// try do election for topic
				self.triggerCheckTopics("", 0, time.Millisecond)
				coordLog.Warningf("topic leader is missing: %v", n)
				atomic.StoreInt32(&self.isClusterUnstable, 1)
			} else {
				coordLog.Warningf("topic leader session changed : %v, %v", n, n.LeaderNode)
				atomic.StoreInt32(&self.isClusterUnstable, 1)
				go self.revokeEnableTopicWrite(n.Topic, n.Partition, true)
			}
		}
	}
}

func (self *NsqLookupCoordinator) triggerCheckTopics(topic string, part int, delay time.Duration) {
	time.Sleep(delay)

	select {
	case self.checkTopicFailChan <- TopicNameInfo{topic, part}:
	case <-self.stopChan:
		return
	case <-time.After(time.Second * 3):
		return
	}
}

// check if partition is enough,
// check if replication is enough
// check any unexpected state.
func (self *NsqLookupCoordinator) checkTopics(monitorChan chan struct{}) {
	ticker := time.NewTicker(time.Second * 30)
	waitingMigrateTopic := make(map[string]map[int]time.Time)
	lostLeaderSessions := make(map[string]bool)
	defer func() {
		ticker.Stop()
		coordLog.Infof("check topics quit.")
	}()

	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			if self.leadership == nil {
				continue
			}
			topics, commonErr := self.leadership.ScanTopics()
			if commonErr != nil {
				coordLog.Infof("scan topics failed. %v", commonErr)
				continue
			}
			coordLog.Debugf("scan found topics: %v", topics)
			self.doCheckTopics(monitorChan, topics, waitingMigrateTopic, lostLeaderSessions, true)
		case failedInfo := <-self.checkTopicFailChan:
			if self.leadership == nil {
				continue
			}
			topics := []TopicPartitionMetaInfo{}
			var err error
			if failedInfo.TopicName == "" {
				topics, err = self.leadership.ScanTopics()
				if err != nil {
					coordLog.Infof("scan topics failed. %v", err)
					continue
				}
			} else {
				coordLog.Infof("check single topic : %v ", failedInfo)
				var t *TopicPartitionMetaInfo
				t, err = self.leadership.GetTopicInfo(failedInfo.TopicName, failedInfo.TopicPartition)
				if err != nil {
					coordLog.Infof("get topic info failed: %v, %v", failedInfo, err)
					continue
				}
				topics = append(topics, *t)
			}
			self.doCheckTopics(monitorChan, topics, waitingMigrateTopic, lostLeaderSessions, failedInfo.TopicName == "")
		}
	}
}

func (self *NsqLookupCoordinator) doCheckTopics(monitorChan chan struct{}, topics []TopicPartitionMetaInfo,
	waitingMigrateTopic map[string]map[int]time.Time, lostLeaderSessions map[string]bool, fullCheck bool) {
	coordLog.Infof("do check topics...")
	// TODO: check partition number for topic, maybe failed to create
	// some partition when creating topic.
	currentNodes, currentNodesEpoch := self.getCurrentNodesWithRemoving()
	checkOK := true
	for _, t := range topics {
		if currentNodesEpoch != atomic.LoadInt64(&self.nodesEpoch) {
			coordLog.Infof("nodes changed while checking topics: %v, %v", currentNodesEpoch, atomic.LoadInt64(&self.nodesEpoch))
			return
		}
		select {
		case <-monitorChan:
			// exiting
			return
		default:
		}

		needMigrate := false
		if len(t.ISR) < t.Replica {
			coordLog.Infof("ISR is not enough for topic %v, isr is :%v", t.GetTopicDesp(), t.ISR)
			needMigrate = true
			checkOK = false
		}

		self.joinStateMutex.Lock()
		state, ok := self.joinISRState[t.Name]
		self.joinStateMutex.Unlock()
		if ok && state != nil {
			state.Lock()
			wj := state.waitingJoin
			state.Unlock()
			if wj {
				checkOK = false
				continue
			}
		}

		aliveCount := 0
		failedNodes := make([]string, 0)
		for _, replica := range t.ISR {
			if _, ok := currentNodes[replica]; !ok {
				coordLog.Warningf("topic %v isr node %v is lost.", t.GetTopicDesp(), replica)
				needMigrate = true
				checkOK = false
				if replica != t.Leader {
					// leader fail will be handled while leader election.
					failedNodes = append(failedNodes, replica)
				}
			} else {
				aliveCount++
			}
		}
		if currentNodesEpoch != atomic.LoadInt64(&self.nodesEpoch) {
			coordLog.Infof("nodes changed while checking topics: %v, %v", currentNodesEpoch, atomic.LoadInt64(&self.nodesEpoch))
			return
		}
		// should copy to avoid reused
		topicInfo := t
		// handle remove this node from ISR
		coordErr := self.handleRemoveFailedISRNodes(failedNodes, &topicInfo)
		if coordErr != nil {
			go self.triggerCheckTopics(t.Name, t.Partition, time.Second*2)
			continue
		}

		if _, ok := currentNodes[t.Leader]; !ok {
			needMigrate = true
			checkOK = false
			coordLog.Warningf("topic %v leader %v is lost.", t.GetTopicDesp(), t.Leader)
			aliveNodes, aliveEpoch := self.getCurrentNodesWithEpoch()
			if aliveEpoch != currentNodesEpoch {
				continue
			}
			coordErr := self.handleTopicLeaderElection(&topicInfo, aliveNodes, aliveEpoch, false)
			if coordErr != nil {
				coordLog.Warningf("topic leader election failed: %v", coordErr)
			}
			continue
		} else {
			// check topic leader session key.
			var leaderSession *TopicLeaderSession
			var err error
			retry := 0
			for retry < 3 {
				retry++
				leaderSession, err = self.leadership.GetTopicLeaderSession(t.Name, t.Partition)
				if err != nil {
					coordLog.Infof("topic %v leader session failed to get: %v", t.GetTopicDesp(), err)
					// notify the nsqd node to acquire the leader session.
					self.notifyISRTopicMetaInfo(&topicInfo)
					self.notifyAcquireTopicLeader(&topicInfo)
					time.Sleep(time.Millisecond * 100)
					continue
				} else {
					break
				}
			}
			if leaderSession == nil {
				checkOK = false
				lostLeaderSessions[t.GetTopicDesp()] = true
				continue
			}
			if leaderSession.LeaderNode == nil || leaderSession.Session == "" {
				checkOK = false
				lostLeaderSessions[t.GetTopicDesp()] = true
				coordLog.Infof("topic %v leader session node is missing.", t.GetTopicDesp())
				self.notifyISRTopicMetaInfo(&topicInfo)
				self.notifyAcquireTopicLeader(&topicInfo)
				continue
			}
			if leaderSession.LeaderNode.ID != t.Leader {
				checkOK = false
				lostLeaderSessions[t.GetTopicDesp()] = true
				coordLog.Warningf("topic %v leader session mismatch: %v, %v", t.GetTopicDesp(), leaderSession, t.Leader)
				tmpTopicInfo := t
				tmpTopicInfo.Leader = leaderSession.LeaderNode.ID
				self.notifyReleaseTopicLeader(&tmpTopicInfo, leaderSession.LeaderEpoch)
				self.notifyISRTopicMetaInfo(&topicInfo)
				self.notifyAcquireTopicLeader(&topicInfo)
				continue
			}
		}

		partitions, ok := waitingMigrateTopic[t.Name]
		if !ok {
			partitions = make(map[int]time.Time)
			waitingMigrateTopic[t.Name] = partitions
		}

		if needMigrate {
			if _, ok := partitions[t.Partition]; !ok {
				partitions[t.Partition] = time.Now()
			}
			if (aliveCount <= t.Replica/2) ||
				partitions[t.Partition].Before(time.Now().Add(-1*waitMigrateInterval)) {
				coordLog.Infof("begin migrate the topic :%v", t.GetTopicDesp())
				aliveNodes, aliveEpoch := self.getCurrentNodesWithEpoch()
				if aliveEpoch != currentNodesEpoch {
					go self.triggerCheckTopics(t.Name, t.Partition, time.Second)
					continue
				}
				self.handleTopicMigrate(&topicInfo, aliveNodes, aliveEpoch)
				delete(partitions, t.Partition)
			} else {
				coordLog.Infof("waiting migrate the topic :%v since time: %v", t.GetTopicDesp(), partitions[t.Partition])
			}
		} else {
			delete(partitions, t.Partition)
		}
		// check if the topic write disabled, and try enable if possible. There is a chance to
		// notify the topic enable write with failure, which may cause the write state is not ok.
		if ok && state != nil {
			state.Lock()
			wj := state.waitingJoin
			state.Unlock()
			if wj {
				checkOK = false
				continue
			}
		}

		if currentNodesEpoch != atomic.LoadInt64(&self.nodesEpoch) {
			coordLog.Infof("nodes changed while checking topics: %v, %v", currentNodesEpoch, atomic.LoadInt64(&self.nodesEpoch))
			return
		}
		// check if write disabled
		if self.isTopicWriteDisabled(&topicInfo) {
			coordLog.Infof("the topic write is disabled but not in waiting join state: %v", t)
			checkOK = false
			go self.revokeEnableTopicWrite(t.Name, t.Partition, true)
		} else {
			if _, ok := lostLeaderSessions[t.GetTopicDesp()]; ok {
				coordLog.Infof("notify %v topic leadership since lost before ", t.GetTopicDesp())
				leaderSession, err := self.leadership.GetTopicLeaderSession(t.Name, t.Partition)
				if err != nil {
					coordLog.Infof("failed to get topic %v leader session: %v", t.GetTopicDesp(), err)
				} else {
					self.notifyTopicLeaderSession(&topicInfo, leaderSession, "")
					delete(lostLeaderSessions, t.GetTopicDesp())
				}
			}
			if aliveCount > t.Replica && atomic.LoadInt32(&self.balanceWaiting) == 0 {
				//remove the unwanted node in isr
				coordLog.Infof("isr is more than replicator: %v, %v", aliveCount, t.Replica)
				failedNodes := make([]string, 0, 1)
				maxLF := 0.0
				removeNode := ""
				for _, nodeID := range t.ISR {
					if nodeID == t.Leader {
						continue
					}
					n, ok := currentNodes[nodeID]
					if !ok {
						continue
					}
					stat, err := self.getNsqdTopicStat(n)
					if err != nil {
						continue
					}
					_, nlf := stat.GetNodeLoadFactor()
					if nlf > maxLF {
						maxLF = nlf
						removeNode = nodeID
					}
				}
				if removeNode != "" {
					failedNodes = append(failedNodes, removeNode)
					coordErr := self.handleRemoveISRNodes(failedNodes, &topicInfo, false)
					if coordErr == nil {
						coordLog.Infof("node %v removed by plan from topic : %v", failedNodes, t)
					}
				}
			}
		}
	}
	if checkOK {
		if fullCheck {
			atomic.StoreInt32(&self.isClusterUnstable, 0)
		}
	} else {
		atomic.StoreInt32(&self.isClusterUnstable, 1)
	}
}

func (self *NsqLookupCoordinator) handleTopicLeaderElection(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo,
	currentNodesEpoch int64, isOldLeaderAlive bool) *CoordErr {
	_, leaderSession, state, coordErr := self.prepareJoinState(topicInfo.Name, topicInfo.Partition, false)
	if coordErr != nil {
		coordLog.Infof("prepare join state failed: %v", coordErr)
		return coordErr
	}
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("failed because another is waiting join: %v", state)
		return ErrLeavingISRWait
	}
	defer func() {
		go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
	}()

	if currentNodesEpoch != atomic.LoadInt64(&self.nodesEpoch) {
		return ErrClusterChanged
	}
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}
	state.waitingJoin = false
	state.waitingSession = ""

	newTopicInfo, err := self.leadership.GetTopicInfo(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		return &CoordErr{err.Error(), RpcNoErr, CoordNetErr}
	}
	if topicInfo.Epoch != newTopicInfo.Epoch {
		return ErrClusterChanged
	}

	coordErr = self.notifyLeaderDisableTopicWriteFast(topicInfo)
	if coordErr != nil {
		coordLog.Infof("disable write failed while elect leader: %v", coordErr)
		// the leader maybe down, so we can ignore this error safely.
	}
	coordErr = self.notifyISRDisableTopicWrite(topicInfo)
	if coordErr != nil {
		coordLog.Infof("failed notify disable write while election: %v", coordErr)
		return coordErr
	}

	if currentNodesEpoch != atomic.LoadInt64(&self.nodesEpoch) {
		return ErrClusterChanged
	}
	// choose another leader in ISR list, and add new node to ISR
	// list.
	newLeader, newestLogID, coordErr := self.dpm.chooseNewLeaderFromISR(topicInfo, currentNodes)
	if coordErr != nil {
		return coordErr
	}

	if leaderSession != nil {
		// notify old leader node to release leader
		self.notifyReleaseTopicLeader(topicInfo, leaderSession.LeaderEpoch)
	}

	err = self.waitOldLeaderRelease(topicInfo)
	if err != nil {
		coordLog.Infof("Leader is not released: %v", topicInfo)
		return ErrLeaderSessionNotReleased
	}
	coordLog.Infof("topic %v leader election result: %v", topicInfo, newLeader)
	coordErr = self.makeNewTopicLeaderAcknowledged(topicInfo, newLeader, newestLogID, isOldLeaderAlive)
	if coordErr != nil {
		return coordErr
	}

	return nil
}

func (self *NsqLookupCoordinator) handleRemoveISRNodes(failedNodes []string, topicInfo *TopicPartitionMetaInfo, leaveCatchup bool) *CoordErr {
	self.joinStateMutex.Lock()
	state, ok := self.joinISRState[topicInfo.Name]
	if !ok {
		state = &JoinISRState{}
		self.joinISRState[topicInfo.Name] = state
	}
	self.joinStateMutex.Unlock()
	state.Lock()
	defer state.Unlock()

	wj := state.waitingJoin
	if wj {
		coordLog.Infof("isr node is waiting for join session %v, removing should wait.", state.waitingSession)
		return ErrLeavingISRWait
	}

	if len(failedNodes) == 0 {
		return nil
	}
	newISR := FilterList(topicInfo.ISR, failedNodes)
	if len(newISR) == 0 {
		coordLog.Infof("no node left in isr if removing failed")
		return nil
	}
	topicInfo.ISR = newISR
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node while removing the failed nodes. %v", topicInfo.ISR)
		if !leaveCatchup {
			return ErrLeavingISRWait
		}
	}
	if leaveCatchup {
		topicInfo.CatchupList = MergeList(topicInfo.CatchupList, failedNodes)
	} else {
		topicInfo.CatchupList = FilterList(topicInfo.CatchupList, failedNodes)
	}
	coordLog.Infof("topic info updated: %v", topicInfo)
	// remove isr node we keep the write epoch unchanged.
	err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, &topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update topic node isr failed: %v", err.Error())
		return &CoordErr{err.Error(), RpcNoErr, CoordNetErr}
	}
	go self.notifyTopicMetaInfo(topicInfo)
	return nil
}

func (self *NsqLookupCoordinator) handleRemoveFailedISRNodes(failedNodes []string, topicInfo *TopicPartitionMetaInfo) *CoordErr {
	return self.handleRemoveISRNodes(failedNodes, topicInfo, true)
}

func (self *NsqLookupCoordinator) handleTopicMigrate(topicInfo *TopicPartitionMetaInfo,
	currentNodes map[string]NsqdNodeInfo, currentNodesEpoch int64) {
	if currentNodesEpoch != atomic.LoadInt64(&self.nodesEpoch) {
		return
	}
	if _, ok := currentNodes[topicInfo.Leader]; !ok {
		coordLog.Warningf("topic leader node is down: %v", topicInfo)
		return
	}
	isrChanged := false
	for _, replica := range topicInfo.ISR {
		if _, ok := currentNodes[replica]; !ok {
			coordLog.Warningf("topic %v isr node %v is lost.", topicInfo.GetTopicDesp(), replica)
			isrChanged = true
		}
	}
	if isrChanged {
		// will re-check to handle isr node failure
		return
	}

	catchupChanged := false
	aliveCatchup := 0
	for _, n := range topicInfo.CatchupList {
		if _, ok := currentNodes[n]; ok {
			aliveCatchup++
		} else {
			coordLog.Infof("topic %v catchup node %v is lost.", topicInfo.GetTopicDesp(), n)
		}
	}
	topicNsqdNum := len(topicInfo.ISR) + aliveCatchup
	if topicNsqdNum < topicInfo.Replica {
		for i := topicNsqdNum; i < topicInfo.Replica; i++ {
			// should exclude the current isr and catchup node
			n, err := self.dpm.allocNodeForTopic(topicInfo, currentNodes)
			if err != nil {
				coordLog.Infof("failed to get a new catchup for topic: %v", topicInfo.GetTopicDesp())
			} else {
				topicInfo.CatchupList = append(topicInfo.CatchupList, n.GetID())
				catchupChanged = true
			}
		}
	}
	if catchupChanged {
		err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
			&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic node info failed: %v", err.Error())
			return
		}
		self.notifyTopicMetaInfo(topicInfo)
	} else {
		self.notifyCatchupTopicMetaInfo(topicInfo)
	}
}

func (self *NsqLookupCoordinator) addCatchupNode(topicInfo *TopicPartitionMetaInfo, nid string) *CoordErr {
	catchupChanged := false
	if FindSlice(topicInfo.CatchupList, nid) == -1 {
		topicInfo.CatchupList = append(topicInfo.CatchupList, nid)
		catchupChanged = true
	}
	if catchupChanged {
		err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
			&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic node info failed: %v", err.Error())
			return &CoordErr{err.Error(), RpcNoErr, CoordCommonErr}
		}
		self.notifyTopicMetaInfo(topicInfo)
	} else {
		self.notifyCatchupTopicMetaInfo(topicInfo)
	}
	return nil
}

// make sure the previous leader is not holding its leader session.
func (self *NsqLookupCoordinator) waitOldLeaderRelease(topicInfo *TopicPartitionMetaInfo) error {
	err := RetryWithTimeout(func() error {
		s, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err == nil {
			// the leader data is clean, we treat as no leader
			if s.LeaderNode == nil && s.Session == "" {
				coordLog.Infof("leader session is clean: %v", s)
				return nil
			}
			time.Sleep(time.Millisecond * 100)
			aliveNodes := self.getCurrentNodes()
			if _, ok := aliveNodes[topicInfo.Leader]; !ok {
				coordLog.Warningf("the leader node %v is lost while wait release for topic: %v", topicInfo.Leader, topicInfo.GetTopicDesp())
				if self.IsMineLeader() {
					self.leadership.ReleaseTopicLeader(topicInfo.Name, topicInfo.Partition, s)
				}
			}
			return ErrWaitingLeaderRelease
		}
		if err != nil {
			coordLog.Infof("get leader session error: %v", err)
		}
		if err == ErrLeaderSessionNotExist {
			return nil
		}
		return err
	})
	return err
}

func (self *NsqLookupCoordinator) makeNewTopicLeaderAcknowledged(topicInfo *TopicPartitionMetaInfo,
	newLeader string, newestLogID int64, isOldLeaderAlive bool) *CoordErr {
	if topicInfo.Leader == newLeader {
		coordLog.Infof("topic new leader is the same with old: %v", topicInfo)
		return ErrLeaderElectionFail
	}
	newTopicInfo := *topicInfo
	newTopicInfo.ISR = make([]string, 0)
	for _, nid := range topicInfo.ISR {
		if nid == topicInfo.Leader {
			continue
		}
		newTopicInfo.ISR = append(newTopicInfo.ISR, nid)
	}
	newTopicInfo.CatchupList = make([]string, 0)
	for _, nid := range topicInfo.CatchupList {
		if nid == topicInfo.Leader {
			continue
		}
		newTopicInfo.CatchupList = append(newTopicInfo.CatchupList, nid)
	}
	if isOldLeaderAlive {
		newTopicInfo.ISR = append(newTopicInfo.ISR, topicInfo.Leader)
	} else {
		newTopicInfo.CatchupList = append(newTopicInfo.CatchupList, topicInfo.Leader)
	}
	newTopicInfo.Leader = newLeader

	rpcErr := self.notifyLeaderDisableTopicWrite(&newTopicInfo)
	if rpcErr != nil {
		coordLog.Infof("disable write failed while make new leader: %v", rpcErr)
		return rpcErr
	}
	// check the node for topic again to avoid conflict of the topic node
	if !self.dpm.checkTopicNodeConflict(topicInfo) {
		coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
		return ErrTopicNodeConflict
	}
	newTopicInfo.EpochForWrite++

	err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
		&newTopicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update topic node info failed: %v", err)
		return &CoordErr{err.Error(), RpcNoErr, CoordCommonErr}
	}
	coordLog.Infof("make new topic leader info : %v", newTopicInfo)
	*topicInfo = newTopicInfo
	self.notifyTopicMetaInfo(topicInfo)

	var leaderSession *TopicLeaderSession
	retry := 3
	for retry > 0 {
		retry--
		leaderSession, err = self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err != nil || leaderSession.LeaderNode == nil || leaderSession.Session == "" {
			coordLog.Infof("topic leader session still missing")
			currentNodes := self.getCurrentNodes()
			_, ok := currentNodes[topicInfo.Leader]
			if !ok {
				coordLog.Warningf("leader is lost while waiting acknowledge")
				return ErrLeaderNodeLost
			}
			self.notifyISRTopicMetaInfo(topicInfo)
			self.notifyAcquireTopicLeader(topicInfo)
			time.Sleep(time.Second)
		} else {
			coordLog.Infof("topic leader session found: %v", leaderSession)
			go self.revokeEnableTopicWrite(topicInfo.Name, topicInfo.Partition, true)
			return nil
		}
	}

	return ErrLeaderElectionFail
}

func (self *NsqLookupCoordinator) checkISRLogConsistence(topicInfo *TopicPartitionMetaInfo) ([]string, *CoordErr) {
	wrongNodes := make([]string, 0)
	leaderLogID, err := self.getNsqdLastCommitLogID(topicInfo.Leader, topicInfo)
	if err != nil {
		coordLog.Infof("failed to get the leader commit log: %v", err)
		return wrongNodes, err
	}

	var coordErr *CoordErr
	for _, nid := range topicInfo.ISR {
		tmp, err := self.getNsqdLastCommitLogID(nid, topicInfo)
		if err != nil {
			coordLog.Infof("failed to get log id for node: %v", nid)
			coordErr = err
			continue
		}
		if tmp != leaderLogID {
			coordLog.Infof("isr log mismatched with leader %v, %v on node: %v", leaderLogID, tmp, nid)
			wrongNodes = append(wrongNodes, nid)
			coordErr = ErrTopicCommitLogNotConsistent
		}
	}
	return wrongNodes, coordErr
}

// if any failed to enable topic write , we need start a new join isr session to
// make sure all the isr nodes are ready for write
// should disable write before call
func (self *NsqLookupCoordinator) revokeEnableTopicWrite(topic string, partition int, isLeadershipWait bool) *CoordErr {
	coordLog.Infof("revoke the topic to enable write: %v-%v", topic, partition)
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed : %v", err.Error())
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("ignore since not enough isr : %v", topicInfo)
		go self.notifyCatchupTopicMetaInfo(topicInfo)
		return ErrTopicISRNotEnough
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get leader session: %v", err)
		return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
	}

	coordLog.Infof("revoke begin check: %v-%v", topic, partition)
	self.joinStateMutex.Lock()
	state, ok := self.joinISRState[topicInfo.Name]
	if !ok {
		state = &JoinISRState{}
		self.joinISRState[topicInfo.Name] = state
	}
	self.joinStateMutex.Unlock()
	start := time.Now()
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("request join isr while is waiting joining: %v", state)
		if isLeadershipWait {
			coordLog.Warningf("interrupt the current join wait since the leader is waiting confirmation")
		} else {
			return ErrWaitingJoinISR
		}
	}
	if time.Since(start) > time.Second*10 {
		return ErrOperationExpired
	}
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}
	state.waitingJoin = false
	state.waitingSession = ""

	coordLog.Infof("revoke begin disable write first : %v", topicInfo.GetTopicDesp())
	rpcErr := self.notifyLeaderDisableTopicWrite(topicInfo)
	if rpcErr != nil {
		coordLog.Infof("try disable write for topic %v failed: %v", topicInfo, rpcErr)
		go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
		return rpcErr
	}

	if rpcErr = self.notifyISRDisableTopicWrite(topicInfo); rpcErr != nil {
		coordLog.Infof("try disable isr write for topic %v failed: %v", topicInfo, rpcErr)
		go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
		return rpcErr
	}
	state.isLeadershipWait = isLeadershipWait
	self.initJoinStateAndWait(topicInfo, leaderSession, state)

	return nil
}

func (self *NsqLookupCoordinator) initJoinStateAndWait(topicInfo *TopicPartitionMetaInfo, leaderSession *TopicLeaderSession, state *JoinISRState) {
	state.waitingJoin = true
	state.waitingStart = time.Now()
	state.waitingSession = topicInfo.Leader + ","
	for _, s := range topicInfo.ISR {
		state.waitingSession += s + ","
	}
	state.waitingSession += strconv.Itoa(int(topicInfo.Epoch)) + "-" + strconv.Itoa(int(leaderSession.LeaderEpoch)) + ","
	state.waitingSession += state.waitingStart.String()

	state.doneChan = make(chan struct{})
	state.readyNodes = make(map[string]struct{})
	state.readyNodes[topicInfo.Leader] = struct{}{}

	coordLog.Infof("topic %v isr waiting session init : %v", topicInfo.GetTopicDesp(), state)
	if len(topicInfo.ISR) <= 1 {
		rpcErr := self.notifyISRTopicMetaInfo(topicInfo)
		state.waitingJoin = false
		state.waitingSession = ""
		if state.doneChan != nil {
			close(state.doneChan)
			state.doneChan = nil
		}

		if rpcErr != nil {
			coordLog.Warningf("failed to notify ISR for topic: %v, %v ", topicInfo.GetTopicDesp(), rpcErr)
			return
		}

		self.notifyTopicLeaderSession(topicInfo, leaderSession, state.waitingSession)
		if len(topicInfo.ISR) > topicInfo.Replica/2 {
			rpcErr = self.notifyEnableTopicWrite(topicInfo)
			if rpcErr != nil {
				coordLog.Warningf("failed to enable write for topic: %v, %v ", topicInfo.GetTopicDesp(), rpcErr)
			}
		} else {
			coordLog.Infof("leaving the topic %v without enable write since not enough replicas.", topicInfo)
		}
		coordLog.Infof("isr join state is ready since only leader in isr")
		return
	} else {
		go self.waitForFinalSyncedISR(*topicInfo, *leaderSession, state, state.doneChan)
	}
	self.notifyTopicMetaInfo(topicInfo)
	self.notifyTopicLeaderSession(topicInfo, leaderSession, state.waitingSession)
}

func (self *NsqLookupCoordinator) notifySingleNsqdForTopicReload(topicInfo TopicPartitionMetaInfo, nodeID string) *CoordErr {
	// TODO: maybe should disable write if reload node is in isr.
	rpcErr := self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nodeID, &topicInfo)
	if rpcErr != nil {
		coordLog.Infof("failed to notify topic %v info to %v : %v", topicInfo.GetTopicDesp(), nodeID, rpcErr)
		if rpcErr.IsEqual(ErrTopicCoordStateInvalid) {
			go self.revokeEnableTopicWrite(topicInfo.Name, topicInfo.Partition, false)
		}
		return rpcErr
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Infof("get leader session failed: %v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
	}
	return self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nodeID, &topicInfo, leaderSession, "")
}

func (self *NsqLookupCoordinator) notifyAllNsqdsForTopicReload(topicInfo TopicPartitionMetaInfo) *CoordErr {
	coordLog.Infof("reload topic %v for all nodes ", topicInfo.GetTopicDesp())
	rpcErr := self.notifyISRTopicMetaInfo(&topicInfo)
	if rpcErr != nil {
		coordLog.Infof("failed to notify topic %v info : %v", topicInfo.GetTopicDesp(), rpcErr)
		if rpcErr.IsEqual(ErrTopicCoordStateInvalid) {
			go self.revokeEnableTopicWrite(topicInfo.Name, topicInfo.Partition, false)
		}
		return rpcErr
	}
	self.notifyTopicMetaInfo(&topicInfo)
	leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	if err == nil {
		self.notifyTopicLeaderSession(&topicInfo, leaderSession, "")
	} else {
		coordLog.Infof("get leader session failed: %v", err)
	}
	return nil
}

func (self *NsqLookupCoordinator) handleRequestJoinCatchup(topic string, partition int, nid string) *CoordErr {
	var topicInfo *TopicPartitionMetaInfo
	var err error
	topicInfo, err = self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nid) != -1 {
		return &CoordErr{"catchup node should not in the isr", RpcCommonErr, CoordCommonErr}
	}
	if len(topicInfo.ISR)+len(topicInfo.CatchupList) > topicInfo.Replica {
		coordLog.Infof("topic(%v) current isr and catchup list: %v", topicInfo.GetTopicDesp(), topicInfo.ISR, topicInfo.CatchupList)
		return ErrTopicISRCatchupEnough
	}
	coordLog.Infof("node %v try join catchup for topic: %v", nid, topicInfo.GetTopicDesp())
	if FindSlice(topicInfo.CatchupList, nid) == -1 {
		topicInfo.CatchupList = append(topicInfo.CatchupList, nid)
		err = self.leadership.UpdateTopicNodeInfo(topic, partition, &topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("failed to update catchup list: %v", err)
			return &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
		}
	}
	go self.notifyTopicMetaInfo(topicInfo)
	return nil
}

func (self *NsqLookupCoordinator) prepareJoinState(topic string, partition int, checkLeaderSession bool) (*TopicPartitionMetaInfo, *TopicLeaderSession, *JoinISRState, *CoordErr) {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v", err)
		return nil, nil, nil, &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get leader session: %v", err)
		if checkLeaderSession {
			return nil, nil, nil, &CoordErr{err.Error(), RpcCommonErr, CoordElectionTmpErr}
		}
	}

	self.joinStateMutex.Lock()
	state, ok := self.joinISRState[topicInfo.Name]
	if !ok {
		state = &JoinISRState{}
		self.joinISRState[topicInfo.Name] = state
	}
	self.joinStateMutex.Unlock()

	return topicInfo, leaderSession, state, nil
}

func (self *NsqLookupCoordinator) handleRequestJoinISR(topic string, partition int, nodeID string) *CoordErr {
	// 1. got join isr request, check valid, should be in catchup list.
	// 2. notify the topic leader disable write then disable other isr
	// 3. add the node to ISR and remove from CatchupList.
	// 4. insert wait join session, notify all nodes for the new isr
	// 5. wait on the join session until all the new isr is ready (got the ready notify from isr)
	// 6. timeout or done, clear current join session, (only keep isr that got ready notify, shoud be quorum), enable write
	topicInfo, leaderSession, state, coordErr := self.prepareJoinState(topic, partition, true)
	if coordErr != nil {
		coordLog.Infof("failed to prepare join state: %v", coordErr)
		return coordErr
	}
	if FindSlice(topicInfo.CatchupList, nodeID) == -1 {
		coordLog.Infof("join isr node is not in catchup list.")
		return ErrJoinISRInvalid
	}
	if len(topicInfo.ISR) > topicInfo.Replica {
		coordLog.Infof("topic(%v) current isr list is enough: %v", topicInfo.GetTopicDesp(), topicInfo.ISR)
		return ErrTopicISRCatchupEnough
	}
	self.nodesMutex.RLock()
	_, ok := self.removingNodes[nodeID]
	self.nodesMutex.RUnlock()
	if ok {
		return ErrClusterNodeRemoving
	}

	coordLog.Infof("node %v request join isr for topic %v", nodeID, topicInfo.GetTopicDesp())

	// we go here to allow the rpc call from client can return ok immediately
	go func() {
		start := time.Now()
		state.Lock()
		defer state.Unlock()
		if state.waitingJoin {
			coordLog.Warningf("failed request join isr because another is joining. :%v", state)
			return
		}
		if time.Since(start) > time.Second*10 {
			coordLog.Warningf("failed since waiting too long for lock")
			return
		}
		if state.doneChan != nil {
			close(state.doneChan)
			state.doneChan = nil
		}
		state.waitingJoin = false
		state.waitingSession = ""

		rpcErr := self.notifyLeaderDisableTopicWrite(topicInfo)
		if rpcErr != nil {
			coordLog.Warningf("try disable write for topic %v failed: %v", topicInfo.GetTopicDesp(), rpcErr)
			go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
			return
		}
		if rpcErr = self.notifyISRDisableTopicWrite(topicInfo); rpcErr != nil {
			coordLog.Infof("try disable isr write for topic %v failed: %v", topicInfo, rpcErr)
			go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
			return
		}

		state.isLeadershipWait = false

		newCatchupList := make([]string, 0)
		for _, nid := range topicInfo.CatchupList {
			if nid == nodeID {
				continue
			}
			newCatchupList = append(newCatchupList, nid)
		}
		topicInfo.CatchupList = newCatchupList
		topicInfo.ISR = append(topicInfo.ISR, nodeID)

		// new node should disable write also.
		if rpcErr = self.notifyISRDisableTopicWrite(topicInfo); rpcErr != nil {
			coordLog.Infof("try disable isr write for topic %v failed: %v", topicInfo, rpcErr)
			go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
			return
		}
		if !self.dpm.checkTopicNodeConflict(topicInfo) {
			coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
			return
		}

		topicInfo.EpochForWrite++

		err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
			&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("move catchup node to isr failed: %v", err)
			// continue here to allow the wait goroutine to handle the timeout
		}
		self.initJoinStateAndWait(topicInfo, leaderSession, state)
	}()
	return nil
}

func (self *NsqLookupCoordinator) handleReadyForISR(topic string, partition int, nodeID string,
	leaderSession TopicLeaderSession, joinISRSession string) *CoordErr {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed : %v", err.Error())
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		coordLog.Infof("got ready for isr but not a isr node: %v, isr is: %v", nodeID, topicInfo.ISR)
		return ErrJoinISRInvalid
	}

	// check for state and should lock for the state to prevent others join isr.
	self.joinStateMutex.Lock()
	state, ok := self.joinISRState[topicInfo.Name]
	self.joinStateMutex.Unlock()
	if !ok || state == nil {
		coordLog.Warningf("failed join isr because the join state is not set: %v", topicInfo.GetTopicDesp())
		return ErrJoinISRInvalid
	}
	// we go here to allow the rpc call from client can return ok immediately
	go func() {
		start := time.Now()
		state.Lock()
		defer state.Unlock()
		if !state.waitingJoin || state.waitingSession != joinISRSession {
			coordLog.Infof("state mismatch: %v, request join session: %v", state, joinISRSession)
			return
		}

		if time.Since(start) > time.Second*10 {
			coordLog.Warningf("failed since waiting too long for lock")
			return
		}
		coordLog.Infof("topic %v isr node %v ready for state: %v", topicInfo.GetTopicDesp(), nodeID, joinISRSession)
		state.readyNodes[nodeID] = struct{}{}
		for _, n := range topicInfo.ISR {
			if _, ok := state.readyNodes[n]; !ok {
				coordLog.Infof("node %v still waiting ready", n)
				return
			}
		}
		// check the newest log for isr to make sure consistence.
		wrongISR, rpcErr := self.checkISRLogConsistence(topicInfo)
		if rpcErr != nil || len(wrongISR) > 0 {
			// isr should be removed since not consistence
			coordLog.Infof("the isr nodes: %v not consistence", wrongISR)
			return
		}
		coordLog.Infof("topic %v isr new state is ready for all: %v", topicInfo.GetTopicDesp(), state)
		if len(topicInfo.ISR) > topicInfo.Replica/2 {
			rpcErr = self.notifyEnableTopicWrite(topicInfo)
			if rpcErr != nil {
				coordLog.Warningf("failed to enable write for topic: %v, %v ", topicInfo.GetTopicDesp(), rpcErr)
				go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
			}
		} else {
			coordLog.Infof("leaving the topic %v without enable write since not enough replicas.", topicInfo)
		}
		state.waitingJoin = false
		state.waitingSession = ""
		if state.doneChan != nil {
			close(state.doneChan)
			state.doneChan = nil
		}
		if len(topicInfo.ISR) >= topicInfo.Replica && len(topicInfo.CatchupList) > 0 {
			oldCatchupList := topicInfo.CatchupList
			topicInfo.CatchupList = make([]string, 0)
			coordErr := self.notifyOldNsqdsForTopicMetaInfo(topicInfo, oldCatchupList)
			if coordErr != nil {
				coordLog.Infof("notify removing catchup failed : %v", coordErr)
				topicInfo.CatchupList = oldCatchupList
			}
			coordLog.Infof("removing catchup since the isr is enough: %v", oldCatchupList)
			err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
				&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
			if err != nil {
				coordLog.Infof("update catchup info failed while removing catchup: %v", err)
			} else {
				self.notifyTopicMetaInfo(topicInfo)
			}
		}
	}()
	return nil
}

func (self *NsqLookupCoordinator) resetJoinISRState(topicInfo TopicPartitionMetaInfo, state *JoinISRState, updateISR bool) *CoordErr {
	state.Lock()
	defer state.Unlock()
	if !state.waitingJoin {
		return nil
	}
	state.waitingJoin = false
	state.waitingSession = ""
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}
	coordLog.Infof("topic: %v reset waiting join state: %v", topicInfo.GetTopicDesp(), state)
	ready := 0
	for _, n := range topicInfo.ISR {
		if _, ok := state.readyNodes[n]; ok {
			ready++
		}
	}

	if ready <= topicInfo.Replica/2 {
		coordLog.Infof("no enough ready isr while reset wait join: %v, expect: %v, actual: %v", state.waitingSession, topicInfo.ISR, state.readyNodes)
		// even timeout we can not enable this topic since no enough replicas
		// however, we should clear the join state so that we can try join new isr later
		go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
	} else {
		// some of isr failed to ready for the new isr state, we need rollback the new isr with the
		// isr got ready.
		coordLog.Infof("the join state: expect ready isr : %v, actual ready: %v ", topicInfo.ISR, state.readyNodes)
		if updateISR && ready != len(topicInfo.ISR) {
			oldISR := topicInfo.ISR
			newCatchupList := make(map[string]struct{})
			for _, n := range topicInfo.CatchupList {
				newCatchupList[n] = struct{}{}
			}
			topicInfo.ISR = make([]string, 0, len(state.readyNodes))
			for _, n := range oldISR {
				if _, ok := state.readyNodes[n]; ok {
					topicInfo.ISR = append(topicInfo.ISR, n)
				} else {
					newCatchupList[n] = struct{}{}
				}
			}
			topicInfo.CatchupList = make([]string, 0)
			for n, _ := range newCatchupList {
				topicInfo.CatchupList = append(topicInfo.CatchupList, n)
			}

			if !self.dpm.checkTopicNodeConflict(&topicInfo) {
				coordLog.Warningf("this topic info update is conflict : %v", topicInfo)
				return ErrTopicNodeConflict
			}

			topicInfo.EpochForWrite++
			err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition,
				&topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
			if err != nil {
				coordLog.Infof("update topic info failed: %v", err)
				return &CoordErr{err.Error(), RpcNoErr, CoordElectionErr}
			}
			rpcErr := self.notifyISRTopicMetaInfo(&topicInfo)
			if rpcErr != nil {
				coordLog.Infof("failed to notify new topic info: %v", topicInfo)
				go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
				return rpcErr
			}
		}

		// check the newest log for isr to make sure consistence.
		wrongISR, rpcErr := self.checkISRLogConsistence(&topicInfo)
		if rpcErr != nil || len(wrongISR) > 0 {
			// isr should be removed since not consistence
			coordLog.Infof("remove the isr node: %v since not consistence", wrongISR)
			go self.handleRemoveFailedISRNodes(wrongISR, &topicInfo)
			return rpcErr
		}
		rpcErr = self.notifyEnableTopicWrite(&topicInfo)
		if rpcErr != nil {
			coordLog.Warningf("failed to enable write :%v, %v", topicInfo.GetTopicDesp(), rpcErr)
			go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second*3)
		}
	}

	return nil
}

func (self *NsqLookupCoordinator) waitForFinalSyncedISR(topicInfo TopicPartitionMetaInfo, leaderSession TopicLeaderSession, state *JoinISRState, doneChan chan struct{}) {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		coordLog.Infof("wait timeout for sync isr.")
	case <-doneChan:
		return
	}

	self.resetJoinISRState(topicInfo, state, true)
}

func (self *NsqLookupCoordinator) handleLeaveFromISR(topic string, partition int, leader *TopicLeaderSession, nodeID string) *CoordErr {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed :%v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		return nil
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node, graceful leaving should wait.")
		go self.notifyCatchupTopicMetaInfo(topicInfo)
		go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
		return ErrLeavingISRWait
	}

	if topicInfo.Leader == nodeID {
		coordLog.Infof("the leader node %v will leave the isr, prepare transfer leader for topic: %v", nodeID, topicInfo.GetTopicDesp())
		currentNodes, currentNodesEpoch := self.getCurrentNodesWithEpoch()

		go self.handleTopicLeaderElection(topicInfo, currentNodes, currentNodesEpoch, false)
		return nil
	}

	if leader != nil {
		newestLeaderSession, err := self.leadership.GetTopicLeaderSession(topic, partition)
		if err != nil {
			coordLog.Infof("get leader session failed: %v.", err.Error())
			return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
		}
		if !leader.IsSame(newestLeaderSession) {
			return ErrNotTopicLeader
		}
	}

	failedNodes := make([]string, 0, 1)
	failedNodes = append(failedNodes, nodeID)
	coordErr := self.handleRemoveFailedISRNodes(failedNodes, topicInfo)
	if coordErr == nil {
		coordLog.Infof("node %v removed by plan from topic isr: %v", nodeID, topicInfo)
	}
	return coordErr
}

func (self *NsqLookupCoordinator) handleRequestCheckTopicConsistence(topic string, partition int) *CoordErr {
	self.triggerCheckTopics(topic, partition, 0)
	return nil
}

func (self *NsqLookupCoordinator) handleRequestNewTopicInfo(topic string, partition int, nodeID string) *CoordErr {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed : %v", err.Error())
		return nil
	}
	self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nodeID, topicInfo)
	leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		coordLog.Infof("get leader session failed: %v", err)
		return nil
	}
	self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nodeID, topicInfo, leaderSession, "")
	return nil
}

func (self *NsqLookupCoordinator) handleMoveTopic(isLeader bool, topic string, partition int,
	nodeID string) *CoordErr {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed :%v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		return nil
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node, graceful leaving should wait.")
		go self.notifyCatchupTopicMetaInfo(topicInfo)
		go self.triggerCheckTopics(topicInfo.Name, topicInfo.Partition, time.Second)
		return ErrLeavingISRWait
	}

	if topicInfo.Leader == nodeID {
		if !isLeader {
			coordLog.Infof("try move topic %v not as leader, but we are leader topic %v ", topicInfo.GetTopicDesp(), nodeID)
			return nil
		}
		coordLog.Infof("the leader node %v will leave the isr, prepare transfer leader for topic: %v", nodeID, topicInfo.GetTopicDesp())
		currentNodes, currentNodesEpoch := self.getCurrentNodesWithEpoch()
		coordErr := self.handleTopicLeaderElection(topicInfo, currentNodes, currentNodesEpoch, true)
		if coordErr != nil {
			return coordErr
		}

		// the old leader removed to catchup, so remove it from catchup
		topicInfo.CatchupList = FilterList(topicInfo.CatchupList, []string{nodeID})
		err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, &topicInfo.TopicPartitionReplicaInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic node isr failed: %v", err)
			return &CoordErr{err.Error(), RpcNoErr, CoordNetErr}
		}
		go self.notifyTopicMetaInfo(topicInfo)
		return nil
	} else {
		coordLog.Infof("the topic %v isr node %v leaving ", topicInfo.GetTopicDesp(), nodeID)
		nodeList := make([]string, 1)
		nodeList[0] = nodeID
		coordErr := self.handleRemoveISRNodes(nodeList, topicInfo, false)
		if coordErr != nil {
			coordLog.Infof("topic %v remove node %v failed: %v", topicInfo.GetTopicDesp(),
				nodeID, coordErr)
		} else {
			coordLog.Infof("topic %v remove node %v by plan", topicInfo.GetTopicDesp(),
				nodeID)
		}
	}
	return nil
}
