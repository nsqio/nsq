package consistence

import (
	"errors"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/cenkalti/backoff"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

var (
	ErrAlreadyExist       = errors.New("already exist")
	ErrTopicNotCreated    = errors.New("topic is not created")
	ErrSessionNotExist    = errors.New("session not exist")
	ErrNodeUnavailable    = errors.New("No node is available for the topic")
	ErrNotNsqLookupLeader = errors.New("Not nsqlookup leader")
	ErrLeaderElectionFail = errors.New("Leader election failed.")
	ErrNodeNotFound       = errors.New("node not found")
	ErrJoinISRInvalid     = NewCoordErr("Join ISR failed", CoordCommonErr)
	ErrJoinISRTimeout     = NewCoordErr("Join ISR timeout", CoordCommonErr)
	ErrWaitingJoinISR     = NewCoordErr("The topic is waiting node to join isr", CoordCommonErr)
)

// new topic can have an advice load factor to give the suggestion about the
// future load. While the topic data is small the load compute is not precise,
// so we use the advised load to determine the actual topic load.
// use 1~10 to advise from lowest to highest. The 1 or 10 should be used with much careful.

const (
	defaultTopicLoadFactor = 3
)

type NodeTopicStats struct {
	NodeID string
	// the consumed data (MB) on the leader last hour for each channel in the topic.
	ChannelConsumeData map[string]map[string]int
	// the data still need consume. unit: MB
	TopicLeaderDataSize map[string]int
	TopicTotalDataSize  map[string]int
	NodeCPUs            int
}

// the load factor is something like cpu load factor that
// stand for the busy/idle state for this node.
// the larger means busier.
func (self *NodeTopicStats) GetNodeLoadFactor() float64 {
	perCpuStat, leaderLf := self.GetNodeLeaderLoadFactor()
	totalDataSize := 0
	for _, v := range perCpuStat.TopicTotalDataSize {
		totalDataSize += v
	}
	totalDataSize += len(perCpuStat.TopicTotalDataSize)
	return leaderLf + float64(totalDataSize)/2.00
}

func (self *NodeTopicStats) GetNodeLeaderLoadFactor() (*NodeTopicStats, float64) {
	perCpuStat := self.GetPerCPUStats()
	totalConsumed := 0
	for _, t := range perCpuStat.ChannelConsumeData {
		for _, c := range t {
			totalConsumed += c
		}
	}
	totalConsumed += len(perCpuStat.ChannelConsumeData)
	totalLeaderDataSize := 0
	for _, v := range perCpuStat.TopicLeaderDataSize {
		totalLeaderDataSize += v
	}
	totalLeaderDataSize += len(perCpuStat.TopicLeaderDataSize)
	return perCpuStat, float64(totalConsumed) + float64(totalLeaderDataSize)/2.00
}

func (self *NodeTopicStats) GetTopicLoadFactor(topic string) float64 {
	perCpuStat := self.GetPerCPUStats()
	topicConsume, ok := perCpuStat.ChannelConsumeData[topic]
	totalConsume := 0
	if ok {
		for _, c := range topicConsume {
			totalConsume += c
		}
	}
	topicData, ok := perCpuStat.TopicTotalDataSize[topic]
	if ok {
		return float64(totalConsume + topicData/2)
	}
	return float64(totalConsume)
}

func (self *NodeTopicStats) GetPerCPUStats() *NodeTopicStats {
	consumed := make(map[string]map[string]int)
	for tname, t := range self.ChannelConsumeData {
		if _, ok := consumed[tname]; !ok {
			consumed[tname] = make(map[string]int)
		}
		for chanName, c := range t {
			consumed[tname][chanName] = c / (self.NodeCPUs/4 + 1)
		}
	}
	leaderSize := make(map[string]int)
	for tname, v := range self.TopicLeaderDataSize {
		leaderSize[tname] = v / (self.NodeCPUs/4 + 1)
	}
	totalSize := make(map[string]int)
	for tname, v := range self.TopicTotalDataSize {
		totalSize[tname] = v / (self.NodeCPUs/4 + 1)
	}
	return &NodeTopicStats{
		self.NodeID,
		consumed,
		leaderSize,
		totalSize,
		1,
	}
}

func (self *NodeTopicStats) LeaderLessLoader(other *NodeTopicStats) bool {
	_, left := self.GetNodeLeaderLoadFactor()
	_, right := other.GetNodeLeaderLoadFactor()
	if left < right {
		return true
	}

	return false
}

func (self *NodeTopicStats) SlaveLessLoader(other *NodeTopicStats) bool {
	left := self.GetNodeLoadFactor()
	right := other.GetNodeLoadFactor()
	if left < right {
		return true
	}
	return false
}

type By func(l, r *NodeTopicStats) bool

func (by By) Sort(statList []NodeTopicStats) {
	sorter := &StatsSorter{
		stats: statList,
		by:    by,
	}
	sort.Sort(sorter)
}

type StatsSorter struct {
	stats []NodeTopicStats
	by    By
}

func (s *StatsSorter) Len() int {
	return len(s.stats)
}
func (s *StatsSorter) Swap(i, j int) {
	s.stats[i], s.stats[j] = s.stats[j], s.stats[i]
}
func (s *StatsSorter) Less(i, j int) bool {
	return s.by(&s.stats[i], &s.stats[j])
}

type JoinISRState struct {
	sync.Mutex
	waitingJoin    bool
	waitingSession string
	waitingStart   time.Time
	readyNodes     map[string]struct{}
	doneChan       chan struct{}
}

type RpcFailedInfo struct {
	nodeID    string
	topic     string
	partition int
	failTime  time.Time
}

func getOthersExceptLeader(topicInfo *TopicPartionMetaInfo) []string {
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

type NsqLookupCoordinator struct {
	clusterKey         string
	myNode             NsqLookupdNodeInfo
	leaderNode         NsqLookupdNodeInfo
	leadership         NSQLookupdLeadership
	nsqdNodes          map[string]NsqdNodeInfo
	nsqdRpcClients     map[string]*NsqdRpcClient
	nsqdNodeFailChan   chan struct{}
	stopChan           chan struct{}
	joinISRState       map[string]*JoinISRState
	failedRpcList      []RpcFailedInfo
	quitChan           chan struct{}
	nsqlookupRpcServer *NsqLookupCoordRpcServer
}

func SetCoordLogger(log levellogger.Logger, level int32) {
	coordLog.logger = log
	coordLog.level = level
}

func NewNsqLookupCoordinator(cluster string, n *NsqLookupdNodeInfo) *NsqLookupCoordinator {
	coord := &NsqLookupCoordinator{
		clusterKey:       cluster,
		myNode:           *n,
		leadership:       &FakeNsqlookupLeadership{},
		nsqdNodes:        make(map[string]NsqdNodeInfo),
		nsqdRpcClients:   make(map[string]*NsqdRpcClient),
		nsqdNodeFailChan: make(chan struct{}, 1),
		stopChan:         make(chan struct{}),
		joinISRState:     make(map[string]*JoinISRState),
		failedRpcList:    make([]RpcFailedInfo, 0),
		quitChan:         make(chan struct{}),
	}
	coord.nsqlookupRpcServer = NewNsqLookupCoordRpcServer(coord)
	return coord
}

func (self *NsqLookupCoordinator) SetLeadershipMgr(l NSQLookupdLeadership) {
	self.leadership = l
}

func RetryWithTimeout(fn func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = time.Second * 30
	bo.MaxInterval = time.Second * 5
	return backoff.Retry(fn, bo)
}

// init and register to leader server
func (self *NsqLookupCoordinator) Start() error {
	self.leadership.InitClusterID(self.clusterKey)
	err := self.leadership.Register(self.myNode)
	if err != nil {
		coordLog.Warningf("failed to start nsqlookupd coordinator: %v", err)
		return err
	}
	go self.handleLeadership()
	go self.nsqlookupRpcServer.start(self.myNode.NodeIp, self.myNode.RpcPort)
	return nil
}

func (self *NsqLookupCoordinator) Stop() {
	close(self.stopChan)
	self.nsqlookupRpcServer.stop()
	<-self.quitChan
}

func (self *NsqLookupCoordinator) handleLeadership() {
	lookupdLeaderChan := make(chan *NsqLookupdNodeInfo)
	go self.leadership.AcquireAndWatchLeader(lookupdLeaderChan, self.stopChan)
	defer close(self.quitChan)
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
				go self.notifyLeaderChanged()
			}
			if self.leaderNode.GetID() == "" {
				coordLog.Warningln("leader is lost.")
			}
		case <-self.stopChan:
			return
		}
	}
}

func (self *NsqLookupCoordinator) notifyLeaderChanged() {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("I am slave.")
		// remove watchers.
		return
	}
	// reload topic information
	newTopics, err := self.leadership.ScanTopics()
	if err != nil {
		coordLog.Errorf("load topic info failed: %v", err)
	} else {
		newTopicsMap := make(map[string]map[int]TopicPartionMetaInfo)
		for _, t := range newTopics {
			if _, ok := newTopicsMap[t.Name]; !ok {
				newTopicsMap[t.Name] = make(map[int]TopicPartionMetaInfo)
			}
			newTopicsMap[t.Name][t.Partition] = t
			coordLog.Infof("found topic %v partition %v: %v", t.Name, t.Partition, t)
		}
		_ = newTopicsMap

		coordLog.Infof("topic loaded : %v", len(newTopics))
		self.NotifyTopicsToAllNsqdForReload(newTopics)
	}
	for _, t := range newTopics {
		go self.watchTopicLeaderSession(self.leaderNode.GetID(), self.leaderNode.Epoch, t.Name, t.Partition)
	}

	go self.handleNsqdNodes(self.leaderNode.GetID(), self.leaderNode.Epoch)
	go self.checkTopics(self.leaderNode.GetID(), self.leaderNode.Epoch)
	go self.rpcFailRetryFunc(self.leaderNode.GetID(), self.leaderNode.Epoch)
	go self.balanceTopicData(self.leaderNode.GetID(), self.leaderNode.Epoch)
}

// for the nsqd node that temporally lost, we need send the related topics to
// it .
func (self *NsqLookupCoordinator) NotifyTopicsToSingleNsqdForReload(topics []TopicPartionMetaInfo, nodeID string) {
	for _, v := range topics {
		if FindSlice(v.ISR, nodeID) != -1 {
			self.notifySingleNsqdForTopicReload(&v, nodeID)
		}
		if FindSlice(v.CatchupList, nodeID) != -1 {
			self.sendUpdateCatchupToNsqd(self.leaderNode.Epoch, nodeID, &v)
		}
		//TODO: check disable write on the nsqd and continue enable the write
	}
}

func (self *NsqLookupCoordinator) NotifyTopicsToAllNsqdForReload(topics []TopicPartionMetaInfo) {
	for _, v := range topics {
		self.notifyAllNsqdsForTopicReload(&v)
		//TODO: check disable write on the nsqd and continue enable the write
	}
}

func (self *NsqLookupCoordinator) handleNsqdNodes(leaderID string, epoch int) {
	nsqdNodesChan := make(chan []NsqdNodeInfo)
	stopWatchNsqd := make(chan struct{})
	go self.leadership.WatchNsqdNodes(nsqdNodesChan, stopWatchNsqd)
	ticker := time.NewTicker(time.Second * 10)
	defer func() {
		ticker.Stop()
		coordLog.Infof("stop watch the nsqd nodes.")
	}()
	for {
		select {
		case nodes, ok := <-nsqdNodesChan:
			if !ok {
				return
			}
			// check if any nsqd node changed.
			coordLog.Infof("Current nsqd nodes: %v", len(nodes))
			oldNodes := self.nsqdNodes
			newNodes := make(map[string]NsqdNodeInfo)
			for _, v := range nodes {
				coordLog.Infof("nsqd node %v : %v", v.GetID(), v)
				newNodes[v.GetID()] = v
			}
			self.nsqdNodes = newNodes
			for oldID, oldNode := range oldNodes {
				if _, ok := newNodes[oldID]; !ok {
					coordLog.Warningf("nsqd node failed: %v, %v", oldID, oldNode)
					// if node is missing we need check election immediately.
					self.nsqdNodeFailChan <- struct{}{}
				}
			}
			for newID, newNode := range newNodes {
				if _, ok := oldNodes[newID]; !ok {
					coordLog.Infof("new nsqd node joined: %v, %v", newID, newNode)
					// TODO: check if this node in catchup list and notify new
					// topicInfo to the node.
					// notify the nsqd node to recheck topic info.(for
					// temp lost)
					topics, err := self.leadership.ScanTopics()
					if err != nil {
						coordLog.Infof("scan topics failed: %v", err)
						continue
					}
					self.NotifyTopicsToSingleNsqdForReload(topics, newID)
				}
			}
		case <-ticker.C:
			if self.leaderNode.GetID() != leaderID ||
				self.leaderNode.Epoch != epoch {
				close(stopWatchNsqd)
				return
			}
		}
	}
}

func (self *NsqLookupCoordinator) watchTopicLeaderSession(leaderID string, epoch int, name string, pid int) {
	leaderChan := make(chan *TopicLeaderSession, 1)
	stopWatch := make(chan struct{})
	go self.leadership.WatchTopicLeader(name, pid, leaderChan, stopWatch)
	ticker := time.NewTicker(time.Second * 10)
	defer func() {
		ticker.Stop()
		coordLog.Infof("stop watch the topic leader session.")
	}()

	for {
		select {
		case <-ticker.C:
			if self.leaderNode.GetID() != leaderID ||
				self.leaderNode.Epoch != epoch {
				close(stopWatch)
				return
			}
		case n, ok := <-leaderChan:
			if !ok {
				return
			}
			topicInfo, err := self.leadership.GetTopicInfo(name, pid)
			if err != nil {
				coordLog.Infof("failed to get topic info: %v", err)
				continue
			}
			self.notifyTopicLeaderSession(topicInfo, n)
			if n.LeaderNode == nil {
				// TODO: try do election for this topic
			}
		}
	}
}

// check if partition is enough,
// check if replication is enough
// check any unexpected state.
func (self *NsqLookupCoordinator) checkTopics(leaderID string, epoch int) {
	ticker := time.NewTicker(time.Second * 10)
	waitingMigrateTopic := make(map[string]map[int]time.Time)
	defer func() {
		ticker.Stop()
		coordLog.Infof("check topics quit.")
	}()

	checking := true
	for {
		select {
		case <-ticker.C:
			if self.leaderNode.GetID() != leaderID ||
				self.leaderNode.Epoch != epoch {
				return
			}
			if !checking {
				checking = true
				self.doCheckTopics(epoch, waitingMigrateTopic)
				checking = false
			}
		case <-self.nsqdNodeFailChan:
			if !checking {
				checking = true
				self.doCheckTopics(epoch, waitingMigrateTopic)
				checking = false
			}
		}
	}
}

func (self *NsqLookupCoordinator) doCheckTopics(epoch int, waitingMigrateTopic map[string]map[int]time.Time) {
	waitMigrateInterval := time.Minute
	topics, err := self.leadership.ScanTopics()
	if err != nil {
		coordLog.Infof("scan topics failed. %v", err)
		return
	}
	// TODO: check partition number for topic, maybe failed to create
	// some partition when creating topic.
	for _, t := range topics {
		needMigrate := false
		if len(t.ISR) < t.Replica {
			coordLog.Infof("ISR is not enough for topic %v, isr is :%v", t.GetTopicDesp(), t.ISR)
			// notify the migrate goroutine to handle isr lost.
			needMigrate = true
		}
		if _, ok := self.nsqdNodes[t.Leader]; !ok {
			needMigrate = true
			coordLog.Warningf("topic %v leader %v is lost.", t.GetTopicDesp(), t.Leader)
			err := self.handleTopicLeaderElection(&t)
			if err != nil {
				coordLog.Warningf("topic leader election failed: %v", err)
			}
		} else {
			// check topic leader session key.
			_, err := self.leadership.GetTopicLeaderSession(t.Name, t.Partition)
			if err != nil {
				coordLog.Infof("topic leader session %v not found.", t.GetTopicDesp())
				// notify the nsqd node to acquire the leader session.
				self.notifyTopicMetaInfo(&t)
			}
		}
		aliveCount := 0
		for _, replica := range t.ISR {
			if _, ok := self.nsqdNodes[replica]; !ok {
				coordLog.Warningf("topic %v isr node %v is lost.", t.GetTopicDesp(), replica)
				needMigrate = true
			} else {
				aliveCount++
			}
		}
		if needMigrate {
			partitions, ok := waitingMigrateTopic[t.Name]
			if !ok {
				partitions = make(map[int]time.Time)
				waitingMigrateTopic[t.Name] = partitions
			}
			if _, ok := partitions[t.Partition]; !ok {
				partitions[t.Partition] = time.Now()
			}
			if (aliveCount <= t.Replica/2) ||
				partitions[t.Partition].Before(time.Now().Add(-1*waitMigrateInterval)) {
				coordLog.Infof("begin migrate the topic :%v", t.GetTopicDesp())
				self.handleTopicMigrate(t)
				delete(partitions, t.Partition)
			}
		} else {
			if _, ok := waitingMigrateTopic[t.Name]; ok {
				delete(waitingMigrateTopic[t.Name], t.Partition)
			}
		}
	}
}

// make sure the previous leader is not holding its leader session.
func (self *NsqLookupCoordinator) waitOldLeaderRelease(topicInfo *TopicPartionMetaInfo) error {
	err := RetryWithTimeout(func() error {
		_, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err == ErrSessionNotExist {
			return nil
		}
		return err
	})
	return err
}

func (self *NsqLookupCoordinator) chooseNewLeaderFromISR(topicInfo *TopicPartionMetaInfo) (string, int64, error) {
	// choose another leader in ISR list, and add new node to ISR
	// list.
	newestReplicas := make([]string, 0)
	newestLogID := int64(0)
	for _, replica := range topicInfo.ISR {
		if _, ok := self.nsqdNodes[replica]; !ok {
			continue
		}
		if replica == topicInfo.Leader {
			continue
		}
		cid, err := self.getNsqdLastCommitLogID(replica, topicInfo)
		if err != nil {
			continue
		}
		if cid > newestLogID {
			newestReplicas = newestReplicas[0:0]
			newestReplicas = append(newestReplicas, replica)
			newestLogID = cid
		} else if cid == newestLogID {
			newestReplicas = append(newestReplicas, replica)
		}
	}
	// select the least load factor node
	newLeader := ""
	minLF := 100.0
	for _, replica := range newestReplicas {
		stat, err := self.getNsqdTopicStat(self.nsqdNodes[replica])
		if err != nil {
			continue
		}
		_, lf := stat.GetNodeLeaderLoadFactor()
		if lf < minLF {
			newLeader = replica
		}
	}
	if newLeader == "" {
		coordLog.Warningf("No leader can be elected.")
		return "", 0, ErrLeaderElectionFail
	}
	coordLog.Infof("new leader %v found with commit id: %v", newLeader, newestLogID)

	return newLeader, newestLogID, nil
}

func (self *NsqLookupCoordinator) makeNewTopicLeaderAcknowledged(topicInfo *TopicPartionMetaInfo, newLeader string, newestLogID int64) error {
	topicInfo.Leader = newLeader
	err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, topicInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update topic node info failed: %v", err)
		return err
	}
	self.notifyTopicMetaInfo(topicInfo)

	var leaderSession *TopicLeaderSession
	for {
		leaderSession, err = self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err != nil {
			coordLog.Infof("topic leader session still missing")
			time.Sleep(time.Second)
			self.notifyTopicMetaInfo(topicInfo)
		} else {
			coordLog.Infof("topic leader session found: %v", leaderSession)
			self.notifyTopicLeaderSession(topicInfo, leaderSession)
			break
		}
	}
	// new leader is ready (init for write disabled),
	// check ISR sync state.
	// if synced, notify the leader to accept write.
	for {
		waiting := false
		for _, replica := range topicInfo.ISR {
			if _, ok := self.nsqdNodes[replica]; !ok {
				continue
			}
		}
		if !waiting {
			break
		}
		self.notifyTopicMetaInfo(topicInfo)
		self.notifyTopicLeaderSession(topicInfo, leaderSession)
		time.Sleep(time.Second)
	}

	return err
}

func (self *NsqLookupCoordinator) handleTopicLeaderElection(topicInfo *TopicPartionMetaInfo) error {
	err := self.waitOldLeaderRelease(topicInfo)
	if err != nil {
		coordLog.Infof("Leader is not released: %v", topicInfo)
		return err
	}
	err = self.notifyISRDisableTopicWrite(topicInfo)
	if err != nil {
		coordLog.Infof("failed notify disable write while election: %v", err)
		return err
	}
	// choose another leader in ISR list, and add new node to ISR
	// list.
	newLeader, newestLogID, err := self.chooseNewLeaderFromISR(topicInfo)
	if err != nil {
		return err
	}
	topicInfo.Leader = newLeader

	err = self.makeNewTopicLeaderAcknowledged(topicInfo, newLeader, newestLogID)
	if err != nil {
		return err
	}
	err = self.notifyEnableTopicWrite(topicInfo)
	if err != nil {
		coordLog.Infof("enable topic write failed: ", err)
	}
	return err
}

func (self *NsqLookupCoordinator) handleTopicMigrate(topicInfo TopicPartionMetaInfo) {
	if _, ok := self.nsqdNodes[topicInfo.Leader]; !ok {
		err := self.handleTopicLeaderElection(&topicInfo)
		if err != nil {
			coordLog.Warningf("topic leader election error : %v", err)
			return
		}
	}
	newCatchupList := make([]string, 0)
	catchupChanged := false
	for _, n := range topicInfo.CatchupList {
		if _, ok := self.nsqdNodes[n]; ok {
			newCatchupList = append(newCatchupList, n)
			catchupChanged = true
		} else {
			coordLog.Infof("topic %v catchup node %v is lost.", topicInfo.GetTopicDesp(), n)
		}
	}
	newISR := make([]string, 0)
	isrChanged := false
	for _, replica := range topicInfo.ISR {
		if _, ok := self.nsqdNodes[replica]; !ok {
			coordLog.Warningf("topic %v isr node %v is lost.", topicInfo.GetTopicDesp(), replica)
			isrChanged = true
		} else {
			newISR = append(newISR, replica)
		}
	}
	topicNsqdNum := len(newISR) + len(newCatchupList)
	if topicNsqdNum < topicInfo.Replica {
		for i := topicNsqdNum; i < topicInfo.Replica; i++ {
			n, err := self.AllocNodeForTopic(&topicInfo)
			if err != nil {
				newCatchupList = append(newCatchupList, n.GetID())
				catchupChanged = true
			}
		}
	}
	topicInfo.CatchupList = newCatchupList
	topicInfo.ISR = newISR
	if isrChanged {
		err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, &topicInfo, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic node info failed: %v", err.Error())
			return
		}
		coordLog.Infof("topic %v isr list changed: %v", topicInfo.GetTopicDesp(), topicInfo.ISR)
	}
	if catchupChanged {
		err := self.leadership.UpdateTopicCatchupList(topicInfo.Name, topicInfo.Partition, topicInfo.CatchupList, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("update topic catchup failed: %v", err.Error())
			return
		}
		coordLog.Infof("topic %v catchup list changed: %v", topicInfo.GetTopicDesp(), topicInfo.CatchupList)
	}
	err := self.notifyTopicMetaInfo(&topicInfo)
	if err != nil {
		coordLog.Infof("notify topic failed: %v", err.Error())
	}
	err = self.notifyCatchupList(&topicInfo)
	if err != nil {
		coordLog.Infof("notify topic catchup failed: %v", err.Error())
	}
}

func (self *NsqLookupCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, error) {
	c, ok := self.nsqdRpcClients[nid]
	var err error
	if !ok {
		n, ok := self.nsqdNodes[nid]
		if !ok {
			return nil, ErrNodeNotFound
		}
		c, err = NewNsqdRpcClient(net.JoinHostPort(n.NodeIp, n.RpcPort), RPC_TIMEOUT)
		if err != nil {
			return nil, err
		}
		self.nsqdRpcClients[nid] = c
	}
	return c, err
}

func (self *NsqLookupCoordinator) notifyEnableTopicWrite(topicInfo *TopicPartionMetaInfo) error {
	if state, ok := self.joinISRState[topicInfo.Name+strconv.Itoa(topicInfo.Partition)]; ok {
		if state.waitingJoin {
			return ErrWaitingJoinISR
		}
	}
	for _, node := range topicInfo.ISR {
		if node == topicInfo.Leader {
			continue
		}
		c, err := self.acquireRpcClient(node)
		if err != nil {
			return err
		}
		err = c.EnableTopicWrite(self.leaderNode.Epoch, topicInfo)
		if err != nil {
			return err
		}
	}
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return err
	}
	err = c.EnableTopicWrite(self.leaderNode.Epoch, topicInfo)
	return err
}

// each time change leader or isr list, make sure disable write.
// Because we need make sure the new leader and isr is in sync before accepting the
// write request.
func (self *NsqLookupCoordinator) notifyLeaderDisableTopicWrite(topicInfo *TopicPartionMetaInfo) error {
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return err
	}
	err = c.DisableTopicWrite(self.leaderNode.Epoch, topicInfo)
	return err
}

func (self *NsqLookupCoordinator) notifyISRDisableTopicWrite(topicInfo *TopicPartionMetaInfo) error {
	for _, node := range topicInfo.ISR {
		if node == topicInfo.Leader {
			continue
		}
		c, err := self.acquireRpcClient(node)
		if err != nil {
			return err
		}
		err = c.DisableTopicWrite(self.leaderNode.Epoch, topicInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *NsqLookupCoordinator) getNsqdLastCommitLogID(nid string, topicInfo *TopicPartionMetaInfo) (int64, error) {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		return 0, err
	}
	logid, err := c.GetLastCommmitLogID(topicInfo)
	return logid, err
}

func (self *NsqLookupCoordinator) getExcludeNodesForTopic(topicInfo *TopicPartionMetaInfo) map[string]struct{} {
	excludeNodes := make(map[string]struct{})
	excludeNodes[topicInfo.Leader] = struct{}{}
	for _, v := range topicInfo.ISR {
		excludeNodes[v] = struct{}{}
	}
	for _, v := range topicInfo.CatchupList {
		excludeNodes[v] = struct{}{}
	}
	// TODO: exclude other partition node with the same topic
	return excludeNodes
}

// find any nsqd node which has the topic-partition data, whether it is in sync.
func (self *NsqLookupCoordinator) getTopicDataNodes(topicInfo *TopicPartionMetaInfo) []NsqdNodeInfo {
	nsqdNodes := make([]NsqdNodeInfo, 0)
	return nsqdNodes
}

func (self *NsqLookupCoordinator) IsTopicLeader(topic string, partition int, nid string) bool {
	info, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed :%v", err)
		return false
	}
	return info.Leader == nid
}

func (self *NsqLookupCoordinator) IsTopicISRNode(topic string, partition int, nid string) bool {
	info, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		return false
	}
	if FindSlice(info.ISR, nid) == -1 {
		return false
	}
	return true
}

func (self *NsqLookupCoordinator) AllocNodeForTopic(topicInfo *TopicPartionMetaInfo) (*NsqdNodeInfo, error) {
	// collect the nsqd data, check if any node has the topic data already.
	var chosenNode *NsqdNodeInfo
	var oldDataList []NsqdNodeInfo

	maxLogID := int64(0)
	// first check if any node has the data of this topic.
	oldDataList = self.getTopicDataNodes(topicInfo)
	excludeNodes := self.getExcludeNodesForTopic(topicInfo)

	for _, n := range oldDataList {
		if _, ok := excludeNodes[n.GetID()]; ok {
			continue
		}

		cid, err := self.getNsqdLastCommitLogID(n.GetID(), topicInfo)
		if err != nil {
			continue
		}
		if cid > maxLogID {
			maxLogID = cid
			chosenNode = &n
		}
	}
	if maxLogID > 0 {
		return chosenNode, nil
	}
	var chosenStat *NodeTopicStats
	for nodeID, nodeInfo := range self.nsqdNodes {
		if _, ok := excludeNodes[nodeID]; ok {
			continue
		}
		topicStat, err := self.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("failed to get topic status for this node: %v", nodeInfo)
			continue
		}
		if chosenNode == nil {
			chosenNode = &nodeInfo
			chosenStat = topicStat
			continue
		}
		if topicStat.SlaveLessLoader(chosenStat) {
			chosenNode = &nodeInfo
			chosenStat = topicStat
		}
	}
	if chosenNode == nil {
		return nil, ErrNodeUnavailable
	}
	return chosenNode, nil
}

func (self *NsqLookupCoordinator) getNsqdTopicStat(node NsqdNodeInfo) (*NodeTopicStats, error) {
	c, err := self.acquireRpcClient(node.GetID())
	if err != nil {
		return nil, err
	}
	return c.GetTopicStats("")
}

// check period for the data balance.
func (self *NsqLookupCoordinator) balanceTopicData(leaderID string, epoch int) {
	ticker := time.NewTicker(time.Minute * 10)
	defer func() {
		ticker.Stop()
		coordLog.Infof("balance check exit.")
	}()
	for {
		select {
		case <-ticker.C:
			if self.leaderNode.GetID() != leaderID ||
				self.leaderNode.Epoch != epoch {
				return
			}

			avgLoad := 0.0
			minLoad := 0.0
			_ = minLoad
			maxLoad := 0.0
			_ = maxLoad
			// if max load is 4 times more than avg load, we need move some
			// leader from max to min load node one by one.
			// if min load is 4 times less than avg load, we can move some
			// leader to this min load node.
			_ = avgLoad
			// check each node
			for nodeID, nodeInfo := range self.nsqdNodes {
				topicStat, err := self.getNsqdTopicStat(nodeInfo)
				if err != nil {
					coordLog.Infof("failed to get node topic status while checking balance: %v", nodeID)
					continue
				}
				_, leaderLF := topicStat.GetNodeLeaderLoadFactor()
				coordLog.Infof("nsqd node %v load factor is : %v, %v", nodeID, leaderLF, topicStat.GetNodeLoadFactor())
			}
		}
	}
}

// init leader node and isr list for the empty topic
func (self *NsqLookupCoordinator) AllocTopicLeaderAndISR(replica int) (string, []string, error) {
	if len(self.nsqdNodes) < replica {
		return "", nil, ErrNodeUnavailable
	}
	nodeTopicStats := make([]NodeTopicStats, 0, len(self.nsqdNodes))
	var minLeaderStat *NodeTopicStats
	for _, nodeInfo := range self.nsqdNodes {
		stats, err := self.getNsqdTopicStat(nodeInfo)
		if err != nil {
			continue
		}
		nodeTopicStats = append(nodeTopicStats, *stats)
		if minLeaderStat == nil {
			minLeaderStat = stats
		} else if stats.LeaderLessLoader(minLeaderStat) {
			minLeaderStat = stats
		}
	}
	isrlist := make([]string, 0, replica)
	isrlist = append(isrlist, minLeaderStat.NodeID)
	slaveSort := func(l, r *NodeTopicStats) bool {
		return l.SlaveLessLoader(r)
	}
	By(slaveSort).Sort(nodeTopicStats)
	for _, s := range nodeTopicStats {
		if s.NodeID == minLeaderStat.NodeID {
			continue
		}
		isrlist = append(isrlist, s.NodeID)
		if len(isrlist) >= replica {
			break
		}
	}
	coordLog.Infof("topic selected isr : %v", isrlist)
	return isrlist[0], isrlist, nil
}

func (self *NsqLookupCoordinator) CreateTopic(topic string, partitionNum int, replica int) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotNsqLookupLeader
	}

	if ok, _ := self.leadership.IsExistTopic(topic); !ok {
		err := self.leadership.CreateTopic(topic, partitionNum, replica)
		if err != nil {
			coordLog.Infof("create topic key %v failed :%v", topic, err)
			return err
		}
	}

	for i := 0; i < partitionNum; i++ {
		err := RetryWithTimeout(func() error {
			err := self.leadership.CreateTopicPartition(topic, i, replica)
			if err != nil {
				coordLog.Warningf("failed to create topic %v-%v: %v", topic, i, err.Error())
			}
			if err == ErrAlreadyExist {
				return nil
			}
			return err
		})
		if err != nil {
			return err
		} else {
			go self.watchTopicLeaderSession(self.leaderNode.GetID(), self.leaderNode.Epoch, topic, i)
		}
	}
	for i := 0; i < partitionNum; i++ {
		leader, ISRList, err := self.AllocTopicLeaderAndISR(replica)
		if err != nil {
			coordLog.Infof("failed alloc nodes for topic: %v", err)
			return err
		}
		var tmpTopicInfo TopicPartionMetaInfo

		tmpTopicInfo.Name = topic
		tmpTopicInfo.Partition = i
		tmpTopicInfo.Replica = replica
		tmpTopicInfo.ISR = ISRList
		tmpTopicInfo.Leader = leader

		err = self.leadership.UpdateTopicNodeInfo(topic, i, &tmpTopicInfo, tmpTopicInfo.Epoch)
		if err != nil {
			coordLog.Infof("failed update info for topic : %v-%v, %v", topic, i, err)
			continue
		}
		self.notifyTopicMetaInfo(&tmpTopicInfo)
		self.notifyEnableTopicWrite(&tmpTopicInfo)
	}
	return nil
}

// some failed rpc means lost, we should always try to notify to the node when they are available
func (self *NsqLookupCoordinator) rpcFailRetryFunc(leaderID string, epoch int) {
	ticker := time.NewTicker(time.Second)
	failList := make([]RpcFailedInfo, 0)
	for {
		select {
		case <-ticker.C:
			if self.leaderNode.GetID() != leaderID ||
				self.leaderNode.Epoch != epoch {
				return
			}

			failList = append(failList, self.failedRpcList...)
			self.failedRpcList = self.failedRpcList[0:0]
			for _, info := range failList {
				topicInfo, err := self.leadership.GetTopicInfo(info.topic, info.partition)
				if err != nil {
					// TODO: ignore if not exist on etcd
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				c, err := self.acquireRpcClient(info.nodeID)
				if err != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				err = c.UpdateTopicInfo(self.leaderNode.Epoch, topicInfo)
				if err != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				leaderSession, err := self.leadership.GetTopicLeaderSession(info.topic, info.partition)
				if err != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				err = c.NotifyTopicLeaderSession(self.leaderNode.Epoch, topicInfo, leaderSession)
				if err != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
				err = c.UpdateCatchupForTopic(self.leaderNode.Epoch, topicInfo)
				if err != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
			}
			failList = failList[0:0]
		}
	}
}

func (self *NsqLookupCoordinator) doNotifyToNsqdNodes(nodes []string, notifyRpcFunc func(string) error) error {
	for _, n := range nodes {
		node := self.nsqdNodes[n]
		err := RetryWithTimeout(func() error {
			return notifyRpcFunc(node.GetID())
		})
		if err != nil {
			coordLog.Infof("notify to nsqd node %v failed.", node)
		}
	}
	return nil
}

func (self *NsqLookupCoordinator) doNotifyToSingleNsqdNode(nodeID string, notifyRpcFunc func(string) error) error {
	node := self.nsqdNodes[nodeID]
	err := RetryWithTimeout(func() error {
		return notifyRpcFunc(node.GetID())
	})
	if err != nil {
		coordLog.Infof("notify to nsqd node %v failed.", node)
	}
	return err
}

func (self *NsqLookupCoordinator) doNotifyToTopicLeaderThenOthers(leader string, others []string, notifyRpcFunc func(string) error) error {
	err := self.doNotifyToSingleNsqdNode(leader, notifyRpcFunc)
	if err != nil {
		coordLog.Infof("notify to topic leader %v failed.", leader)
		return err
	}
	return self.doNotifyToNsqdNodes(others, notifyRpcFunc)
}

func (self *NsqLookupCoordinator) notifyTopicLeaderSession(topicInfo *TopicPartionMetaInfo, leaderSession *TopicLeaderSession) error {
	coordLog.Infof("notify topic leader session changed: %v, %v", topicInfo.GetTopicDesp(), leaderSession.Session)
	others := getOthersExceptLeader(topicInfo)
	err := self.doNotifyToTopicLeaderThenOthers(topicInfo.Leader, others, func(nid string) error {
		return self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nid, topicInfo, leaderSession)
	})
	return err
}

func (self *NsqLookupCoordinator) notifyTopicMetaInfo(topicInfo *TopicPartionMetaInfo) error {
	others := getOthersExceptLeader(topicInfo)
	return self.doNotifyToTopicLeaderThenOthers(topicInfo.Leader, others, func(nid string) error {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
}

func (self *NsqLookupCoordinator) notifyCatchupList(topicInfo *TopicPartionMetaInfo) error {
	return self.doNotifyToNsqdNodes(topicInfo.CatchupList, func(nid string) error {
		return self.sendUpdateCatchupToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
	return nil
}

func (self *NsqLookupCoordinator) notifyOldNsqdsForTopicMetaInfo(topicInfo *TopicPartionMetaInfo, oldNodes []string) error {
	return self.doNotifyToNsqdNodes(oldNodes, func(nid string) error {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
}

func (self *NsqLookupCoordinator) notifySingleNsqdForTopicReload(topicInfo *TopicPartionMetaInfo, nodeID string) error {
	err := self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nodeID, topicInfo)
	if err != nil {
		return err
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nodeID, topicInfo, leaderSession)
	self.sendUpdateCatchupToNsqd(self.leaderNode.Epoch, nodeID, topicInfo)
	return nil
}

func (self *NsqLookupCoordinator) notifyAllNsqdsForTopicReload(topicInfo *TopicPartionMetaInfo) error {
	err := self.notifyTopicMetaInfo(topicInfo)
	if err != nil {
		return err
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	if err == nil {
		self.notifyTopicLeaderSession(topicInfo, leaderSession)
	} else {
		coordLog.Infof("get leader session failed: %v", err)
	}
	self.notifyCatchupList(topicInfo)
	return nil
}

func (self *NsqLookupCoordinator) addRetryFailedRpc(topic string, partition int, nid string) {
	failed := RpcFailedInfo{
		nodeID:    nid,
		topic:     topic,
		partition: partition,
		failTime:  time.Now(),
	}
	self.failedRpcList = append(self.failedRpcList, failed)
}

func (self *NsqLookupCoordinator) sendTopicLeaderSessionToNsqd(epoch int, nid string, topicInfo *TopicPartionMetaInfo, leaderSession *TopicLeaderSession) error {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
		return err
	}
	err = c.NotifyTopicLeaderSession(epoch, topicInfo, leaderSession)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return err
}

func (self *NsqLookupCoordinator) sendTopicInfoToNsqd(epoch int, nid string, topicInfo *TopicPartionMetaInfo) error {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
		return err
	}
	err = c.UpdateTopicInfo(epoch, topicInfo)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return err
}

func (self *NsqLookupCoordinator) sendUpdateCatchupToNsqd(epoch int, nid string, topicInfo *TopicPartionMetaInfo) error {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
		return err
	}
	err = c.UpdateCatchupForTopic(epoch, topicInfo)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return err
}

func (self *NsqLookupCoordinator) handleRequestJoinCatchup(topic string, partition int, nid string) *CoordErr {
	var topicInfo *TopicPartionMetaInfo
	var err error
	topicInfo, err = self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.ISR, nid) != -1 {
		return &CoordErr{"catchup node should not in the isr", RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.CatchupList, nid) == -1 {
		topicInfo.CatchupList = append(topicInfo.CatchupList, nid)
		err = self.leadership.UpdateTopicCatchupList(topic, partition, topicInfo.CatchupList, topicInfo.Epoch)
		if err != nil {
			coordLog.Infof("failed to update catchup list: %v", err)
			return &CoordErr{err.Error(), RpcCommonErr, CoordNetErr}
		}
	}
	go self.notifyCatchupList(topicInfo)
	return nil
}

func (self *NsqLookupCoordinator) handleRequestJoinISR(topic string, partition int, nodeID string) (string, *CoordErr) {
	// 1. got join isr request, check valid, should be in catchup list.
	// 2. notify the topic leader disable write
	// 3. add the node to ISR and remove from
	// CatchupList.
	// 4. insert wait join session, notify all nodes for the new isr
	// 5. wait on the join session until all the new isr is ready (got the ready notify from isr)
	// 6. timeout or done, clear current join session, (only keep isr that got ready notify, shoud be quorum), enable write
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v", err)
		return "", &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if FindSlice(topicInfo.CatchupList, nodeID) == -1 {
		coordLog.Infof("join isr node is not in catchup list.")
		return "", ErrJoinISRInvalid
	}
	state, ok := self.joinISRState[topicInfo.GetTopicDesp()]
	if !ok {
		state = &JoinISRState{}
		self.joinISRState[topicInfo.GetTopicDesp()] = state
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get leader session: %v", err)
		return "", &CoordErr{err.Error(), RpcCommonErr, CoordElectionTmpErr}
	}
	state.Lock()
	defer state.Unlock()
	if state.waitingJoin {
		coordLog.Warningf("failed request join isr because another is joining.")
		return "", ErrJoinISRInvalid
	}
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}

	err = self.notifyLeaderDisableTopicWrite(topicInfo)
	if err != nil {
		coordLog.Infof("try disable write for topic failed: %v", topicInfo.GetTopicDesp())
		return "", &CoordErr{err.Error(), RpcCommonErr, CoordElectionTmpErr}
	}

	state.waitingJoin = true
	state.waitingStart = time.Now()

	newCatchupList := make([]string, 0)
	for _, nid := range topicInfo.CatchupList {
		if nid == nodeID {
			continue
		}
		newCatchupList = append(newCatchupList, nid)
	}
	topicInfo.CatchupList = newCatchupList
	err = self.leadership.UpdateTopicCatchupList(topicInfo.Name, topicInfo.Partition, topicInfo.CatchupList, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update catchup failed: %v", err)
		// continue here to allow the wait goroutine to handle the timeout
	}

	topicInfo.ISR = append(topicInfo.ISR, nodeID)
	err = self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, topicInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("move catchup node to isr failed: %v", err)
		// continue here to allow the wait goroutine to handle the timeout
	}

	state.waitingSession = topicInfo.Leader
	for _, s := range topicInfo.ISR {
		state.waitingSession += s
	}
	state.waitingSession += strconv.Itoa(int(topicInfo.Epoch)) + "-" + strconv.Itoa(int(leaderSession.LeaderEpoch))
	state.waitingSession += state.waitingStart.String()

	state.doneChan = make(chan struct{})
	state.readyNodes = make(map[string]struct{})

	go self.waitForFinalSyncedISR(*topicInfo, *leaderSession, nodeID, state)

	return state.waitingSession, nil
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
	state, ok := self.joinISRState[topicInfo.GetTopicDesp()]
	if !ok {
		coordLog.Warningf("failed join isr because the join state is not set: %v", topicInfo.GetTopicDesp())
		return ErrJoinISRInvalid
	}
	state.Lock()
	defer state.Unlock()
	if !state.waitingJoin || state.waitingSession != joinISRSession {
		coordLog.Infof("state mismatch: %v", state, joinISRSession)
		return ErrJoinISRInvalid
	}

	coordLog.Infof("topic %v isr node %v ready for new state", topicInfo.GetTopicDesp(), nodeID)
	state.readyNodes[nodeID] = struct{}{}
	for _, n := range topicInfo.ISR {
		if _, ok := state.readyNodes[n]; !ok {
			coordLog.Infof("node %v still waiting ready", n)
			return nil
		}
	}
	coordLog.Infof("topic %v isr new state is ready for all: %v", topicInfo.GetTopicDesp(), state)
	err = self.notifyEnableTopicWrite(topicInfo)
	if err != nil {
		coordLog.Warningf("failed to enable write for topic: %v ", topicInfo.GetTopicDesp())
		return nil
	}
	state.waitingJoin = false
	state.waitingSession = ""
	if state.doneChan != nil {
		close(state.doneChan)
		state.doneChan = nil
	}
	return nil
}

func (self *NsqLookupCoordinator) resetJoinISRState(topicInfo *TopicPartionMetaInfo, state *JoinISRState, updateISR bool) error {
	state.Lock()
	defer state.Unlock()
	if !state.waitingJoin {
		return nil
	}
	state.waitingJoin = false
	state.waitingSession = ""
	coordLog.Infof("reset waiting join state: %v", state.waitingSession)
	ready := 0
	for _, n := range topicInfo.ISR {
		if _, ok := state.readyNodes[n]; ok {
			ready++
		}
	}

	if ready <= topicInfo.Replica/2 {
		coordLog.Infof("no enough ready isr while reset wait join: %v, expect: %v, actual: %v", state.waitingSession, topicInfo.ISR, state.readyNodes)
		// even timeout we can not enable this topic since no enough replicas
		// however, we should clear the join state so that we can try join new isr again
	} else {
		// some of isr failed to ready for the new isr state, we need rollback the new isr with the
		// isr got ready.
		coordLog.Infof("the join state: expect ready isr : %v, actual ready: %v ", topicInfo.ISR, state.readyNodes)
		if updateISR && ready != len(topicInfo.ISR) {
			topicInfo.ISR = make([]string, 0, len(state.readyNodes))
			for n, _ := range state.readyNodes {
				topicInfo.ISR = append(topicInfo.ISR, n)
			}
			err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, topicInfo, topicInfo.Epoch)
			if err != nil {
				coordLog.Infof("update topic info failed: %v", err)
				return err
			}
		}
		err := self.notifyEnableTopicWrite(topicInfo)
		if err != nil {
			coordLog.Warningf("failed to enable write :%v", topicInfo.GetTopicDesp())
		}
	}

	return nil
}

func (self *NsqLookupCoordinator) waitForFinalSyncedISR(topicInfo TopicPartionMetaInfo, leaderSession TopicLeaderSession, nodeID string, state *JoinISRState) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		coordLog.Infof("wait timeout for sync isr.")
	case <-state.doneChan:
		return
	}

	self.resetJoinISRState(&topicInfo, state, true)
}

func (self *NsqLookupCoordinator) transferTopicLeader(topicInfo *TopicPartionMetaInfo) error {
	if len(topicInfo.ISR) < 2 {
		return ErrNodeUnavailable
	}
	// try
	newLeader, newestLogID, err := self.chooseNewLeaderFromISR(topicInfo)
	if err != nil {
		return err
	}
	if newLeader == "" {
		return ErrLeaderElectionFail
	}
	err = self.notifyLeaderDisableTopicWrite(topicInfo)
	if err != nil {
		coordLog.Infof("disable write failed while transfer leader: %v", err)
		return err
	}
	defer self.notifyEnableTopicWrite(topicInfo)
	newLeader, newestLogID, err = self.chooseNewLeaderFromISR(topicInfo)
	if err != nil {
		return err
	}
	err = self.makeNewTopicLeaderAcknowledged(topicInfo, newLeader, newestLogID)

	return err
}

func (self *NsqLookupCoordinator) handleLeaveFromISR(topic string, partition int, leader *TopicLeaderSession, nodeID string) *CoordErr {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed :%v", err)
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}
	if topicInfo.Leader == nodeID {
		coordLog.Infof("the leader node %v will leave the isr, prepare transfer leader", nodeID)
		self.transferTopicLeader(topicInfo)
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		return nil
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node, leaving should wait.")
		// TODO: notify catchup and wait join isr
		return ErrLeavingISRWait
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
	newISR := make([]string, 0, len(topicInfo.ISR)-1)
	for _, n := range topicInfo.ISR {
		if n == nodeID {
			continue
		}
		newISR = append(newISR, n)
	}
	topicInfo.ISR = newISR
	topicInfo.CatchupList = append(topicInfo.CatchupList, nodeID)
	err = self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, topicInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("remove node from isr failed: %v", err.Error())
		return &CoordErr{err.Error(), RpcCommonErr, CoordCommonErr}
	}

	self.notifyTopicMetaInfo(topicInfo)
	coordLog.Infof("node %v removed by plan from topic isr: %v", nodeID, topicInfo.GetTopicDesp())
	return nil
}
