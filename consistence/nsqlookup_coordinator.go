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
	ErrNotLeader          = errors.New("Not leader")
	ErrLeaderElectionFail = errors.New("Leader election failed.")
	ErrNodeNotFound       = errors.New("node not found")
	ErrJoinISRInvalid     = errors.New("Join ISR failed")
	ErrJoinISRTimeout     = errors.New("Join ISR timeout")
	ErrWaitingJoinISR     = errors.New("The topic is waiting node to join isr")
	ErrLeavingISRAsLeader = errors.New("node should not leave the isr as leader")
)

func MergeList(l []string, r []string) []string {
	tmp := make(map[string]struct{})
	for _, v := range l {
		tmp[v] = struct{}{}
	}
	for _, v := range r {
		tmp[v] = struct{}{}
	}
	ret := make([]string, 0, len(tmp))
	for k, _ := range tmp {
		ret = append(ret, k)
	}
	return ret
}

func FilterList(l []string, filter []string) []string {
	tmp := make(map[string]struct{})
	for _, v := range l {
		tmp[v] = struct{}{}
	}
	for _, v := range filter {
		delete(tmp, v)
	}
	ret := make([]string, 0, len(tmp))
	for k, _ := range tmp {
		ret = append(ret, k)
	}
	return ret
}

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
	doneChan       chan struct{}
}

type RpcFailedInfo struct {
	nodeID    string
	topic     string
	partition int
	failTime  time.Time
}

type NSQLookupdCoordinator struct {
	clusterKey       string
	myNode           NsqLookupdNodeInfo
	leaderNode       NsqLookupdNodeInfo
	leadership       NSQLookupdLeadership
	nsqdNodes        map[string]NsqdNodeInfo
	nsqdRpcClients   map[string]*NsqdRpcClient
	nsqdNodeFailChan chan struct{}
	stopChan         chan struct{}
	joinISRState     map[string]*JoinISRState
	failedRpcList    []RpcFailedInfo
	quitChan         chan struct{}
}

func NewNSQLookupdCoordinator(cluster string, n *NsqLookupdNodeInfo, log levellogger.Logger) *NSQLookupdCoordinator {
	coordLog.logger = log
	return &NSQLookupdCoordinator{
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
}

func (self *NSQLookupdCoordinator) SetLeadershipMgr(l NSQLookupdLeadership) {
	self.leadership = l
}

func RetryWithTimeout(fn func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = time.Second * 30
	bo.MaxInterval = time.Second * 5
	return backoff.Retry(fn, bo)
}

// init and register to leader server
func (self *NSQLookupdCoordinator) Start() error {
	self.leadership.InitClusterID(self.clusterKey)
	err := self.leadership.Register(self.myNode)
	if err != nil {
		coordLog.Warningf("failed to start nsqlookupd coordinator: %v", err)
		return err
	}
	go self.handleLeadership()
	return nil
}

func (self *NSQLookupdCoordinator) Stop() {
	close(self.stopChan)
	<-self.quitChan
}

func (self *NSQLookupdCoordinator) handleLeadership() {
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
				coordLog.Infof("lookup leader changed from %v to %v", self.leaderNode.GetID(), l.GetID())
				self.leaderNode = *l
				go self.notifyLeaderChanged()
			}
			if self.leaderNode.GetID() == "" {
				coordLog.Warningln("leader is lost.")
			}
		}
	}
}

func (self *NSQLookupdCoordinator) notifyLeaderChanged() {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("I am slave.")
		// remove watchers.
		return
	}
	// reload topic information
	err := RetryWithTimeout(func() error {
		newTopics, err := self.leadership.ScanTopics()
		if err != nil {
			coordLog.Errorf("load topic info failed: %v", err)
			return err
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
		return nil
	})
	if err != nil {
		coordLog.Errorf("load topic info failed: %v", err)
	}
	go self.handleNsqdNodes(self.leaderNode.GetID(), self.leaderNode.Epoch)
	go self.checkTopics(self.leaderNode.GetID(), self.leaderNode.Epoch)
	go self.rpcFailRetryFunc(self.leaderNode.GetID(), self.leaderNode.Epoch)
	go self.balanceTopicData(self.leaderNode.GetID(), self.leaderNode.Epoch)
}

// for the nsqd node that temporally lost, we need send the related topics to
// it .
func (self *NSQLookupdCoordinator) NotifyTopicsToSingleNsqdForReload(topics []TopicPartionMetaInfo, nodeID string) {
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

func (self *NSQLookupdCoordinator) NotifyTopicsToAllNsqdForReload(topics []TopicPartionMetaInfo) {
	for _, v := range topics {
		self.notifyAllNsqdsForTopicReload(&v)
		//TODO: check disable write on the nsqd and continue enable the write
	}
}

func (self *NSQLookupdCoordinator) handleNsqdNodes(leaderID string, epoch int) {
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

func (self *NSQLookupdCoordinator) watchTopicLeaderSession(leaderID string, epoch int, name string, pid int) {
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
			self.notifyTopicLeaderSession(name, pid, n)
		}
	}
}

func (self *NSQLookupdCoordinator) checkTopics(leaderID string, epoch int) {
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

func (self *NSQLookupdCoordinator) doCheckTopics(epoch int, waitingMigrateTopic map[string]map[int]time.Time) {
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
				self.notifyNsqdForTopic(&t)
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
func (self *NSQLookupdCoordinator) waitOldLeaderRelease(topicInfo *TopicPartionMetaInfo) error {
	err := RetryWithTimeout(func() error {
		_, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err == ErrSessionNotExist {
			return nil
		}
		return err
	})
	return err
}

func (self *NSQLookupdCoordinator) chooseNewLeaderFromISR(topicInfo *TopicPartionMetaInfo) (string, int64, error) {
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

func (self *NSQLookupdCoordinator) makeNewTopicLeaderAcknowledged(topicInfo *TopicPartionMetaInfo, newLeader string, newestLogID int64) error {
	topicInfo.Leader = newLeader
	err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, topicInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update topic node info failed: %v", err)
		return err
	}
	self.notifyNsqdForTopic(topicInfo)

	for {
		session, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err != nil {
			coordLog.Infof("topic leader session still missing")
			time.Sleep(time.Second)
			self.notifyNsqdForTopic(topicInfo)
		} else {
			coordLog.Infof("topic leader session found: %v", session)
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
			cid, err := self.getNsqdLastCommitLogID(replica, topicInfo)
			if err != nil {
				continue
			}
			if cid != newestLogID {
				coordLog.Infof("node in isr is still syncing: %v", replica)
				waiting = true
				break
			}
		}
		if !waiting {
			break
		}
		self.notifyNsqdForTopic(topicInfo)
		time.Sleep(time.Second)
	}

	// make sure new leader has all new channels
	err = self.notifyNsqdUpdateChannels(topicInfo)
	if err != nil {
		coordLog.Infof("update channels after election failed: ", err)
	}
	return err
}

func (self *NSQLookupdCoordinator) handleTopicLeaderElection(topicInfo *TopicPartionMetaInfo) error {
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

func (self *NSQLookupdCoordinator) handleTopicMigrate(topicInfo TopicPartionMetaInfo) {
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
	err := self.notifyNsqdForTopic(&topicInfo)
	if err != nil {
		coordLog.Infof("notify topic failed: %v", err.Error())
	}
	err = self.notifyNsqdUpdateCatchup(&topicInfo)
	if err != nil {
		coordLog.Infof("notify topic catchup failed: %v", err.Error())
	}
}

func (self *NSQLookupdCoordinator) acquireRpcClient(nid string) (*NsqdRpcClient, error) {
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

func (self *NSQLookupdCoordinator) notifyEnableTopicWrite(topicInfo *TopicPartionMetaInfo) error {
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
func (self *NSQLookupdCoordinator) notifyLeaderDisableTopicWrite(topicInfo *TopicPartionMetaInfo) error {
	c, err := self.acquireRpcClient(topicInfo.Leader)
	if err != nil {
		return err
	}
	err = c.DisableTopicWrite(self.leaderNode.Epoch, topicInfo)
	return err
}

func (self *NSQLookupdCoordinator) notifyISRDisableTopicWrite(topicInfo *TopicPartionMetaInfo) error {
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

func (self *NSQLookupdCoordinator) getNsqdLastCommitLogID(nid string, topicInfo *TopicPartionMetaInfo) (int64, error) {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		return 0, err
	}
	logid, err := c.GetLastCommmitLogID(topicInfo)
	return logid, err
}

func (self *NSQLookupdCoordinator) getExcludeNodesForTopic(topicInfo *TopicPartionMetaInfo) map[string]struct{} {
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
func (self *NSQLookupdCoordinator) getTopicDataNodes(topicInfo *TopicPartionMetaInfo) []NsqdNodeInfo {
	nsqdNodes := make([]NsqdNodeInfo, 0)
	return nsqdNodes
}

func (self *NSQLookupdCoordinator) IsTopicLeader(topic string, partition int, nid string) bool {
	info, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed :%v", err)
		return false
	}
	return info.Leader == nid
}

func (self *NSQLookupdCoordinator) IsTopicISRNode(topic string, partition int, nid string) bool {
	info, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		return false
	}
	if FindSlice(info.ISR, nid) == -1 {
		return false
	}
	return true
}

func (self *NSQLookupdCoordinator) AllocNodeForTopic(topicInfo *TopicPartionMetaInfo) (*NsqdNodeInfo, error) {
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

func (self *NSQLookupdCoordinator) getNsqdTopicStat(node NsqdNodeInfo) (*NodeTopicStats, error) {
	c, err := self.acquireRpcClient(node.GetID())
	if err != nil {
		return nil, err
	}
	return c.GetTopicStats("")
}

// check period for the data balance.
func (self *NSQLookupdCoordinator) balanceTopicData(leaderID string, epoch int) {
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
				coordLog.Infof("nsqd node load factor is : %v, %v", leaderLF, topicStat.GetNodeLoadFactor())
			}
		}
	}
}

// init leader node and isr list for the empty topic
func (self *NSQLookupdCoordinator) AllocTopicLeaderAndISR(replica int) (string, []string, error) {
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

func (self *NSQLookupdCoordinator) CreateTopic(topic string, partitionNum int, replica int) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotLeader
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
		self.notifyNsqdForTopic(&tmpTopicInfo)
		self.notifyEnableTopicWrite(&tmpTopicInfo)
	}
	return nil
}

func (self *NSQLookupdCoordinator) CreateChannelForAll(topic string, channel string) error {
	pnum, err := self.leadership.GetTopicPartitionNum(topic)
	if err != nil {
		return err
	}
	for i := 0; i < pnum; i++ {
		err = self.CreateChannel(topic, i, channel)
	}
	return err
}

func (self *NSQLookupdCoordinator) CreateChannel(topic string, partition int, channel string) error {
	if self.leaderNode.GetID() != self.myNode.GetID() {
		coordLog.Infof("not leader while create topic")
		return ErrNotLeader
	}

	if ok, _ := self.leadership.IsExistTopicPartition(topic, partition); !ok {
		return ErrTopicNotCreated
	}
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed to get info for topic: %v", err)
		return err
	}
	for _, v := range topicInfo.Channels {
		if v == channel {
			coordLog.Infof("channel already exist: %v", channel)
			self.notifyNsqdUpdateChannels(topicInfo)
			return nil
		}
	}
	err = self.leadership.CreateChannel(topic, partition, channel)
	if err != nil {
		coordLog.Infof("failed to create channel : %v for topic %v-%v", channel, topic, partition)
		return err
	}
	topicInfo.Channels = append(topicInfo.Channels, channel)
	err = self.notifyNsqdUpdateChannels(topicInfo)
	return err
}

// some failed rpc means lost, we should always try to notify to the node when they are available
func (self *NSQLookupdCoordinator) rpcFailRetryFunc(leaderID string, epoch int) {
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
				err = c.UpdateChannelsForTopic(self.leaderNode.Epoch, topicInfo)
				if err != nil {
					self.addRetryFailedRpc(info.topic, info.partition, info.nodeID)
					continue
				}
			}
			failList = failList[0:0]
		}
	}
}

func (self *NSQLookupdCoordinator) doNotifyToTopicISRNodes(topicInfo *TopicPartionMetaInfo, notifyRpcFunc func(string) error) error {
	node := self.nsqdNodes[topicInfo.Leader]
	err := RetryWithTimeout(func() error {
		err := notifyRpcFunc(node.GetID())
		return err
	})
	if err != nil {
		coordLog.Infof("notify to topic leader %v for topic %v failed.", node, topicInfo)
		return err
	}
	for _, n := range topicInfo.ISR {
		if n == topicInfo.Leader {
			continue
		}
		node := self.nsqdNodes[n]
		err := RetryWithTimeout(func() error {
			return notifyRpcFunc(node.GetID())
		})
		if err != nil {
			coordLog.Infof("notify to nsqd ISR node %v for topic %v failed.", node, topicInfo)
		}
	}
	return nil
}

func (self *NSQLookupdCoordinator) notifyTopicLeaderSession(topic string, partition int, leaderSession *TopicLeaderSession) error {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("failed get topic info : %v", err)
		return err
	}
	coordLog.Infof("notify topic leader session changed: %v-%v, %v", topic, partition, leaderSession.Session)
	err = self.doNotifyToTopicISRNodes(topicInfo, func(nid string) error {
		return self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nid, topicInfo, leaderSession)
	})

	return err
}

func (self *NSQLookupdCoordinator) notifyNsqdUpdateChannels(topicInfo *TopicPartionMetaInfo) error {
	err := self.doNotifyToTopicISRNodes(topicInfo, func(nid string) error {
		return self.sendUpdateChannelsToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})

	return err
}

func (self *NSQLookupdCoordinator) notifyNsqdUpdateCatchup(topicInfo *TopicPartionMetaInfo) error {
	for _, catchNode := range topicInfo.CatchupList {
		node, ok := self.nsqdNodes[catchNode]
		if !ok {
			coordLog.Infof("catchup node not found: %v", catchNode)
			continue
		}
		err := RetryWithTimeout(func() error {
			return self.sendUpdateCatchupToNsqd(self.leaderNode.Epoch, node.GetID(), topicInfo)
		})
		if err != nil {
			coordLog.Infof("notify to nsqd catchup node %v for topic %v failed.", node, topicInfo)
			return err
		}
	}
	return nil
}

func (self *NSQLookupdCoordinator) notifyNsqdForTopic(topicInfo *TopicPartionMetaInfo) error {
	return self.doNotifyToTopicISRNodes(topicInfo, func(nid string) error {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})
}

func (self *NSQLookupdCoordinator) notifySingleNsqdForTopicReload(topicInfo *TopicPartionMetaInfo, nodeID string) error {
	err := self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nodeID, topicInfo)
	if err != nil {
		return err
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nodeID, topicInfo, leaderSession)
	self.sendUpdateChannelsToNsqd(self.leaderNode.Epoch, nodeID, topicInfo)
	return nil
}

func (self *NSQLookupdCoordinator) notifyAllNsqdsForTopicReload(topicInfo *TopicPartionMetaInfo) error {
	err := self.doNotifyToTopicISRNodes(topicInfo, func(nid string) error {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, topicInfo)
	})

	if err != nil {
		return err
	}
	leaderSession, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
	self.notifyTopicLeaderSession(topicInfo.Name, topicInfo.Partition, leaderSession)
	self.notifyNsqdUpdateChannels(topicInfo)
	self.notifyNsqdUpdateCatchup(topicInfo)
	return nil
}

func (self *NSQLookupdCoordinator) addRetryFailedRpc(topic string, partition int, nid string) {
	failed := RpcFailedInfo{
		nodeID:    nid,
		topic:     topic,
		partition: partition,
		failTime:  time.Now(),
	}
	self.failedRpcList = append(self.failedRpcList, failed)
}

func (self *NSQLookupdCoordinator) sendTopicLeaderSessionToNsqd(epoch int, nid string, topicInfo *TopicPartionMetaInfo, leaderSession *TopicLeaderSession) error {
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

func (self *NSQLookupdCoordinator) sendTopicInfoToNsqd(epoch int, nid string, topicInfo *TopicPartionMetaInfo) error {
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

func (self *NSQLookupdCoordinator) sendUpdateCatchupToNsqd(epoch int, nid string, topicInfo *TopicPartionMetaInfo) error {
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

func (self *NSQLookupdCoordinator) sendUpdateChannelsToNsqd(epoch int, nid string, topicInfo *TopicPartionMetaInfo) error {
	c, err := self.acquireRpcClient(nid)
	if err != nil {
		return err
	}
	err = c.UpdateChannelsForTopic(epoch, topicInfo)
	if err != nil {
		self.addRetryFailedRpc(topicInfo.Name, topicInfo.Partition, nid)
	}
	return err
}

func (self *NSQLookupdCoordinator) handleRequestJoinCatchup(topic string, partition int, nid string) error {
	var topicInfo *TopicPartionMetaInfo
	err := RetryWithTimeout(func() error {
		var err error
		topicInfo, err = self.leadership.GetTopicInfo(topic, partition)
		if err != nil {
			return err
		}
		if FindSlice(topicInfo.CatchupList, nid) == -1 {
			topicInfo.CatchupList = append(topicInfo.CatchupList, nid)
			err = self.leadership.UpdateTopicCatchupList(topic, partition, topicInfo.CatchupList, topicInfo.Epoch)
			if err != nil {
				coordLog.Infof("failed to update catchup list: %v", err)
				return err
			}
		}
		return err
	})
	if err != nil {
		return err
	}
	self.notifyNsqdUpdateCatchup(topicInfo)
	return nil
}

func (self *NSQLookupdCoordinator) handleRequestJoinISR(topic string, partition int, nodeID string) (string, error) {
	// 1. got join isr request, check valid, should be in catchup list.
	// 2. notify the topic leader disable write
	// 3. wait the final sync notify , if timeout just go to end
	// 4. got final sync finished event, add the node to ISR and remove from
	// CatchupList.
	// 5. notify all nodes for the new isr
	// 6. enable write
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		return "", err
	}
	if FindSlice(topicInfo.CatchupList, nodeID) == -1 {
		coordLog.Infof("join isr node is not in catchup list.")
		return "", ErrJoinISRInvalid
	}
	if _, ok := self.joinISRState[topicInfo.GetTopicDesp()]; !ok {
		self.joinISRState[topicInfo.GetTopicDesp()] = &JoinISRState{}
	} else {
		if self.joinISRState[topicInfo.GetTopicDesp()].waitingJoin {
			coordLog.Warningf("failed request join isr because the join state is used.")
			return "", ErrJoinISRInvalid
		}
	}
	state := self.joinISRState[topicInfo.GetTopicDesp()]
	state.waitingJoin = true

	err = self.notifyLeaderDisableTopicWrite(topicInfo)
	if err != nil {
		coordLog.Infof("try disable write for topic failed: %v", topicInfo.GetTopicDesp())
		state.waitingJoin = false
		return "", err
	}

	state.waitingSession = time.Now().String()
	if state.doneChan != nil {
		close(state.doneChan)
	}

	state.doneChan = make(chan struct{})
	go self.waitForFinalSyncedISR(topic, partition, nodeID, state)
	return state.waitingSession, nil
}

func (self *NSQLookupdCoordinator) handleReadyForISR(topic string, partition int, nodeID string, leaderSession TopicLeaderSession, isr []string) error {
	session := leaderSession.Session
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed while sync isr.")
		return err
	}
	// check for state and should lock for the state to prevent others join isr.
	state, ok := self.joinISRState[topicInfo.GetTopicDesp()]
	if !ok {
		if FindSlice(topicInfo.ISR, nodeID) != -1 {
			self.notifyNsqdForTopic(topicInfo)
			return nil
		}

		coordLog.Warningf("failed join isr because the join state is not set.")
		return ErrJoinISRInvalid
	}
	state.Lock()
	defer state.Unlock()
	if !state.waitingJoin || state.waitingSession != session {
		return ErrJoinISRInvalid
	}

	defer self.notifyEnableTopicWrite(topicInfo)
	newCatchupList := make([]string, 0)
	for _, nid := range topicInfo.CatchupList {
		if nid == nodeID {
			continue
		}
		newCatchupList = append(newCatchupList, nid)
	}
	topicInfo.CatchupList = newCatchupList
	topicInfo.ISR = append(topicInfo.ISR, nodeID)
	err = self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, topicInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("move catchup node to isr failed")
		return err
	}

	self.notifyNsqdForTopic(topicInfo)
	err = self.leadership.UpdateTopicCatchupList(topicInfo.Name, topicInfo.Partition, topicInfo.CatchupList, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("update catchup failed")
		return err
	}
	self.notifyNsqdUpdateCatchup(topicInfo)

	coordLog.Infof("topic %v isr node added %v.", topicInfo.GetTopicDesp(), nodeID)
	state.waitingJoin = false
	state.waitingSession = ""
	close(state.doneChan)
	return nil
}

func (self *NSQLookupdCoordinator) waitForFinalSyncedISR(topic string, partition int, nodeID string, state *JoinISRState) {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		coordLog.Infof("wait timeout for sync isr.")
	case <-state.doneChan:
		return
	}

	state.Lock()
	defer state.Unlock()
	if !state.waitingJoin {
		return
	}
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed while sync isr.")
		return
	}
	state.waitingJoin = false
	state.waitingSession = ""
	err = self.notifyEnableTopicWrite(topicInfo)
	if err != nil {
		coordLog.Warningf("failed to enable write ")
	}
}

func (self *NSQLookupdCoordinator) transferTopicLeader(topicInfo *TopicPartionMetaInfo) error {
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

func (self *NSQLookupdCoordinator) handlePrepareLeaveFromISR(topic string, partition int, nodeID string) error {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed while sync isr.")
		return err
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		return nil
	}
	if len(topicInfo.ISR)-1 <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node, leaving should wait.")
		return ErrLeavingISRWait
	}
	if topicInfo.Leader == nodeID {
		coordLog.Infof("the leader node will leave the isr, prepare transfer leader")
		err := self.transferTopicLeader(topicInfo)
		return err
	}
	return nil
}

func (self *NSQLookupdCoordinator) handleLeaveFromISR(topic string, partition int, leader *TopicLeaderSession, nodeID string) error {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		coordLog.Infof("get topic info failed while sync isr.")
		return err
	}
	if topicInfo.Leader == nodeID {
		coordLog.Infof("node should not leave as leader, need transfer leader first.")
		return ErrLeavingISRAsLeader
	}
	if FindSlice(topicInfo.ISR, nodeID) == -1 {
		return nil
	}
	if len(topicInfo.ISR) <= topicInfo.Replica/2 {
		coordLog.Infof("no enough isr node, leaving should wait.")
		return ErrLeavingISRWait
	}

	if leader != nil {
		newestLeaderSession, err := self.leadership.GetTopicLeaderSession(topic, partition)
		if err != nil {
			return err
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
	err = self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, topicInfo, topicInfo.Epoch)
	if err != nil {
		coordLog.Infof("remove node from isr failed")
		return err
	}

	self.notifyNsqdForTopic(topicInfo)
	coordLog.Infof("node %v removed by plan from topic isr: %v", nodeID, topicInfo.GetTopicDesp())
	return nil
}
