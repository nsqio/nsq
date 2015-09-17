package consistence

import (
	"errors"
	"github.com/cenkalti/backoff"
	"github.com/golang/glog"
	"sort"
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
	NodeId string
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
		self.NodeId,
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

type NSQLookupdCoordinator struct {
	masterKey      string
	coordID        string
	leaderNode     NsqLookupdNodeInfo
	leadership     NSQLookupdLeadership
	nsqdNodes      map[string]NsqdNodeInfo
	nsqdRpcClients map[string]*NsqdRpcClient
}

func NewNSQLookupdCoordinator(id string) *NSQLookupdCoordinator {
	return &NSQLookupdCoordinator{
		masterKey:  "nsqlookupd-master-key",
		coordID:    id,
		leadership: &FakeNsqlookupLeadership{},
		nsqdNodes:  make(map[string]NsqdNodeInfo),
	}
}

func RetryWithTimeout(fn func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = time.Second * 30
	bo.MaxInterval = time.Second * 5
	return backoff.Retry(fn, bo)
}

// init and register to leader server
func (self *NSQLookupdCoordinator) Start() error {
	var n NsqLookupdNodeInfo
	n.ID = self.coordID
	n.NodeIp = "127.0.0.1"
	n.HttpPort = "3000"
	n.Epoch = 0
	err := self.leadership.Register(self.masterKey, n)
	if err != nil {
		glog.Warningf("failed to start nsqlookupd coordinator: %v", err)
		return err
	}
	go self.handleLeadership()
	return nil
}

func (self *NSQLookupdCoordinator) handleLeadership() {
	lookupdLeaderChan := make(chan NsqLookupdNodeInfo)
	go self.leadership.AcquireAndWatchLeader(self.masterKey, lookupdLeaderChan)
	for {
		select {
		case l, ok := <-lookupdLeaderChan:
			if !ok {
				glog.Warningf("leader chan closed.")
				return
			}
			if l.GetId() != self.leaderNode.GetId() ||
				l.Epoch != self.leaderNode.Epoch {
				glog.Infof("lookup leader changed from %v to %v", self.leaderNode.GetId(), l.GetId())
				self.leaderNode = l
				go self.notifyLeaderChanged()
			}
			if self.leaderNode.GetId() == "" {
				glog.Warningln("leader is lost.")
			}
		}
	}
}

func (self *NSQLookupdCoordinator) notifyLeaderChanged() {
	if self.leaderNode.GetId() != self.coordID {
		glog.Infof("I am slave.")
		// remove watchers.
		return
	}
	// reload topic information
	err := RetryWithTimeout(func() error {
		newTopics, err := self.leadership.ScanTopics()
		if err != nil {
			glog.Errorf("load topic info failed: %v", err)
			return err
		} else {
			newTopicsMap := make(map[string]map[int]TopicLeadershipInfo)
			for _, t := range newTopics {
				if _, ok := newTopicsMap[t.Name]; !ok {
					newTopicsMap[t.Name] = make(map[int]TopicLeadershipInfo)
				}
				newTopicsMap[t.Name][t.Partition] = t
				glog.Infof("found topic %v partition %v: %v", t.Name, t.Partition, t)
			}
			_ = newTopicsMap

			glog.Infof("topic loaded : %v", len(newTopics))
			self.NotifyTopicsToNsqdForReload(newTopics)
		}
		for _, t := range newTopics {
			go self.watchTopicLeaderSession(self.leaderNode.GetId(), self.leaderNode.Epoch, t.Name, t.Partition)
		}
		return nil
	})
	if err != nil {
		glog.Errorf("load topic info failed: %v", err)
	}
	go self.handleNsqdNodes(self.leaderNode.GetId(), self.leaderNode.Epoch)
	go self.checkTopics(self.leaderNode.GetId(), self.leaderNode.Epoch)
	go self.balanceTopicData(self.leaderNode.GetId(), self.leaderNode.Epoch)
}

func (self *NSQLookupdCoordinator) NotifyTopicsToNsqdForReload(topics []TopicLeadershipInfo) {
	for _, v := range topics {
		self.notifyNsqdForTopic(v)
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
		glog.Infof("stop watch the nsqd nodes.")
	}()
	for {
		select {
		case nodes, ok := <-nsqdNodesChan:
			if !ok {
				return
			}
			// check if any nsqd node changed.
			glog.Infof("Current nsqd nodes: %v", len(nodes))
			oldNodes := self.nsqdNodes
			newNodes := make(map[string]NsqdNodeInfo)
			for _, v := range nodes {
				glog.Infof("nsqd node %v : %v", v.GetId(), v)
				newNodes[v.GetId()] = v
			}
			self.nsqdNodes = newNodes
			for oldId, oldNode := range oldNodes {
				if _, ok := newNodes[oldId]; !ok {
					glog.Warningf("nsqd node failed: %v, %v", oldId, oldNode)
				}
			}
			for newId, newNode := range newNodes {
				if _, ok := oldNodes[newId]; !ok {
					glog.Infof("new nsqd node joined: %v, %v", newId, newNode)
				}
			}
		case <-ticker.C:
			if self.leaderNode.GetId() != leaderID ||
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
		glog.Infof("stop watch the topic leader session.")
	}()

	for {
		select {
		case <-ticker.C:
			if self.leaderNode.GetId() != leaderID ||
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
	waitMigrateInterval := time.Minute
	defer func() {
		ticker.Stop()
		glog.Infof("check topics quit.")
	}()

	for {
		select {
		case <-ticker.C:
			if self.leaderNode.GetId() != leaderID ||
				self.leaderNode.Epoch != epoch {
				return
			}

			topics, err := self.leadership.ScanTopics()
			if err != nil {
				glog.Infof("scan topics failed. %v", err)
				continue
			}
			// TODO: check partition number for topic, maybe failed to create
			// some partition when creating topic.
			for _, t := range topics {
				needMigrate := false
				if len(t.ISR) < t.Replica {
					glog.Infof("ISR is not enough for topic %v, isr is :%v", t.GetTopicDesp(), t.ISR)
					// notify the migrate goroutine to handle isr lost.
					needMigrate = true
				}
				if _, ok := self.nsqdNodes[t.Leader]; !ok {
					needMigrate = true
					glog.Warningf("topic %v leader %v is lost.", t.GetTopicDesp(), t.Leader)
				} else {
					// check topic leader session key.
					_, err := self.leadership.GetTopicLeaderSession(t.Name, t.Partition)
					if err != nil {
						glog.Infof("topic leader session %v not found.", t.GetTopicDesp())
						// notify the nsqd node to acquire the leader session.
						self.notifyNsqdForTopic(t)
					}
				}
				for _, replica := range t.ISR {
					if _, ok := self.nsqdNodes[replica]; !ok {
						glog.Warningf("topic %v isr node %v is lost.", t.GetTopicDesp(), replica)
						needMigrate = true
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
					if partitions[t.Partition].Before(time.Now().Add(-1 * waitMigrateInterval)) {
						glog.Infof("begin migrate the topic :%v", t.GetTopicDesp())
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
	}
}

// make sure the previous leader is not holding its leader session.
func (self *NSQLookupdCoordinator) waitOldLeaderRelease(topicInfo TopicLeadershipInfo) error {
	err := RetryWithTimeout(func() error {
		_, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err == ErrSessionNotExist {
			return nil
		}
		return err
	})
	return err
}

func (self *NSQLookupdCoordinator) handleTopicLeaderElection(topicInfo TopicLeadershipInfo) error {
	err := self.waitOldLeaderRelease(topicInfo)
	if err != nil {
		glog.Infof("Leader is not released: %v", topicInfo)
		return err
	}
	// choose another leader in ISR list, and add new node to ISR
	// list.
	// check if this is admin operation, if it is admin op, we can
	// just ignore this to wait admin operation.
	failed := 0
	commitIdList := make(map[int]int)
	newestReplicas := make([]string, 0)
	newestLogId := 0
	for _, replica := range topicInfo.ISR {
		if _, ok := self.nsqdNodes[replica]; !ok {
			continue
		}
		cid, err := self.getNsqdLastCommitLogId(topicInfo, replica)
		if err != nil {
			continue
		}
		if cid > newestLogId {
			newestReplicas = newestReplicas[0:0]
			newestReplicas = append(newestReplicas, replica)
			newestLogId = cid
		} else if cid == newestLogId {
			newestReplicas = append(newestReplicas, replica)
		}
	}
	// select the least load factor node
	newLeader := ""
	minLF := 100.0
	for _, replica := range newestReplicas {
		stat, err := self.getNsqdTopicStat(self.nsqdNodes[replica])
		_, lf := stat.GetNodeLeaderLoadFactor()
		if lf < minLF {
			newLeader = replica
		}
	}
	if newLeader == "" {
		glog.Warningf("No leader can be elected.")
		return ErrLeaderElectionFail
	}
	glog.Infof("new leader found %v with commit id: %v", newLeader, newestLogId)
	topicInfo.Leader = newLeader
	self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, &topicInfo)
	self.notifyNsqdForTopic(topicInfo)

	for {
		session, err := self.leadership.GetTopicLeaderSession(topicInfo.Name, topicInfo.Partition)
		if err != nil {
			glog.Infof("topic leader session still missing")
			time.Sleep(time.Second)
		} else {
			glog.Infof("topic leader session found: %v", session)
			break
		}
	}
	// check ISR sync state.
	// if synced, notify the leader to accept write.
	for {
		waiting := false
		for _, replica := range topicInfo.ISR {
			if _, ok := self.nsqdNodes[replica]; !ok {
				continue
			}
			cid, err := self.getNsqdLastCommitLogId(topicInfo, replica)
			if err != nil {
				continue
			}
			if cid != newestLogId {
				glog.Infof("node in isr is still syncing: %v", replica)
				waiting = true
				break
			}
		}
		if !waiting {
			break
		}
		time.Sleep(time.Second)
	}

	err = self.notifyEnableTopicWrite(topicInfo)
	if err != nil {
		glog.Infof("enable topic write failed: ", err)
	}
	return nil
}

func (self *NSQLookupdCoordinator) handleTopicMigrate(topicInfo TopicLeadershipInfo) {
	if _, ok := self.nsqdNodes[topicInfo.Leader]; !ok {
		err := self.handleTopicLeaderElection(topicInfo)
		if err != nil {
			glog.Warningf("topic leader election error : %v", err)
			return
		}
	}
	catchupList := make([]string, 0)
	for _, n := range topicInfo.CatchupList {
		if _, ok := self.nsqdNodes[n]; ok {
			catchupList = append(catchupList, n)
		} else {
			glog.Infof("topic %v catchup node %v is lost.", topicInfo.GetTopicDesp(), n)
		}
	}
	newISR := make([]string, 0)
	for _, replica := range topicInfo.ISR {
		if _, ok := self.nsqdNodes[replica]; !ok {
			glog.Warningf("topic %v isr node %v is lost.", topicInfo.GetTopicDesp(), replica)
		} else {
			newISR = append(newISR, replica)
		}
	}
	topicInfo.ISR = newISR
	topicNsqdNum := len(newISR) + len(catchupList)
	if topicNsqdNum < topicInfo.Replica {
		for i := topicNsqdNum; i < topicInfo.Replica; i++ {
			n, err := self.AllocNodeForTopic(topicInfo)
			if err != nil {
				catchupList = append(catchupList, n.GetId())
			}
		}
	}
	topicInfo.CatchupList = catchupList
	err := self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, &topicInfo)
	if err != nil {
		glog.Infof("update topic node info failed: %v", err.Error())
		return
	}
	var wg sync.WaitGroup
	nearlyCatchup := make(map[string]bool, len(catchupList))
	for _, catchNode := range catchupList {
		wg.Add(1)
		go func(nodeId string) {
			successCatchup := true
			defer func() {
				nearlyCatchup[nodeId] = successCatchup
				wg.Done()
			}()
			self.notifyNsqdForTopic(topicInfo)
			ticker := time.NewTicker(time.Second * 3)
			for {
				select {
				case <-ticker.C:
					cid, err := self.getNsqdLastCommitLogId(topicInfo, nodeId)
					if err != nil {
						glog.Infof("failed to get last commit : %v", nodeId)
						if _, ok := self.nsqdNodes[nodeId]; !ok {
							glog.Infof("catchup node lost while migrate: %v", nodeId)
							successCatchup = false
							return
						}
						continue
					}
					leaderCid, err := self.getNsqdLastCommitLogId(topicInfo, topicInfo.Leader)
					if err != nil {
						glog.Infof("failed to get last commit on leader: %v", topicInfo.Leader)
						if _, ok := self.nsqdNodes[topicInfo.Leader]; !ok {
							glog.Infof("leader node lost while migrate: %v", topicInfo.Leader)
							successCatchup = false
							return
						}
						continue
					}
					if leaderCid-cid < 0 {
						glog.Warningf("the catch log id should less than leader. %v vs %v", cid, leaderCid)
						successCatchup = false
						return
					}
					if leaderCid-cid > 10 {
						glog.Infof("catch up log id %v still fall behind leader log id : %v", cid, leaderCid)
						delete(nearlyCatchup, nodeId)
					} else {
						nearlyCatchup[nodeId] = true
						successCatchup = true
						glog.Infof("node %v is nearly catching up leader log", nodeId)
						if len(nearlyCatchup) >= len(catchupList) {
							return
						}
					}
				}
			}
		}(catchNode)
	}
	wg.Wait()
	if len(nearlyCatchup) < len(catchupList) {
		glog.Infof("Part of catchup nodes nearly catching up.")
	}
	if len(nearlyCatchup) == 0 {
		glog.Infof("No catchup node.")
		return
	}
	ticker := time.NewTicker(time.Second * 3)
	defer func() {
		ticker.Stop()
		self.notifyEnableTopicWrite(topicInfo)
	}()

	err = self.notifyDisableTopicWrite(topicInfo)
	if err != nil {
		glog.Infof("try disable write for topic failed: %v", topicInfo.GetTopicDesp())
		return
	}
	newISR = make([]string, 0)
	for len(nearlyCatchup) > 0 {
		select {
		case <-ticker.C:
			for nodeId, nearly := range nearlyCatchup {
				if !nearly {
					delete(nearlyCatchup, nodeId)
					continue
				}
				cid, _ := self.getNsqdLastCommitLogId(topicInfo, nodeId)
				leaderCid, _ := self.getNsqdLastCommitLogId(topicInfo, topicInfo.Leader)
				if leaderCid-cid < 0 {
					glog.Warningf("the catch log id should less than leader. %v vs %v", cid, leaderCid)
					return
				}
				if cid > 0 && leaderCid == cid {
					newISR = append(newISR, nodeId)
					delete(nearlyCatchup, nodeId)
				}
			}
		}
	}

	topicInfo.ISR = MergeList(topicInfo.ISR, newISR)
	topicInfo.CatchupList = FilterList(topicInfo.CatchupList, newISR)
	err = self.leadership.UpdateTopicNodeInfo(topicInfo.Name, topicInfo.Partition, &topicInfo)
	if err != nil {
		glog.Infof("move catchup node to isr failed")
		return
	}

	glog.Infof("topic %v migrate done.", topicInfo.GetTopicDesp())
}

func (self *NSQLookupdCoordinator) notifyEnableTopicWrite(topicInfo TopicLeadershipInfo) error {
	err := self.nsqdRpcClients[topicInfo.Leader].EnableTopicWrite(self.leaderNode.Epoch, &topicInfo)
	return err
}

// each time change leader or isr list, make sure disable write.
// Because we need make sure the new leader and isr is in sync before accepting the
// write request.
func (self *NSQLookupdCoordinator) notifyDisableTopicWrite(topicInfo TopicLeadershipInfo) error {
	err := self.nsqdRpcClients[topicInfo.Leader].DisableTopicWrite(self.leaderNode.Epoch, &topicInfo)
	return err
}

func (self *NSQLookupdCoordinator) getNsqdLastCommitLogId(topicInfo TopicLeadershipInfo, node string) (int, error) {
	return 0, nil
}

func (self *NSQLookupdCoordinator) getExcludeNodesForTopic(topicInfo TopicLeadershipInfo) map[string]struct{} {
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
func (self *NSQLookupdCoordinator) getTopicDataNodes(topicInfo TopicLeadershipInfo) []NsqdNodeInfo {
	nsqdNodes := make([]NsqdNodeInfo, 0)
	return nsqdNodes
}

func (self *NSQLookupdCoordinator) AllocNodeForTopic(topicInfo TopicLeadershipInfo) (*NsqdNodeInfo, error) {
	// collect the nsqd data, check if any node has the topic data already.
	var chosenNode *NsqdNodeInfo
	var oldDataList []NsqdNodeInfo

	maxLogId := 0
	// first check if any node has the data of this topic.
	oldDataList = self.getTopicDataNodes(topicInfo)
	excludeNodes := self.getExcludeNodesForTopic(topicInfo)

	for _, n := range oldDataList {
		if _, ok := excludeNodes[n.GetId()]; ok {
			continue
		}

		cid, err := self.getNsqdLastCommitLogId(topicInfo, n.GetId())
		if err != nil {
			continue
		}
		if cid > maxLogId {
			maxLogId = cid
			chosenNode = &n
		}
	}
	if maxLogId > 0 {
		return chosenNode, nil
	}
	var chosenStat *NodeTopicStats
	for nodeId, nodeInfo := range self.nsqdNodes {
		if _, ok := excludeNodes[nodeId]; ok {
			continue
		}
		topicStat, err := self.getNsqdTopicStat(nodeInfo)
		if err != nil {
			glog.Infof("failed to get topic status for this node: %v", nodeInfo)
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
	return self.nsqdRpcClients[node.GetId()].GetTopicStats("")
}

// check period for the data balance.
func (self *NSQLookupdCoordinator) balanceTopicData(leaderID string, epoch int) {
	ticker := time.NewTicker(time.Minute * 10)
	defer func() {
		ticker.Stop()
		glog.Infof("balance check exit.")
	}()
	for {
		select {
		case <-ticker.C:
			if self.leaderNode.GetId() != leaderID ||
				self.leaderNode.Epoch != epoch {
				return
			}

			avgLoad := 0.0
			minLoad := 0.0
			maxLoad := 0.0
			// if max load is 4 times more than avg load, we need move some
			// leader from max to min load node one by one.
			// if min load is 4 times less than avg load, we can move some
			// leader to this min load node.
			_ = avgLoad
			// check each node
			for nodeId, nodeInfo := range self.nsqdNodes {
				topicStat, err := self.getNsqdTopicStat(nodeInfo)
				if err != nil {
					glog.Infof("failed to get node topic status while checking balance: %v", nodeId)
					continue
				}
				_, leaderLF := topicStat.GetNodeLeaderLoadFactor()
				glog.Infof("nsqd node load factor is : %v, %v", leaderLF, topicStat.GetNodeLoadFactor())
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
	for nid, nodeInfo := range self.nsqdNodes {
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
	isrlist = append(isrlist, minLeaderStat.NodeId)
	slaveSort := func(l, r *NodeTopicStats) bool {
		return l.SlaveLessLoader(r)
	}
	By(slaveSort).Sort(nodeTopicStats)
	for _, s := range nodeTopicStats {
		if s.NodeId == minLeaderStat.NodeId {
			continue
		}
		isrlist = append(isrlist, s.NodeId)
		if len(isrlist) >= replica {
			break
		}
	}
	glog.Infof("topic selected isr : %v", isrlist)
	return isrlist[0], isrlist, nil
}

func (self *NSQLookupdCoordinator) CreateTopic(topic string, partitionNum int, replica int) error {
	if self.leaderNode.GetId() != self.coordID {
		glog.Infof("not leader while create topic")
		return ErrNotLeader
	}
	if len(self.nsqdNodes) < replica {
		return ErrNodeUnavailable
	}

	if ok, _ := self.leadership.IsExistTopic(topic); ok {
		return ErrAlreadyExist
	}
	// for each node, get the total topic data size,
	// we assume the more data the more throughput need.
	// The least load factor will became new leader for this topic. ( first filter the
	// storage not enough nodes)
	// Make data size MB, if has the same MB, the topic number is used.
	//

	err := self.leadership.CreateTopic(topic, partitionNum, replica)
	if err != nil {
		glog.Infof("create topic key %v failed :%v", topic, err)
		return err
	}
	for i := 0; i < partitionNum; i++ {
		err := RetryWithTimeout(func() error {
			err := self.leadership.CreateTopicPartition(topic, i, replica)
			if err != nil {
				glog.Warningf("failed to create topic %v-%v: %v", topic, i, err.Error())
			}
			return err
		})
		if err != nil {
			return err
		}
		go self.watchTopicLeaderSession(self.leaderNode.GetId(), self.leaderNode.Epoch, topic, i)
	}
	for i := 0; i < partitionNum; i++ {
		leader, ISRList, err := self.AllocTopicLeaderAndISR(replica)
		if err != nil {
			glog.Infof("failed alloc nodes for topic: %v", err)
			return err
		}
		var tmpTopicInfo TopicLeadershipInfo

		tmpTopicInfo.Name = topic
		tmpTopicInfo.Partition = i
		tmpTopicInfo.Replica = replica
		tmpTopicInfo.ISR = ISRList
		tmpTopicInfo.Leader = leader

		err = self.leadership.UpdateTopicNodeInfo(topic, i, &(tmpTopicInfo))
		if err != nil {
			glog.Infof("failed update info for topic : %v-%v, %v", topic, i, err)
			continue
		}
		self.notifyNsqdForTopic(tmpTopicInfo)
		self.notifyEnableTopicWrite(tmpTopicInfo)
	}
	return nil
}

func (self *NSQLookupdCoordinator) CreateChannel(topic string, partition int, channel string) error {
	if self.leaderNode.GetId() != self.coordID {
		glog.Infof("not leader while create topic")
		return ErrNotLeader
	}

	if ok, _ := self.leadership.IsExistTopicPartition(topic, partition); !ok {
		return ErrTopicNotCreated
	}
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		glog.Infof("failed to get info for topic: %v", err)
		return err
	}
	for _, v := range topicInfo.Channels {
		if v == channel {
			glog.Infof("channel already exist: %v", channel)
			self.notifyNsqdForTopic(*topicInfo)
			return nil
		}
	}
	err = self.leadership.CreateChannel(topic, partition, channel)
	if err != nil {
		glog.Infof("failed to create channel : %v for topic %v-%v", channel, topic, partition)
		return err
	}
	topicInfo.Channels = append(topicInfo.Channels, channel)
	self.notifyNsqdForTopic(*topicInfo)
	return nil

}

func (self *NSQLookupdCoordinator) notifyTopicLeaderSession(topic string, partition int, leaderSession *TopicLeaderSession) error {
	topicInfo, err := self.leadership.GetTopicInfo(topic, partition)
	if err != nil {
		glog.Infof("failed get topic info : %v", err)
		return err
	}
	glog.Infof("notify topic leader session to nsqd nodes: %v-%v, %v", topic, partition, leaderSession.leaderNode.GetId())
	err = self.doNotifyToTopicISRNodes(*topicInfo, func(nid string) error {
		return self.sendTopicLeaderSessionToNsqd(self.leaderNode.Epoch, nid, topicInfo, leaderSession)
	})

	return err
}

func (self *NSQLookupdCoordinator) sendTopicLeaderSessionToNsqd(epoch int, nid string, topicInfo *TopicLeadershipInfo, leaderSession *TopicLeaderSession) error {
	err := self.nsqdRpcClients[nid].NotifyTopicLeaderSession(epoch, topicInfo, leaderSession)
	return err
}

func (self *NSQLookupdCoordinator) sendTopicInfoToNsqd(epoch int, nid string, topicInfo *TopicLeadershipInfo) error {
	err := self.nsqdRpcClients[nid].UpdateTopicInfo(epoch, topicInfo)
	return err
}

func (self *NSQLookupdCoordinator) addCatchupToTopicOnNsqd(epoch int, nid string, topicInfo TopicLeadershipInfo) error {
	return nil
}

func (self *NSQLookupdCoordinator) addChannelForTopicOnNsqd(epoch int, nid string, topicInfo TopicLeadershipInfo) error {
	return nil
}

func (self *NSQLookupdCoordinator) doNotifyToTopicISRNodes(topicInfo TopicLeadershipInfo, notifyRpcFunc func(string) error) error {
	node := self.nsqdNodes[topicInfo.Leader]
	err := RetryWithTimeout(func() error {
		err := notifyRpcFunc(node.GetId())
		return err
	})
	if err != nil {
		glog.Infof("notify to topic leader %v for topic %v failed.", node, topicInfo)
	}
	for _, n := range topicInfo.ISR {
		node := self.nsqdNodes[n]
		err := RetryWithTimeout(func() error {
			return notifyRpcFunc(node.GetId())
		})
		if err != nil {
			glog.Infof("notify to nsqd ISR node %v for topic %v failed.", node, topicInfo)
		}
	}
	return nil
}

func (self *NSQLookupdCoordinator) notifyNsqdForTopic(topicInfo TopicLeadershipInfo) {
	err := self.doNotifyToTopicISRNodes(topicInfo, func(nid string) error {
		return self.sendTopicInfoToNsqd(self.leaderNode.Epoch, nid, &topicInfo)
	})

	for _, catchNode := range topicInfo.CatchupList {
		node, ok := self.nsqdNodes[catchNode]
		if !ok {
			glog.Infof("catchup node not found: %v", catchNode)
			continue
		}
		err := RetryWithTimeout(func() error {
			return self.addCatchupToTopicOnNsqd(self.leaderNode.Epoch, node.GetId(), topicInfo)
		})
		if err != nil {
			glog.Infof("notify to nsqd catchup node %v for topic %v failed.", node, topicInfo)
		}
	}
}
