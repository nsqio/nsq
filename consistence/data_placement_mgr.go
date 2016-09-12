package consistence

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// new topic can have an advice load factor to give the suggestion about the
// future load. While the topic data is small the load compute is not precise,
// so we use the advised load to determine the actual topic load.
// use 1~10 to advise from lowest to highest. The 1 or 10 should be used with much careful.

// TODO: support topic move from one to another by manual.

// left consume data size level: 0~1GB low, < 5GB medium, < 50GB high , >=50GB very high
// left data size level: <5GB low, <50GB medium , <200GB high, >=200GB very high

const (
	defaultTopicLoadFactor       = 3
	HIGHEST_PUB_QPS_LEVEL        = 100
	HIGHEST_LEFT_CONSUME_MB_SIZE = 50 * 1024
	HIGHEST_LEFT_DATA_MB_SIZE    = 200 * 1024
)

// pub qps level : 1~13 low (< 30 KB/sec), 13 ~ 51 medium (<1000 KB/sec), 51 ~ 74 high (<8 MB/sec), >74 very high (>8 MB/sec)
func convertQPSLevel(hourlyPubSize int64) float64 {
	qps := hourlyPubSize / 3600 / 1024
	if qps <= 3 {
		return 1.0 + float64(qps)
	} else if qps <= 30 {
		return 4.0 + float64(qps-3)/3
	} else if qps <= 300 {
		return 13.0 + float64(qps-30)/15
	} else if qps <= 1000 {
		return 31.0 + float64(qps-300)/35
	} else if qps <= 2000 {
		return 51.0 + math.Log2(1.0+float64(qps-1000))
	} else if qps <= 8000 {
		return 61.0 + math.Log2(1.0+float64(qps-2000))
	} else if qps <= 16000 {
		return 74.0 + math.Log2(1.0+float64(qps-8000))
	}
	return HIGHEST_PUB_QPS_LEVEL
}

func splitTopicPartitionID(topicFullName string) (string, int, error) {
	partIndex := strings.LastIndex(topicFullName, "-")
	if partIndex == -1 {
		return "", 0, fmt.Errorf("invalide topic full name: %v", topicFullName)
	}
	topicName := topicFullName[:partIndex]
	partitionID, err := strconv.Atoi(topicFullName[partIndex+1:])
	return topicName, partitionID, err
}

type NodeTopicStats struct {
	NodeID string
	// the data (MB) need to be consumed on the leader for all channels in the topic.
	ChannelDepthData map[string]int64
	// the data left on disk. unit: MB
	TopicLeaderDataSize map[string]int64
	TopicTotalDataSize  map[string]int64
	NodeCPUs            int

	// the pub stat for past 24hr
	TopicHourlyPubDataList map[string][24]int64
	ChannelNum             map[string]int
}

func NewNodeTopicStats(nid string, cap int, cpus int) *NodeTopicStats {
	return &NodeTopicStats{
		NodeID:                 nid,
		ChannelDepthData:       make(map[string]int64, cap),
		TopicLeaderDataSize:    make(map[string]int64, cap),
		TopicTotalDataSize:     make(map[string]int64, cap),
		TopicHourlyPubDataList: make(map[string][24]int64, cap),
		ChannelNum:             make(map[string]int, cap),
		NodeCPUs:               cpus,
	}
}

// the load factor is something like cpu load factor that
// stand for the busy/idle state for this node.
// the larger means busier.
// 60% recent avg load in 24hr + 30% left need to be consumed + 10% data size left

func (self *NodeTopicStats) GetNodeLoadFactor() (float64, float64) {
	perCpuStat, leaderLF := self.GetNodeLeaderLoadFactor()
	totalDataSize := int64(0)
	for _, v := range perCpuStat.TopicTotalDataSize {
		totalDataSize += v
	}
	totalDataSize += int64(len(perCpuStat.TopicTotalDataSize))
	if totalDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return leaderLF, leaderLF + float64(totalDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*5
}

func (self *NodeTopicStats) GetNodeLeaderLoadFactor() (*NodeTopicStats, float64) {
	perCpuStat := self.GetPerCPUStats()
	leftConsumed := int64(0)
	for _, t := range perCpuStat.ChannelDepthData {
		leftConsumed += t
	}
	leftConsumed += int64(len(perCpuStat.ChannelDepthData))
	if leftConsumed > HIGHEST_LEFT_CONSUME_MB_SIZE {
		leftConsumed = HIGHEST_LEFT_CONSUME_MB_SIZE
	}
	totalLeaderDataSize := int64(0)
	for _, v := range perCpuStat.TopicLeaderDataSize {
		totalLeaderDataSize += v
	}
	totalLeaderDataSize += int64(len(perCpuStat.TopicLeaderDataSize))
	if totalLeaderDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalLeaderDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	avgWrite := self.GetNodeAvgWriteLevel()
	if avgWrite > HIGHEST_PUB_QPS_LEVEL {
		avgWrite = HIGHEST_PUB_QPS_LEVEL
	}

	return perCpuStat, avgWrite/HIGHEST_PUB_QPS_LEVEL*60.0 + float64(leftConsumed)/HIGHEST_LEFT_CONSUME_MB_SIZE*30.0 + float64(totalLeaderDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*10.0
}

func (self *NodeTopicStats) GetNodePeakLevelList() []int64 {
	levelList := make([]int64, 8)
	for _, dataList := range self.TopicHourlyPubDataList {
		peak := int64(10)
		for _, data := range dataList {
			if data > peak {
				peak = data
			}
		}
		index := int(convertQPSLevel(peak))
		if index > HIGHEST_PUB_QPS_LEVEL {
			index = HIGHEST_PUB_QPS_LEVEL
		}
		levelList[index]++
	}
	return levelList
}

func (self *NodeTopicStats) GetNodeAvgWriteLevel() float64 {
	level := int64(0)
	for _, dataList := range self.TopicHourlyPubDataList {
		sum := int64(10)
		for _, data := range dataList {
			sum += data
		}
		sum = sum / int64(len(dataList))
		level += sum
	}
	return convertQPSLevel(level)
}

func (self *NodeTopicStats) GetNodeAvgReadLevel() float64 {
	level := float64(0)
	for topicName, dataList := range self.TopicHourlyPubDataList {
		sum := int64(10)
		for _, data := range dataList {
			sum += data
		}
		sum = sum / int64(len(dataList))
		num, ok := self.ChannelNum[topicName]
		if ok {
			level += math.Log2(1.0+float64(num)) * float64(sum)
		}
	}
	return convertQPSLevel(int64(level))
}

func (self *NodeTopicStats) GetTopicAvgWriteLevel(topic TopicPartitionID) float64 {
	selectedTopic := topic.String()
	dataList, ok := self.TopicHourlyPubDataList[selectedTopic]
	if ok {
		sum := int64(10)
		for _, data := range dataList {
			sum += data
		}
		sum = sum / int64(len(dataList))
		return convertQPSLevel(sum)
	}
	return 1.0
}

func (self *NodeTopicStats) GetMostBusyAndIdleTopicWriteLevel(leaderOnly bool) (string, string, float64, float64) {
	busy := int64(0)
	busyTopic := ""
	idle := int64(math.MaxInt32)
	idleTopic := ""
	for topicName, dataList := range self.TopicHourlyPubDataList {
		if leaderOnly {
			d, ok := self.TopicLeaderDataSize[topicName]
			if !ok || d <= 0 {
				continue
			}
		}
		sum := int64(0)
		for _, d := range dataList {
			sum += d
		}
		sum = sum / int64(len(dataList))
		if sum > busy {
			busy = sum
			busyTopic = topicName
		}
		if sum < idle {
			idle = sum
			idleTopic = topicName
		}
	}
	return idleTopic, busyTopic, convertQPSLevel(idle), convertQPSLevel(busy)
}

func (self *NodeTopicStats) GetTopicAvgReadLevel(topic TopicPartitionID) float64 {
	level := self.GetTopicAvgWriteLevel(topic)
	selectedTopic := topic.String()
	num, ok := self.ChannelNum[selectedTopic]
	if ok {
		return math.Log2(1.0+float64(num)) * level
	}
	return 0.0
}

func (self *NodeTopicStats) GetTopicPeakLevel(topic TopicPartitionID) float64 {
	selectedTopic := topic.String()
	dataList, ok := self.TopicHourlyPubDataList[selectedTopic]
	if ok {
		peak := int64(10)
		for _, data := range dataList {
			if data > peak {
				peak = data
			}
		}
		return convertQPSLevel(peak)
	}
	return 1.0
}

func (self *NodeTopicStats) GetPerCPUStats() *NodeTopicStats {
	consumed := make(map[string]int64)
	for tname, t := range self.ChannelDepthData {
		consumed[tname] = t / int64(self.NodeCPUs/4+1)
	}
	leaderSize := make(map[string]int64)
	for tname, v := range self.TopicLeaderDataSize {
		leaderSize[tname] = v / int64(self.NodeCPUs/4+1)
	}
	totalSize := make(map[string]int64)
	for tname, v := range self.TopicTotalDataSize {
		totalSize[tname] = v / int64(self.NodeCPUs/4+1)
	}
	return &NodeTopicStats{
		NodeID:                 self.NodeID,
		ChannelDepthData:       consumed,
		TopicLeaderDataSize:    leaderSize,
		TopicTotalDataSize:     totalSize,
		NodeCPUs:               1,
		TopicHourlyPubDataList: self.TopicHourlyPubDataList,
		ChannelNum:             self.ChannelNum,
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
	_, left := self.GetNodeLoadFactor()
	_, right := other.GetNodeLoadFactor()
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

type DataPlacement struct {
	balanceInterval [2]int
	lookupCoord     *NsqLookupCoordinator
}

func NewDataPlacement(coord *NsqLookupCoordinator) *DataPlacement {
	return &DataPlacement{
		lookupCoord:     coord,
		balanceInterval: [2]int{2, 4},
	}
}

func (self *DataPlacement) SetBalanceInterval(start int, end int) {
	if start == end && start == 0 {
		return
	}
	self.balanceInterval[0] = start
	self.balanceInterval[1] = end
}

func (self *DataPlacement) DoBalance(monitorChan chan struct{}) {
	//check period for the data balance.
	ticker := time.NewTicker(time.Minute * 10)
	defer func() {
		ticker.Stop()
		coordLog.Infof("balance check exit.")
	}()
	for {
		select {
		case <-monitorChan:
			return
		case <-ticker.C:
			// only balance at given interval
			if time.Now().Hour() > self.balanceInterval[1] || time.Now().Hour() < self.balanceInterval[0] {
				continue
			}
			if !self.lookupCoord.IsMineLeader() {
				coordLog.Infof("not leader while checking balance")
				continue
			}
			if !self.lookupCoord.IsClusterStable() {
				coordLog.Infof("no balance since cluster is not stable while checking balance")
				continue
			}
			avgLeaderLoad := 0.0
			minLeaderLoad := float64(math.MaxInt32)
			maxLeaderLoad := 0.0
			avgNodeLoad := 0.0
			minNodeLoad := float64(math.MaxInt32)
			maxNodeLoad := 0.0
			// if max load is 4 times more than avg load, we need move some
			// leader from max to min load node one by one.
			// if min load is 4 times less than avg load, we can move some
			// leader to this min load node.
			coordLog.Infof("begin checking balance of topic data...")
			currentNodes := self.lookupCoord.getCurrentNodes()
			validNum := 0
			topicStatsMinMax := make([]*NodeTopicStats, 2)
			for nodeID, nodeInfo := range currentNodes {
				topicStat, err := self.lookupCoord.getNsqdTopicStat(nodeInfo)
				if err != nil {
					coordLog.Infof("failed to get node topic status while checking balance: %v", nodeID)
					continue
				}
				leaderLF, nodeLF := topicStat.GetNodeLoadFactor()
				coordLog.Infof("nsqd node %v load factor is : (%v, %v)", nodeID, leaderLF, nodeLF)
				if leaderLF < minLeaderLoad {
					topicStatsMinMax[0] = topicStat
					minLeaderLoad = leaderLF
				}
				minNodeLoad = math.Min(nodeLF, minNodeLoad)
				if leaderLF > maxLeaderLoad {
					topicStatsMinMax[1] = topicStat
					maxLeaderLoad = leaderLF
				}
				maxNodeLoad = math.Max(nodeLF, maxNodeLoad)

				avgLeaderLoad += leaderLF
				avgNodeLoad += nodeLF
				validNum++
			}
			if validNum == 0 || topicStatsMinMax[0] == nil || topicStatsMinMax[1] == nil {
				continue
			}
			avgLeaderLoad = avgLeaderLoad / float64(validNum)
			avgNodeLoad = avgNodeLoad / float64(validNum)
			coordLog.Infof("min/avg/max leader load %v, %v, %v", minLeaderLoad, avgLeaderLoad, maxLeaderLoad)
			coordLog.Infof("min/avg/max node load %v, %v, %v", minNodeLoad, avgNodeLoad, maxNodeLoad)
			if len(topicStatsMinMax[1].TopicHourlyPubDataList) <= 2 {
				coordLog.Infof("the topic number is so less on both nodes, no need balance: %v", topicStatsMinMax)
				continue
			}

			topicList, err := self.lookupCoord.leadership.ScanTopics()
			if err != nil {
				coordLog.Infof("scan topics error: %v", err)
				continue
			}
			if len(topicList) <= len(currentNodes)*2 {
				coordLog.Infof("the topics less than nodes, no need balance: %v ", len(topicList))
				continue
			}
			if minLeaderLoad*4 < avgLeaderLoad {
				// move some topic from the most busy node to the most idle node
				self.balanceTopicLeaderBetweenNodes(minLeaderLoad, maxLeaderLoad, topicStatsMinMax)
			} else if avgLeaderLoad < 5 && maxLeaderLoad < 10 {
				// all nodes in the cluster are under low load, no need balance
				continue
			} else if avgLeaderLoad*2 < maxLeaderLoad {
				self.balanceTopicLeaderBetweenNodes(minLeaderLoad, maxLeaderLoad, topicStatsMinMax)
			} else if avgLeaderLoad >= 20 {
				if (minLeaderLoad*2 < avgLeaderLoad) || (maxLeaderLoad-avgLeaderLoad > 10) {
					self.balanceTopicLeaderBetweenNodes(minLeaderLoad, maxLeaderLoad, topicStatsMinMax)
				}
			}
		}
	}
}

func (self *DataPlacement) balanceTopicLeaderBetweenNodes(minLF float64, maxLF float64, statsMinMax []*NodeTopicStats) {
	idleTopic, busyTopic, _, busyLevel := statsMinMax[1].GetMostBusyAndIdleTopicWriteLevel(true)
	if busyTopic == "" && idleTopic == "" {
		coordLog.Infof("no idle or busy topic found")
		return
	}

	// avoid move the too busy topic to reduce the impaction of the online service.
	// if the busiest topic is not so busy, we try move this topic to avoid move too much idle topics
	if busyTopic != "" && busyLevel < 13 && (busyLevel < maxLF-minLF) {
		topicName, partitionID, err := splitTopicPartitionID(busyTopic)
		if err != nil {
			coordLog.Warningf("split topic name and partition failed: %v", err)
			return
		}
		self.lookupCoord.handleMoveTopicLeader(topicName, partitionID, statsMinMax[1].NodeID)
	} else if idleTopic != "" {
		topicName, partitionID, err := splitTopicPartitionID(idleTopic)
		if err != nil {
			coordLog.Warningf("split topic name and partition failed: %v", err)
			return
		}
		self.lookupCoord.handleMoveTopicLeader(topicName, partitionID, statsMinMax[1].NodeID)
	}
}

func (self *DataPlacement) getExcludeNodesForTopic(topicInfo *TopicPartitionMetaInfo) map[string]struct{} {
	excludeNodes := make(map[string]struct{})
	excludeNodes[topicInfo.Leader] = struct{}{}
	for _, v := range topicInfo.ISR {
		excludeNodes[v] = struct{}{}
	}
	for _, v := range topicInfo.CatchupList {
		excludeNodes[v] = struct{}{}
	}
	// exclude other partition node with the same topic
	meta, err := self.lookupCoord.leadership.GetTopicMetaInfo(topicInfo.Name)
	if err != nil {
		coordLog.Infof("failed get the meta info: %v", err)
		return excludeNodes
	}
	num := meta.PartitionNum
	for i := 0; i < num; i++ {
		topicPartInfo, err := self.lookupCoord.leadership.GetTopicInfo(topicInfo.Name, i)
		if err != nil {
			continue
		}
		excludeNodes[topicPartInfo.Leader] = struct{}{}
		for _, v := range topicPartInfo.ISR {
			excludeNodes[v] = struct{}{}
		}
		for _, v := range topicPartInfo.CatchupList {
			excludeNodes[v] = struct{}{}
		}
	}
	return excludeNodes
}

func (self *DataPlacement) allocNodeForTopic(topicInfo *TopicPartitionMetaInfo, currentNodes map[string]NsqdNodeInfo) (*NsqdNodeInfo, *CoordErr) {
	// collect the nsqd data, check if any node has the topic data already.
	var chosenNode NsqdNodeInfo
	var chosenStat *NodeTopicStats

	excludeNodes := self.getExcludeNodesForTopic(topicInfo)

	for nodeID, nodeInfo := range currentNodes {
		if _, ok := excludeNodes[nodeID]; ok {
			continue
		}
		topicStat, err := self.lookupCoord.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("failed to get topic status for this node: %v", nodeInfo)
			continue
		}
		if chosenNode.ID == "" {
			chosenNode = nodeInfo
			chosenStat = topicStat
			continue
		}
		if topicStat.SlaveLessLoader(chosenStat) {
			chosenNode = nodeInfo
			chosenStat = topicStat
		}
	}
	if chosenNode.ID == "" {
		coordLog.Infof("no more available node for topic: %v, excluding nodes: %v, all nodes: %v", topicInfo.GetTopicDesp(), excludeNodes, currentNodes)
		return nil, ErrNodeUnavailable
	}
	coordLog.Infof("node %v is alloc for topic: %v", chosenNode, topicInfo.GetTopicDesp())
	return &chosenNode, nil
}

// init leader node and isr list for the empty topic
func (self *DataPlacement) allocTopicLeaderAndISR(currentNodes map[string]NsqdNodeInfo,
	replica int, partitionNum int, existPart map[int]*TopicPartitionMetaInfo) ([]string, [][]string, *CoordErr) {
	if len(currentNodes) < replica || len(currentNodes) < partitionNum {
		coordLog.Infof("nodes %v is less than replica %v or partition %v", len(currentNodes), replica, partitionNum)
		return nil, nil, ErrNodeUnavailable
	}
	if len(currentNodes) < replica*partitionNum {
		coordLog.Infof("nodes is less than replica*partition")
		return nil, nil, ErrNodeUnavailable
	}
	coordLog.Infof("alloc current nodes: %v", len(currentNodes))

	existLeaders := make(map[string]struct{})
	existSlaves := make(map[string]struct{})
	for _, topicInfo := range existPart {
		for _, n := range topicInfo.ISR {
			if n == topicInfo.Leader {
				existLeaders[n] = struct{}{}
			} else {
				existSlaves[n] = struct{}{}
			}
		}
	}
	nodeTopicStats := make([]NodeTopicStats, 0, len(currentNodes))
	for _, nodeInfo := range currentNodes {
		stats, err := self.lookupCoord.getNsqdTopicStat(nodeInfo)
		if err != nil {
			coordLog.Infof("got topic status for node %v failed: %v", nodeInfo.GetID(), err)
			continue
		}
		nodeTopicStats = append(nodeTopicStats, *stats)
	}
	if len(nodeTopicStats) < partitionNum*replica {
		return nil, nil, ErrNodeUnavailable
	}
	leaderSort := func(l, r *NodeTopicStats) bool {
		return l.LeaderLessLoader(r)
	}
	By(leaderSort).Sort(nodeTopicStats)
	leaders := make([]string, partitionNum)
	p := 0
	currentSelect := 0
	coordLog.Infof("alloc current exist status: %v, \n %v", existLeaders, existSlaves)
	for p < partitionNum {
		if elem, ok := existPart[p]; ok {
			leaders[p] = elem.Leader
		} else {
			for {
				if currentSelect >= len(nodeTopicStats) {
					coordLog.Infof("not enough nodes for leaders")
					return nil, nil, ErrNodeUnavailable
				}
				nodeInfo := nodeTopicStats[currentSelect]
				currentSelect++
				if _, ok := existLeaders[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist other leader(different partition) node: %v", nodeInfo)
					continue
				}
				// TODO: should slave can be used for other leader?
				if _, ok := existSlaves[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist other slave (different partition) node: %v", nodeInfo)
					continue
				}
				leaders[p] = nodeInfo.NodeID
				existLeaders[nodeInfo.NodeID] = struct{}{}
				break
			}
		}
		p++
	}
	p = 0
	currentSelect = 0
	slaveSort := func(l, r *NodeTopicStats) bool {
		return l.SlaveLessLoader(r)
	}
	By(slaveSort).Sort(nodeTopicStats)

	isrlist := make([][]string, partitionNum)
	for p < partitionNum {
		isr := make([]string, 0, replica)
		isr = append(isr, leaders[p])
		if len(isr) >= replica {
			// already done with replica
		} else if elem, ok := existPart[p]; ok {
			isr = elem.ISR
		} else {
			for {
				if currentSelect >= len(nodeTopicStats) {
					coordLog.Infof("not enough nodes for slaves")
					return nil, nil, ErrNodeUnavailable
				}
				nodeInfo := nodeTopicStats[currentSelect]
				currentSelect++
				if nodeInfo.NodeID == leaders[p] {
					coordLog.Infof("ignore for leader node: %v", nodeInfo)
					continue
				}
				if _, ok := existSlaves[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist slave node: %v", nodeInfo)
					continue
				}
				// TODO: should slave can be used for other leader?
				if _, ok := existLeaders[nodeInfo.NodeID]; ok {
					coordLog.Infof("ignore for exist other leader(different partition) node: %v", nodeInfo)
					continue
				}
				existSlaves[nodeInfo.NodeID] = struct{}{}
				isr = append(isr, nodeInfo.NodeID)
				if len(isr) >= replica {
					break
				}
			}
		}
		isrlist[p] = isr
		p++
	}
	coordLog.Infof("topic selected leader: %v, topic selected isr : %v", leaders, isrlist)
	return leaders, isrlist, nil
}
