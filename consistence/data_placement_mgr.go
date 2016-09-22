package consistence

import (
	"container/heap"
	"errors"
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
// TODO: separate the high peak topics to avoid too much high peak on node

// left consume data size level: 0~1GB low, < 5GB medium, < 50GB high , >=50GB very high
// left data size level: <5GB low, <50GB medium , <200GB high, >=200GB very high

var (
	ErrBalanceNodeUnavailable = errors.New("can not find a node to be balanced")
)

const (
	defaultTopicLoadFactor       = 3
	HIGHEST_PUB_QPS_LEVEL        = 100
	HIGHEST_LEFT_CONSUME_MB_SIZE = 50 * 1024
	HIGHEST_LEFT_DATA_MB_SIZE    = 200 * 1024
)

// pub qps level : 1~13 low (< 30 KB/sec), 13 ~ 31 medium (<1000 KB/sec), 31 ~ 74 high (<8 MB/sec), >74 very high (>8 MB/sec)
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
		return "", 0, fmt.Errorf("invalid topic full name: %v", topicFullName)
	}
	topicName := topicFullName[:partIndex]
	partitionID, err := strconv.Atoi(topicFullName[partIndex+1:])
	return topicName, partitionID, err
}

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
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
	leaderLF := self.GetNodeLeaderLoadFactor()
	totalDataSize := int64(0)
	for _, v := range self.TopicTotalDataSize {
		totalDataSize += v
	}
	totalDataSize += int64(len(self.TopicTotalDataSize))
	if totalDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	return leaderLF, leaderLF + float64(totalDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*5
}

func (self *NodeTopicStats) GetNodeLeaderLoadFactor() float64 {
	leftConsumed := int64(0)
	for _, t := range self.ChannelDepthData {
		leftConsumed += t
	}
	leftConsumed += int64(len(self.ChannelDepthData))
	if leftConsumed > HIGHEST_LEFT_CONSUME_MB_SIZE {
		leftConsumed = HIGHEST_LEFT_CONSUME_MB_SIZE
	}
	totalLeaderDataSize := int64(0)
	for _, v := range self.TopicLeaderDataSize {
		totalLeaderDataSize += v
	}
	totalLeaderDataSize += int64(len(self.TopicLeaderDataSize))
	if totalLeaderDataSize > HIGHEST_LEFT_DATA_MB_SIZE {
		totalLeaderDataSize = HIGHEST_LEFT_DATA_MB_SIZE
	}
	avgWrite := self.GetNodeAvgWriteLevel()
	if avgWrite > HIGHEST_PUB_QPS_LEVEL {
		avgWrite = HIGHEST_PUB_QPS_LEVEL
	}

	return avgWrite/HIGHEST_PUB_QPS_LEVEL*60.0 + float64(leftConsumed)/HIGHEST_LEFT_CONSUME_MB_SIZE*30.0 + float64(totalLeaderDataSize)/HIGHEST_LEFT_DATA_MB_SIZE*10.0
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
		cnt := 0
		tmp := make(IntHeap, 0, len(dataList))
		heap.Init(&tmp)
		for _, data := range dataList {
			sum += data
			cnt++
			heap.Push(&tmp, int(data))
		}
		// remove the lowest 1/4 (at midnight all topics are low)
		for i := 0; i < len(dataList)/4; i++ {
			v := heap.Pop(&tmp)
			sum -= int64(v.(int))
			cnt--
		}
		sum = sum / int64(cnt)
		level += sum
	}
	return convertQPSLevel(level)
}

func (self *NodeTopicStats) GetNodeAvgReadLevel() float64 {
	level := float64(0)
	for topicName, dataList := range self.TopicHourlyPubDataList {
		sum := int64(10)
		cnt := 0
		tmp := make(IntHeap, 0, len(dataList))
		heap.Init(&tmp)
		for _, data := range dataList {
			sum += data
			cnt++
			heap.Push(&tmp, int(data))
		}
		for i := 0; i < len(dataList)/4; i++ {
			v := heap.Pop(&tmp)
			sum -= int64(v.(int))
			cnt--
		}

		sum = sum / int64(cnt)
		num, ok := self.ChannelNum[topicName]
		if ok {
			level += math.Log2(1.0+float64(num)) * float64(sum)
		}
	}
	return convertQPSLevel(int64(level))
}

func (self *NodeTopicStats) GetTopicLoadFactor(topicFullName string) float64 {
	data := self.TopicTotalDataSize[topicFullName]
	return self.GetTopicLeaderLoadFactor(topicFullName) + float64(data)/HIGHEST_LEFT_DATA_MB_SIZE*5.0
}

func (self *NodeTopicStats) GetTopicLeaderLoadFactor(topicFullName string) float64 {
	writeLevel := self.GetTopicAvgWriteLevel(topicFullName)
	depth := self.ChannelDepthData[topicFullName]
	data := self.TopicLeaderDataSize[topicFullName]
	return writeLevel/HIGHEST_PUB_QPS_LEVEL*60.0 + float64(depth)/HIGHEST_LEFT_CONSUME_MB_SIZE*30.0 + float64(data)/HIGHEST_LEFT_DATA_MB_SIZE*10.0
}

func (self *NodeTopicStats) GetTopicAvgWriteLevel(topicFullName string) float64 {
	dataList, ok := self.TopicHourlyPubDataList[topicFullName]
	if ok {
		sum := int64(10)
		cnt := 0
		tmp := make(IntHeap, 0, len(dataList))
		heap.Init(&tmp)
		for _, data := range dataList {
			sum += data
			cnt++
			heap.Push(&tmp, int(data))
		}
		for i := 0; i < len(dataList)/4; i++ {
			v := heap.Pop(&tmp)
			sum -= int64(v.(int))
			cnt--
		}

		sum = sum / int64(cnt)
		return convertQPSLevel(sum)
	}
	return 1.0
}

func (self *NodeTopicStats) GetMostBusyAndIdleTopicWriteLevel(leaderOnly bool) (string, string, float64, float64) {
	busy := float64(0.0)
	busyTopic := ""
	idle := float64(math.MaxInt32)
	idleTopic := ""
	for topicName, _ := range self.TopicHourlyPubDataList {
		d, ok := self.TopicLeaderDataSize[topicName]
		if leaderOnly {
			if !ok || d <= 0 {
				continue
			}
		} else {
			if ok && d > 0 {
				continue
			}
		}

		sum := 0.0
		if leaderOnly {
			sum = self.GetTopicLeaderLoadFactor(topicName)
		} else {
			sum = self.GetTopicLoadFactor(topicName)
		}

		if sum > busy {
			busy = sum
			busyTopic = topicName
		}
		if sum < idle {
			idle = sum
			idleTopic = topicName
		}
	}
	return idleTopic, busyTopic, idle, busy
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

func (self *NodeTopicStats) LeaderLessLoader(other *NodeTopicStats) bool {
	left := self.GetNodeLeaderLoadFactor()
	right := other.GetNodeLeaderLoadFactor()
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
			nodeTopicStats := make([]NodeTopicStats, 0, len(currentNodes))
			var mostLeaderStats *NodeTopicStats
			mostLeaderNum := 0
			mostReplicaNum := 0
			var mostReplicaStats *NodeTopicStats
			for nodeID, nodeInfo := range currentNodes {
				topicStat, err := self.lookupCoord.getNsqdTopicStat(nodeInfo)
				if err != nil {
					coordLog.Infof("failed to get node topic status while checking balance: %v", nodeID)
					continue
				}
				nodeTopicStats = append(nodeTopicStats, *topicStat)
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
				if len(topicStat.TopicLeaderDataSize) > mostLeaderNum {
					mostLeaderNum = len(topicStat.TopicLeaderDataSize)
					mostLeaderStats = topicStat
				}
				replicaNum := len(topicStat.TopicTotalDataSize) - len(topicStat.TopicLeaderDataSize)
				if replicaNum > mostReplicaNum {
					mostReplicaNum = replicaNum
					mostReplicaStats = topicStat
				}
			}
			if validNum == 0 || topicStatsMinMax[0] == nil || topicStatsMinMax[1] == nil {
				continue
			}

			leaderSort := func(l, r *NodeTopicStats) bool {
				return l.LeaderLessLoader(r)
			}
			By(leaderSort).Sort(nodeTopicStats)

			avgLeaderLoad = avgLeaderLoad / float64(validNum)
			avgNodeLoad = avgNodeLoad / float64(validNum)
			coordLog.Infof("min/avg/max leader load %v, %v, %v", minLeaderLoad, avgLeaderLoad, maxLeaderLoad)
			coordLog.Infof("min/avg/max node load %v, %v, %v", minNodeLoad, avgNodeLoad, maxNodeLoad)
			if len(topicStatsMinMax[1].TopicHourlyPubDataList) <= 2 {
				coordLog.Infof("the topic number is so less on both nodes, no need balance: %v", topicStatsMinMax[1])
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
			moveLeader := false
			avgTopicNum := len(topicList) / len(currentNodes)
			if len(topicStatsMinMax[1].TopicLeaderDataSize) > int(1.1*float64(avgTopicNum)) {
				// too many leader topics on this node, try move leader topic
				coordLog.Infof("move leader topic since too many on this node")
				moveLeader = true
			} else {
				// too many replica topics on this node, try move replica topic to others
				coordLog.Infof("move replica topic since too many on this node")
			}
			if minLeaderLoad*4 < avgLeaderLoad {
				// move some topic from the most busy node to the most idle node
				self.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, true, minLeaderLoad,
					maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
			} else if avgLeaderLoad < 5 && maxLeaderLoad < 10 {
				// all nodes in the cluster are under low load, no need balance
				continue
			} else if avgLeaderLoad*2 < maxLeaderLoad {
				self.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader, false, minLeaderLoad,
					maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
			} else if avgLeaderLoad >= 20 &&
				((minLeaderLoad*2 < maxLeaderLoad) || (maxLeaderLoad > avgLeaderLoad*1.5)) {
				self.balanceTopicLeaderBetweenNodes(monitorChan, moveLeader,
					minLeaderLoad*2 < maxLeaderLoad, minLeaderLoad, maxLeaderLoad,
					topicStatsMinMax, nodeTopicStats)
			} else {
				if mostLeaderStats != nil && avgTopicNum > 10 && mostLeaderNum > int(float64(avgTopicNum)*1.5) {
					needMove := checkMoveLotsOfTopics(true, mostLeaderNum, mostLeaderStats, avgTopicNum, topicStatsMinMax, nodeTopicStats)
					if needMove {
						topicStatsMinMax[1] = mostLeaderStats
						leaderLF, _ := mostLeaderStats.GetNodeLoadFactor()
						maxLeaderLoad = leaderLF
						self.balanceTopicLeaderBetweenNodes(monitorChan, true, true, minLeaderLoad,
							maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
						continue
					}
				}
				if mostReplicaStats != nil && avgTopicNum > 10 && mostReplicaNum > int(float64(avgTopicNum)*1.5) {
					needMove := checkMoveLotsOfTopics(false, mostReplicaNum, mostReplicaStats, avgTopicNum, topicStatsMinMax, nodeTopicStats)
					if needMove {
						topicStatsMinMax[1] = mostReplicaStats
						leaderLF, _ := mostReplicaStats.GetNodeLoadFactor()
						maxLeaderLoad = leaderLF
						self.balanceTopicLeaderBetweenNodes(monitorChan, false, true, minLeaderLoad,
							maxLeaderLoad, topicStatsMinMax, nodeTopicStats)
						continue
					}
				}
			}
		}
	}
}

func checkMoveLotsOfTopics(moveLeader bool, moveTopicNum int, moveNodeStats *NodeTopicStats, avgTopicNum int,
	topicStatsMinMax []*NodeTopicStats, nodeTopicStats []NodeTopicStats) bool {
	if moveNodeStats.NodeID == topicStatsMinMax[0].NodeID ||
		len(topicStatsMinMax[0].TopicLeaderDataSize) > avgTopicNum {
		return false
	}
	coordLog.Infof("too many topic on node: %v, num: %v", moveNodeStats.NodeID, moveTopicNum)
	//we should avoid move leader topic if the load is not so much
	busyIndex := 0
	for index, stat := range nodeTopicStats {
		if stat.NodeID == moveNodeStats.NodeID {
			busyIndex = index
			break
		}
	}
	if busyIndex == 0 {
		return false
	}
	if busyIndex < len(nodeTopicStats)/2 && moveTopicNum < avgTopicNum*2 {
		coordLog.Infof("although many topics , the load is not much: %v", busyIndex)
		return false
	}
	return true
}

func (self *DataPlacement) balanceTopicLeaderBetweenNodes(monitorChan chan struct{}, moveLeader bool, moveToMinLF bool,
	minLF float64, maxLF float64, statsMinMax []*NodeTopicStats, sortedNodeTopicStats []NodeTopicStats) {
	idleTopic, busyTopic, _, busyLevel := statsMinMax[1].GetMostBusyAndIdleTopicWriteLevel(moveLeader)
	if busyTopic == "" && idleTopic == "" {
		coordLog.Infof("no idle or busy topic found")
		return
	}

	coordLog.Infof("balance topic: %v, %v(%v) from node: %v ", idleTopic, busyTopic, busyLevel, statsMinMax[1].NodeID)
	checkMoveOK := false
	topicName := ""
	partitionID := 0
	var err error
	// avoid move the too busy topic to reduce the impaction of the online service.
	// if the busiest topic is not so busy, we try move this topic to avoid move too much idle topics
	if busyTopic != "" && busyLevel < 10 && (busyLevel*2 < maxLF-minLF) {
		topicName, partitionID, err = splitTopicPartitionID(busyTopic)
		if err != nil {
			coordLog.Warningf("split topic name and partition failed: %v", err)
			return
		}
		checkMoveOK = self.checkAndPrepareMove(monitorChan, topicName, partitionID, sortedNodeTopicStats, moveToMinLF, moveLeader)
	}
	if !checkMoveOK && idleTopic != "" {
		topicName, partitionID, err = splitTopicPartitionID(idleTopic)
		if err != nil {
			coordLog.Warningf("split topic name and partition failed: %v", err)
			return
		}
		checkMoveOK = self.checkAndPrepareMove(monitorChan, topicName, partitionID, sortedNodeTopicStats, moveToMinLF, moveLeader)
	}
	if checkMoveOK {
		self.lookupCoord.handleMoveTopic(moveLeader, topicName, partitionID, statsMinMax[1].NodeID)
	}
}

func (self *DataPlacement) checkAndPrepareMove(monitorChan chan struct{}, topicName string, partitionID int,
	sortedNodeTopicStats []NodeTopicStats, moveToMinLF bool, moveLeader bool) bool {
	topicInfo, err := self.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic %v info: %v", topicName, err)
		return false
	}
	checkMoveOK := false
	if moveToMinLF || (!moveLeader && len(topicInfo.ISR)-1 <= topicInfo.Replica/2) {
		if moveLeader {
			// check if any of current isr nodes is already idle for move
			for _, nid := range topicInfo.ISR {
				if checkMoveOK {
					break
				}
				for index, stat := range sortedNodeTopicStats {
					if len(sortedNodeTopicStats) <= 5 {
						if index > 0 {
							break
						}
					} else if index > 1 {
						break
					}
					if stat.NodeID == nid {
						checkMoveOK = true
						break
					}
				}
			}
		}
		if !checkMoveOK {
			// the isr not so idle , we try add a new idle node to the isr.
			// and make sure that node can accept the topic (no other leader/follower for this topic)
			err := self.addToCatchupAndWaitISRReady(monitorChan, topicName, partitionID, sortedNodeTopicStats)
			if err != nil {
				checkMoveOK = false
			} else {
				checkMoveOK = true
			}
		}
	} else {
		// we are allowed to move to any node and no need to add any node to isr
		checkMoveOK = true
	}

	return checkMoveOK
}

func (self *DataPlacement) addToCatchupAndWaitISRReady(monitorChan chan struct{}, topicName string, partitionID int, sortedNodeTopicStats []NodeTopicStats) error {
	if len(sortedNodeTopicStats) == 0 {
		return errors.New("no stats data")
	}
	retry := 0
	currentSelect := 0
	topicInfo, err := self.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
	if err != nil {
		coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
	}
	filteredNodes := make([]string, 0)
	for index, s := range sortedNodeTopicStats {
		if index >= len(sortedNodeTopicStats)-2 {
			// never move to the most two busy nodes
			break
		}
		if FindSlice(topicInfo.ISR, s.NodeID) != -1 {
			// filter
		} else {
			filteredNodes = append(filteredNodes, s.NodeID)
		}
	}
	for {
		// the most load node should not be chosen
		if currentSelect >= len(filteredNodes) {
			coordLog.Infof("currently no any node can be balanced for topic: %v", topicName)
			return ErrBalanceNodeUnavailable
		}
		nid := filteredNodes[currentSelect]
		topicInfo, err := self.lookupCoord.leadership.GetTopicInfo(topicName, partitionID)
		if err != nil {
			coordLog.Infof("failed to get topic info: %v-%v: %v", topicName, partitionID, err)
		} else {
			if FindSlice(topicInfo.ISR, nid) != -1 {
				break
			} else if FindSlice(topicInfo.CatchupList, nid) != -1 {
				// wait ready
				select {
				case <-monitorChan:
					return errors.New("quiting")
				case <-time.After(time.Second * 5):
				}
				continue
			} else {
				excludeNodes := self.getExcludeNodesForTopic(topicInfo)
				if _, ok := excludeNodes[nid]; ok {
					coordLog.Infof("current node: %v is excluded for topic: %v-%v", nid, topicName, partitionID)
					currentSelect++
					continue
				}
				coordLog.Infof("node: %v is added for topic: %v-%v catchup", nid, topicName, partitionID)
				self.lookupCoord.addCatchupNode(topicInfo, nid)
			}
		}
		select {
		case <-monitorChan:
			return errors.New("quiting")
		case <-time.After(time.Second * 5):
		}
		if retry > 5 {
			coordLog.Infof("add catchup and wait timeout : %v", topicName)
			return errors.New("wait timeout")
		}
		retry++
	}
	return nil
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
