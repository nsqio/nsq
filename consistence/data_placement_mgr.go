package consistence

import (
	"sort"
)

// new topic can have an advice load factor to give the suggestion about the
// future load. While the topic data is small the load compute is not precise,
// so we use the advised load to determine the actual topic load.
// use 1~10 to advise from lowest to highest. The 1 or 10 should be used with much careful.

// TODO: support topic move from one to another by manual.

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

// TODO: handle recent avg load in 24hr.
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
