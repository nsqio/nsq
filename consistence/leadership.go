package consistence

import (
	"strconv"
)

type NsqdNodeInfo struct {
	ID      string
	NodeIp  string
	TcpPort int
	RpcPort string
}

func (self *NsqdNodeInfo) GetId() string {
	return self.ID
}

type NsqLookupdNodeInfo struct {
	ID       string
	NodeIp   string
	HttpPort string
	Epoch    int
}

func (self *NsqLookupdNodeInfo) GetId() string {
	return self.ID + self.NodeIp + self.HttpPort
}

type TopicLeadershipInfo struct {
	Name        string
	Partition   int
	Channels    []string
	Leader      string
	ISR         []string
	CatchupList []string
	Epoch       int
	Replica     int
}

func (self *TopicLeadershipInfo) GetTopicDesp() string {
	return self.Name + "-" + strconv.Itoa(self.Partition)
}

func (self *TopicLeadershipInfo) GetLeaderElectionKey() string {
	return self.Name + "-" + strconv.Itoa(self.Partition) + "-election"
}

type TopicLeaderSession struct {
	leaderNode *NsqdNodeInfo
	session    string
	topicEpoch int
}

func (self *TopicLeaderSession) IsSame(other *TopicLeaderSession) bool {
	if self.topicEpoch != other.topicEpoch {
		return false
	}
	if self.session != "" && other.session != "" && self.session != other.session {
		return false
	}
	if self.leaderNode.GetId() != other.leaderNode.GetId() {
		return false
	}
	return true
}

type ConsistentStore interface {
	WriteKey(key, value string) error
	ReadKey(key string) (string, error)
	ListKey(key string) ([]string, error)
}

type NSQLookupdLeadership interface {
	Register(key string, value NsqLookupdNodeInfo) error
	Unregister(key string) error
	Stop()
	AcquireAndWatchLeader(key string, leader chan NsqLookupdNodeInfo)
	UpdateLookupEpoch(key string) (int, error)
	WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{})
	ScanTopics() ([]TopicLeadershipInfo, error)
	GetTopicInfo(topic string, partition int) (*TopicLeadershipInfo, error)
	CreateTopicPartition(topic string, partition int, replica int) error
	CreateTopic(topic string, partitionNum int, replica int) error
	IsExistTopic(topic string) (bool, error)
	IsExistTopicPartition(topic string, partitionNum int) (bool, error)
	GetTopicPartitionNum(topic string) (int, error)
	DeleteTopic(topic string, partition int) error
	// update leader, isr, epoch
	UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicLeadershipInfo) error
	UpdateTopicCatchupList(topic string, partition int, catchupList []string) error
	CreateChannel(topic string, partition int, channel string) error
	DeleteChannel(topic string, partition int, channel string) error
	GetTopicLeaderSession(topic string, partition int) (string, error)
	WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{}) error
}

type NSQDLeadership interface {
	Register(nodeData NsqdNodeInfo) error
	Unregister(nodeData NsqdNodeInfo) error
	UpdateTopicEpoch(topic string, partition int) (int, error)
	AcquireTopicLeader(topic string, partition int, nodeData NsqdNodeInfo) error
	ReleaseTopicLeader(topic string, partition int) error
	WatchLookupdLeader(key string, leader chan NsqLookupdNodeInfo) error
	GetTopicInfo(topic string, partition int) (*TopicLeadershipInfo, error)
}

type FakeNsqlookupLeadership struct {
	fakeTopicsData map[string]map[int]*TopicSummaryData
	fakeNsqdNodes  map[string]NsqdNodeInfo
	fakeEpoch      int
}

func (self *FakeNsqlookupLeadership) Register(key string, value NsqLookupdNodeInfo) error {
	return nil
}

func (self *FakeNsqlookupLeadership) Unregister(key string) error {
	return nil
}

func (self *FakeNsqlookupLeadership) Stop() {
}

func (self *FakeNsqlookupLeadership) AcquireAndWatchLeader(key string, leader chan NsqLookupdNodeInfo) {
}

func (self *FakeNsqlookupLeadership) UpdateLookupEpoch(key string) (int, error) {
	self.fakeEpoch++
	return self.fakeEpoch, nil
}

func (self *FakeNsqlookupLeadership) WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{}) {
	nodes := make([]NsqdNodeInfo, 0)
	for _, v := range self.fakeNsqdNodes {
		nodes = append(nodes, v)
	}
	nsqds <- nodes
}

func (self *FakeNsqlookupLeadership) ScanTopics() ([]TopicLeadershipInfo, error) {
	alltopics := make([]TopicLeadershipInfo, 0)
	for _, v := range self.fakeTopicsData {
		for _, topicInfo := range v {
			alltopics = append(alltopics, topicInfo.topicInfo)
		}
	}
	return alltopics, nil
}

func (self *FakeNsqlookupLeadership) GetTopicInfo(topic string, partition int) (*TopicLeadershipInfo, error) {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		return nil, ErrTopicNotCreated
	}
	tp, ok := t[partition]
	if !ok {
		return nil, ErrTopicNotCreated
	}
	return &tp.topicInfo, nil
}

func (self *FakeNsqlookupLeadership) CreateTopicPartition(topic string, partition int, replica int) error {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		t = make(map[int]*TopicSummaryData)
		self.fakeTopicsData[topic] = t
	}
	tp, ok := t[partition]
	if ok {
		return ErrAlreadyExist
	}
	var newtp TopicSummaryData
	newtp.topicInfo.Name = topic
	newtp.topicInfo.Partition = partition
	newtp.topicInfo.Replica = replica
	t[partition] = &newtp
	return nil
}

func (self *FakeNsqlookupLeadership) CreateTopic(topic string, partitionNum int, replica int) error {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		t = make(map[int]*TopicSummaryData)
		self.fakeTopicsData[topic] = t
	}
	return nil
}

func (self *FakeNsqlookupLeadership) IsExistTopic(topic string) (bool, error) {
	_, ok := self.fakeTopicsData[topic]
	return ok, nil
}

func (self *FakeNsqlookupLeadership) IsExistTopicPartition(topic string, partitionNum int) (bool, error) {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		return false, nil
	}
	_, ok = t[partitionNum]
	return ok, nil
}

func (self *FakeNsqlookupLeadership) GetTopicPartitionNum(topic string) (int, error) {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		return 0, nil
	}
	return len(t), nil
}

func (self *FakeNsqlookupLeadership) DeleteTopic(topic string, partition int) error {
	delete(self.fakeTopicsData[topic], partition)
	return nil
}

// update leader, isr, epoch
func (self *FakeNsqlookupLeadership) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicLeadershipInfo) error {
	self.fakeTopicsData[topic][partition].topicInfo = *topicInfo
	return nil
}

func (self *FakeNsqlookupLeadership) UpdateTopicCatchupList(topic string, partition int, catchupList []string) error {
	self.fakeTopicsData[topic][partition].topicInfo.CatchupList = catchupList
	return nil
}

func (self *FakeNsqlookupLeadership) CreateChannel(topic string, partition int, channel string) error {
	return nil
}

func (self *FakeNsqlookupLeadership) DeleteChannel(topic string, partition int, channel string) error {
	return nil
}

func (self *FakeNsqlookupLeadership) GetTopicLeaderSession(topic string, partition int) (string, error) {
	return "", nil
}

func (self *FakeNsqlookupLeadership) WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{}) error {
	return nil
}
