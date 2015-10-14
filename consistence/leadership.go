package consistence

import (
	"strconv"
)

type NsqdNodeInfo struct {
	ID      string
	NodeIp  string
	TcpPort string
	RpcPort string
}

func (self *NsqdNodeInfo) GetID() string {
	return self.ID
}

type NsqLookupdNodeInfo struct {
	ID       string
	NodeIp   string
	HttpPort string
	RpcPort  string
	Epoch    int
}

func (self *NsqLookupdNodeInfo) GetID() string {
	return self.ID
}

// topicLeaderEpoch increase while leader changed.
// topicEpoch increase while leader or isr changed. (seems not really needed?)
// lookupEpoch increase while lookup leader changed.

type TopicPartionMetaInfo struct {
	Name        string
	Partition   int
	Channels    []string
	Leader      string
	ISR         []string
	CatchupList []string
	Epoch       int
	Replica     int
}

func (self *TopicPartionMetaInfo) GetTopicDesp() string {
	return self.Name + "-" + strconv.Itoa(self.Partition)
}

type TopicLeaderSession struct {
	LeaderNode  *NsqdNodeInfo
	Session     string
	LeaderEpoch int
}

func (self *TopicLeaderSession) IsSame(other *TopicLeaderSession) bool {
	if self.LeaderEpoch != other.LeaderEpoch {
		return false
	}
	if self.Session != "" && other.Session != "" && self.Session != other.Session {
		return false
	}
	if self.LeaderNode.GetID() != other.LeaderNode.GetID() {
		return false
	}
	return true
}

type ConsistentStore interface {
	WriteKey(key, value string) error
	ReadKey(key string) (string, error)
	ListKey(key string) ([]string, error)
}

// We need check leader lock session before do any modify to etcd.
type NSQLookupdLeadership interface {
	InitClusterID(id string)
	Register(value NsqLookupdNodeInfo) error
	Unregister() error
	Stop()
	AcquireAndWatchLeader(leader chan NsqLookupdNodeInfo, stop chan struct{})
	CheckIfLeader(session string) bool
	UpdateLookupEpoch(key string, oldGen int) (int, error)
	WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{})
	ScanTopics() ([]TopicPartionMetaInfo, error)
	GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error)
	CreateTopicPartition(topic string, partition int, replica int) error
	CreateTopic(topic string, partitionNum int, replica int) error
	IsExistTopic(topic string) (bool, error)
	IsExistTopicPartition(topic string, partitionNum int) (bool, error)
	GetTopicPartitionNum(topic string) (int, error)
	DeleteTopic(topic string, partition int) error
	// update leader, isr, epoch
	// Note: update should do check-and-set to avoid unexpected override.
	UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo, oldGen int) error
	UpdateTopicCatchupList(topic string, partition int, catchupList []string, oldGen int) error
	CreateChannel(topic string, partition int, channel string) error
	DeleteChannel(topic string, partition int, channel string) error
	GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error)
	WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{}) error
}

type NSQDLeadership interface {
	InitClusterID(id string)
	Register(nodeData NsqdNodeInfo) error
	Unregister(nodeData NsqdNodeInfo) error
	AcquireTopicLeader(topic string, partition int, nodeData NsqdNodeInfo) error
	ReleaseTopicLeader(topic string, partition int) error
	WatchLookupdLeader(key string, leader chan *NsqLookupdNodeInfo, stop chan struct{}) error
	GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error)
}

type FakeNsqlookupLeadership struct {
	fakeTopicsData map[string]map[int]*TopicSummaryData
	fakeNsqdNodes  map[string]NsqdNodeInfo
	fakeEpoch      int
}

func (self *FakeNsqlookupLeadership) InitClusterID(id string) {
}

func (self *FakeNsqlookupLeadership) Register(value NsqLookupdNodeInfo) error {
	return nil
}

func (self *FakeNsqlookupLeadership) Unregister() error {
	return nil
}

func (self *FakeNsqlookupLeadership) Stop() {
}

func (self *FakeNsqlookupLeadership) AcquireAndWatchLeader(leader chan NsqLookupdNodeInfo, stopChan chan struct{}) {
}

func (self *FakeNsqlookupLeadership) CheckIfLeader(session string) bool {
	return true
}

func (self *FakeNsqlookupLeadership) UpdateLookupEpoch(key string, oldGen int) (int, error) {
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

func (self *FakeNsqlookupLeadership) ScanTopics() ([]TopicPartionMetaInfo, error) {
	alltopics := make([]TopicPartionMetaInfo, 0)
	for _, v := range self.fakeTopicsData {
		for _, topicInfo := range v {
			alltopics = append(alltopics, topicInfo.topicInfo)
		}
	}
	return alltopics, nil
}

func (self *FakeNsqlookupLeadership) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
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
func (self *FakeNsqlookupLeadership) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo, oldGen int) error {
	self.fakeTopicsData[topic][partition].topicInfo = *topicInfo
	return nil
}

func (self *FakeNsqlookupLeadership) UpdateTopicCatchupList(topic string, partition int, catchupList []string, oldGen int) error {
	self.fakeTopicsData[topic][partition].topicInfo.CatchupList = catchupList
	return nil
}

func (self *FakeNsqlookupLeadership) CreateChannel(topic string, partition int, channel string) error {
	return nil
}

func (self *FakeNsqlookupLeadership) DeleteChannel(topic string, partition int, channel string) error {
	return nil
}

func (self *FakeNsqlookupLeadership) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	return nil, nil
}

func (self *FakeNsqlookupLeadership) WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{}) error {
	return nil
}
