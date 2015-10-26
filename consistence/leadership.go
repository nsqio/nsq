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
	AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{})
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
	WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{})
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
	nodeChanged    chan struct{}
	fakeEpoch      int
	fakeLeader     *NsqLookupdNodeInfo
}

func NewFakeNsqlookupLeadership() *FakeNsqlookupLeadership {
	return &FakeNsqlookupLeadership{
		fakeTopicsData: make(map[string]map[int]*TopicSummaryData),
		fakeNsqdNodes:  make(map[string]NsqdNodeInfo),
		nodeChanged:    make(chan struct{}, 1),
	}
}

func (self *FakeNsqlookupLeadership) InitClusterID(id string) {
}

func (self *FakeNsqlookupLeadership) Register(value NsqLookupdNodeInfo) error {
	self.fakeLeader = &value
	return nil
}

func (self *FakeNsqlookupLeadership) Unregister() error {
	self.fakeLeader = nil
	return nil
}

func (self *FakeNsqlookupLeadership) Stop() {
}

func (self *FakeNsqlookupLeadership) AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stopChan chan struct{}) {
	leader <- self.fakeLeader
	for {
		select {
		case <-stopChan:
			close(leader)
			return
		}
	}
}

func (self *FakeNsqlookupLeadership) CheckIfLeader(session string) bool {
	return true
}

func (self *FakeNsqlookupLeadership) UpdateLookupEpoch(key string, oldGen int) (int, error) {
	self.fakeEpoch++
	return self.fakeEpoch, nil
}

func (self *FakeNsqlookupLeadership) AddFakedNsqdNode(n NsqdNodeInfo) {
	self.fakeNsqdNodes[n.GetID()] = n
	self.nodeChanged <- struct{}{}
}

func (self *FakeNsqlookupLeadership) RemoveFakedNsqdNode(nid string) {
	delete(self.fakeNsqdNodes, nid)
}

func (self *FakeNsqlookupLeadership) WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{}) {
	nodes := make([]NsqdNodeInfo, 0)
	for _, v := range self.fakeNsqdNodes {
		nodes = append(nodes, v)
	}
	nsqds <- nodes
	for {
		select {
		case <-stop:
			close(nsqds)
			return
		case <-self.nodeChanged:
			nodes := make([]NsqdNodeInfo, 0)
			for _, v := range self.fakeNsqdNodes {
				nodes = append(nodes, v)
			}
			nsqds <- nodes
		}
	}
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
	_, ok = t[partition]
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

func (self *FakeNsqlookupLeadership) IsExistTopicPartition(topic string, partition int) (bool, error) {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		return false, nil
	}
	_, ok = t[partition]
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

func (self *FakeNsqlookupLeadership) UpdateTopicLeader(topic string, partition string, leader string) {
	var topicInfo TopicPartionMetaInfo
	topicInfo.Leader = leader
	topicInfo.ISR = []string{leader}
	topicInfo.Epoch = 1
	pid, _ := strconv.Atoi(partition)
	t, err := self.GetTopicInfo(topic, pid)

	if err != nil {
		self.CreateTopic(topic, pid+1, 3)
		self.CreateTopicPartition(topic, pid, 3)
		self.UpdateTopicNodeInfo(topic, pid, &topicInfo, topicInfo.Epoch)
	} else {
		t.Leader = leader
		if FindSlice(t.ISR, leader) == -1 {
			t.ISR = append(t.ISR, leader)
		}
		t.Epoch++
	}
}

// update leader, isr, epoch
func (self *FakeNsqlookupLeadership) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo, oldGen int) error {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		return ErrTopicNotCreated
	}
	tp, ok := t[partition]
	if !ok {
		return ErrTopicNotCreated
	}
	tp.topicInfo.Leader = topicInfo.Leader
	tp.topicInfo.ISR = topicInfo.ISR
	tp.topicInfo.Epoch = topicInfo.Epoch
	return nil
}

func (self *FakeNsqlookupLeadership) UpdateTopicCatchupList(topic string, partition int, catchupList []string, oldGen int) error {
	t, ok := self.fakeTopicsData[topic]
	if !ok {
		return ErrTopicNotCreated
	}
	tp, ok := t[partition]
	if !ok {
		return ErrTopicNotCreated
	}

	tp.topicInfo.CatchupList = catchupList
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

func (self *FakeNsqlookupLeadership) WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{}) {
	for {
		select {
		case <-stop:
			close(leader)
			return
		}
	}
}
