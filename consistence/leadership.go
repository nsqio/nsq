package consistence

import (
	"errors"
	"strconv"
)

var (
	ErrLeaderSessionAlreadyExist = errors.New("leader session already exist")
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
	LeaderEpoch int32
}

func (self *TopicLeaderSession) IsSame(other *TopicLeaderSession) bool {
	if other == nil || self == nil {
		return false
	}
	if self.LeaderEpoch != other.LeaderEpoch {
		return false
	}
	if self.Session != "" && other.Session != "" && self.Session != other.Session {
		return false
	}
	if self.LeaderNode == nil || other.LeaderNode == nil {
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
	GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error)
	WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{})
}

type NSQDLeadership interface {
	InitClusterID(id string)
	RegisterNsqd(nodeData NsqdNodeInfo) error
	UnregisterNsqd(nodeData NsqdNodeInfo) error
	AcquireTopicLeader(topic string, partition int, nodeData NsqdNodeInfo) error
	ReleaseTopicLeader(topic string, partition int) error
	WatchLookupdLeader(key string, leader chan *NsqLookupdNodeInfo, stop chan struct{}) error
	GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error)
	GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error)
}

type fakeTopicData struct {
	metaInfo      *TopicPartionMetaInfo
	leaderSession *TopicLeaderSession
	leaderChanged chan struct{}
}

type FakeNsqlookupLeadership struct {
	fakeTopics    map[string]map[int]*fakeTopicData
	fakeNsqdNodes map[string]NsqdNodeInfo
	nodeChanged   chan struct{}
	fakeEpoch     int
	fakeLeader    *NsqLookupdNodeInfo
	leaderChanged chan struct{}
}

func NewFakeNsqlookupLeadership() *FakeNsqlookupLeadership {
	return &FakeNsqlookupLeadership{
		fakeTopics:    make(map[string]map[int]*fakeTopicData),
		fakeNsqdNodes: make(map[string]NsqdNodeInfo),
		nodeChanged:   make(chan struct{}, 1),
		leaderChanged: make(chan struct{}, 1),
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

func (self *FakeNsqlookupLeadership) changeLookupLeader(newLeader *NsqLookupdNodeInfo) {
	self.fakeLeader = newLeader
	self.leaderChanged <- struct{}{}
}

func (self *FakeNsqlookupLeadership) AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			close(leader)
			return
		case <-self.leaderChanged:
			leader <- self.fakeLeader
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

func (self *FakeNsqlookupLeadership) addFakedNsqdNode(n NsqdNodeInfo) {
	self.fakeNsqdNodes[n.GetID()] = n
	self.nodeChanged <- struct{}{}
}

func (self *FakeNsqlookupLeadership) removeFakedNsqdNode(nid string) {
	delete(self.fakeNsqdNodes, nid)
	self.nodeChanged <- struct{}{}
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
	for _, v := range self.fakeTopics {
		for _, topicInfo := range v {
			alltopics = append(alltopics, *topicInfo.metaInfo)
		}
	}
	return alltopics, nil
}

func (self *FakeNsqlookupLeadership) GetTopicInfo(topic string, partition int) (*TopicPartionMetaInfo, error) {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return nil, ErrTopicNotCreated
	}
	tp, ok := t[partition]
	if !ok {
		return nil, ErrTopicNotCreated
	}
	return tp.metaInfo, nil
}

func (self *FakeNsqlookupLeadership) CreateTopicPartition(topic string, partition int, replica int) error {
	t, ok := self.fakeTopics[topic]
	if !ok {
		t = make(map[int]*fakeTopicData)
		self.fakeTopics[topic] = t
	}
	_, ok = t[partition]
	if ok {
		return ErrAlreadyExist
	}
	var newtp TopicPartionMetaInfo
	newtp.Name = topic
	newtp.Partition = partition
	newtp.Replica = replica
	newtp.Epoch = 1
	var fakeData fakeTopicData
	fakeData.metaInfo = &newtp
	fakeData.leaderChanged = make(chan struct{}, 1)
	t[partition] = &fakeData
	return nil
}

func (self *FakeNsqlookupLeadership) CreateTopic(topic string, partitionNum int, replica int) error {
	t, ok := self.fakeTopics[topic]
	if !ok {
		t = make(map[int]*fakeTopicData)
		self.fakeTopics[topic] = t
	}
	return nil
}

func (self *FakeNsqlookupLeadership) IsExistTopic(topic string) (bool, error) {
	_, ok := self.fakeTopics[topic]
	return ok, nil
}

func (self *FakeNsqlookupLeadership) IsExistTopicPartition(topic string, partition int) (bool, error) {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return false, nil
	}
	_, ok = t[partition]
	return ok, nil
}

func (self *FakeNsqlookupLeadership) GetTopicPartitionNum(topic string) (int, error) {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return 0, nil
	}
	return len(t), nil
}

func (self *FakeNsqlookupLeadership) DeleteTopic(topic string, partition int) error {
	delete(self.fakeTopics[topic], partition)
	return nil
}

// update leader, isr, epoch
func (self *FakeNsqlookupLeadership) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo, oldGen int) error {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return ErrTopicNotCreated
	}
	tp, ok := t[partition]
	if !ok {
		return ErrTopicNotCreated
	}
	if tp.metaInfo.Epoch != oldGen {
		return ErrEpochMismatch
	}
	newEpoch := tp.metaInfo.Epoch
	tp.metaInfo = topicInfo
	tp.metaInfo.Epoch = newEpoch + 1
	oldGen++
	return nil
}

func (self *FakeNsqlookupLeadership) updateTopicLeaderSession(topic string, partition int, leader *TopicLeaderSession) {
	t, ok := self.fakeTopics[topic]
	if !ok {
		t = make(map[int]*fakeTopicData)
		self.fakeTopics[topic] = t
	}
	tp, ok := t[partition]
	if ok && tp.leaderSession != nil {
		tp.leaderSession.LeaderNode = leader.LeaderNode
		tp.leaderSession.Session = leader.Session
		tp.leaderSession.LeaderEpoch++
		tp.leaderChanged <- struct{}{}
		return
	}
	var newtp TopicLeaderSession
	newtp = *leader
	t[partition].leaderSession = &newtp
	tp.leaderChanged <- struct{}{}
}

func (self *FakeNsqlookupLeadership) GetTopicLeaderSession(topic string, partition int) (*TopicLeaderSession, error) {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return nil, ErrMissingTopicLeaderSession
	}
	tp, ok := t[partition]
	if !ok {
		return nil, ErrMissingTopicLeaderSession
	}

	if tp.leaderSession == nil {
		return nil, ErrMissingTopicLeaderSession
	}

	return tp.leaderSession, nil
}

func (self *FakeNsqlookupLeadership) WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{}) {
	t, _ := self.fakeTopics[topic]
	leaderChanged := t[partition].leaderChanged
	for {
		select {
		case <-stop:
			close(leader)
			return
		case <-leaderChanged:
			l, _ := self.GetTopicLeaderSession(topic, partition)
			leader <- l
		}
	}
}

func (self *FakeNsqlookupLeadership) RegisterNsqd(nodeData NsqdNodeInfo) error {
	return nil
}

func (self *FakeNsqlookupLeadership) UnregisterNsqd(nodeData NsqdNodeInfo) error {
	return nil
}

func (self *FakeNsqlookupLeadership) AcquireTopicLeader(topic string, partition int, nodeData NsqdNodeInfo) error {
	l, err := self.GetTopicLeaderSession(topic, partition)
	if err == nil {
		if *l.LeaderNode == nodeData {
			t, _ := self.fakeTopics[topic]
			leaderChanged := t[partition].leaderChanged
			leaderChanged <- struct{}{}
			return nil
		}
		return ErrLeaderSessionAlreadyExist
	}
	leaderSession := &TopicLeaderSession{}
	leaderSession.LeaderNode = &nodeData
	leaderSession.Session = "fake-leader-session"
	self.updateTopicLeaderSession(topic, partition, leaderSession)
	return nil
}

func (self *FakeNsqlookupLeadership) ReleaseTopicLeader(topic string, partition int) error {
	l, err := self.GetTopicLeaderSession(topic, partition)
	if err != nil {
		return err
	}
	l.LeaderNode = nil
	l.Session = ""
	l.LeaderEpoch++
	t, _ := self.fakeTopics[topic]
	leaderChanged := t[partition].leaderChanged
	leaderChanged <- struct{}{}
	return nil
}

func (self *FakeNsqlookupLeadership) WatchLookupdLeader(key string, leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	for {
		select {
		case <-stop:
			close(leader)
			return nil
		}
	}
	return nil
}
