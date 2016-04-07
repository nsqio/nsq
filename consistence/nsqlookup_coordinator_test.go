package consistence

import (
	"github.com/absolute8511/glog"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

type fakeTopicData struct {
	metaInfo      *TopicPartionMetaInfo
	leaderSession *TopicLeaderSession
	leaderChanged chan struct{}
}

type FakeNsqlookupLeadership struct {
	fakeTopics    map[string]map[int]*fakeTopicData
	topicReplica  map[string]int
	fakeNsqdNodes map[string]NsqdNodeInfo
	nodeChanged   chan struct{}
	fakeEpoch     int
	fakeLeader    *NsqLookupdNodeInfo
	leaderChanged chan struct{}
}

func NewFakeNsqlookupLeadership() *FakeNsqlookupLeadership {
	return &FakeNsqlookupLeadership{
		fakeTopics:    make(map[string]map[int]*fakeTopicData),
		topicReplica:  make(map[string]int),
		fakeNsqdNodes: make(map[string]NsqdNodeInfo),
		nodeChanged:   make(chan struct{}, 1),
		leaderChanged: make(chan struct{}, 1),
	}
}

func (self *FakeNsqlookupLeadership) InitClusterID(id string) {
}

func (self *FakeNsqlookupLeadership) Register(value *NsqLookupdNodeInfo) error {
	self.fakeLeader = value
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
	coordLog.Infof("add fake node: %v", n)
	self.nodeChanged <- struct{}{}
}

func (self *FakeNsqlookupLeadership) removeFakedNsqdNode(nid string) {
	delete(self.fakeNsqdNodes, nid)
	coordLog.Infof("remove fake node: %v", nid)
	self.nodeChanged <- struct{}{}
}

func (self *FakeNsqlookupLeadership) WatchNsqdNodes(nsqds chan []*NsqdNodeInfo, stop chan struct{}) {
	for {
		select {
		case <-stop:
			close(nsqds)
			return
		case <-self.nodeChanged:
			nodes := make([]*NsqdNodeInfo, 0)
			for _, v := range self.fakeNsqdNodes {
				n := v
				nodes = append(nodes, &n)
			}
			nsqds <- nodes
		}
	}
}

func (self *FakeNsqlookupLeadership) ScanTopics() ([]*TopicPartionMetaInfo, error) {
	alltopics := make([]*TopicPartionMetaInfo, 0)
	for _, v := range self.fakeTopics {
		for _, topicInfo := range v {
			alltopics = append(alltopics, topicInfo.metaInfo)
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
	tmpInfo := *tp.metaInfo
	return &tmpInfo, nil
}

func (self *FakeNsqlookupLeadership) CreateTopicPartition(topic string, partition int) error {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return ErrTopicNotCreated
	}
	_, ok = t[partition]
	if ok {
		return ErrAlreadyExist
	}
	var newtp TopicPartionMetaInfo
	newtp.Name = topic
	newtp.Partition = partition
	newtp.Replica = self.topicReplica[topic]
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
		self.topicReplica[topic] = replica
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

func (self *FakeNsqlookupLeadership) CreateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartionMetaInfo) (error, int) {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return ErrTopicNotCreated, 0
	}
	tp, ok := t[partition]
	if !ok {
		tp = &fakeTopicData{}
		t[partition] = tp
	}
	newEpoch := tp.metaInfo.Epoch
	*tp.metaInfo = *topicInfo
	tp.metaInfo.Epoch = newEpoch + 1
	topicInfo.Epoch = tp.metaInfo.Epoch
	return nil, topicInfo.Epoch
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
	*tp.metaInfo = *topicInfo
	tp.metaInfo.Epoch = newEpoch + 1
	topicInfo.Epoch = tp.metaInfo.Epoch
	return nil
}

func (self *FakeNsqlookupLeadership) updateTopicLeaderSession(topic string, partition int, leader *TopicLeaderSession) {
	t, ok := self.fakeTopics[topic]
	if !ok {
		return
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

func (self *FakeNsqlookupLeadership) WatchTopicLeader(topic string, partition int, leader chan *TopicLeaderSession, stop chan struct{}) error {
	t, _ := self.fakeTopics[topic]
	leaderChanged := t[partition].leaderChanged
	for {
		select {
		case <-stop:
			close(leader)
			return nil
		case <-leaderChanged:
			l, _ := self.GetTopicLeaderSession(topic, partition)
			leader <- l
		}
	}
	return nil
}

func (self *FakeNsqlookupLeadership) RegisterNsqd(nodeData *NsqdNodeInfo) error {
	return nil
}

func (self *FakeNsqlookupLeadership) UnregisterNsqd(nodeData *NsqdNodeInfo) error {
	return nil
}

func (self *FakeNsqlookupLeadership) AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo) error {
	l, err := self.GetTopicLeaderSession(topic, partition)
	if err == nil {
		if l.LeaderNode == nil {
			l.LeaderNode = nodeData
			l.Session = "fake-leader-session" + nodeData.GetID()
			l.LeaderEpoch++
			t, _ := self.fakeTopics[topic]
			leaderChanged := t[partition].leaderChanged
			leaderChanged <- struct{}{}
			coordLog.Infof("leader session update to : %v", l)
			return nil
		} else if *l.LeaderNode == *nodeData {
			t, _ := self.fakeTopics[topic]
			leaderChanged := t[partition].leaderChanged
			leaderChanged <- struct{}{}
			return nil
		}
		return ErrLeaderSessionAlreadyExist
	}
	leaderSession := &TopicLeaderSession{}
	leaderSession.LeaderNode = nodeData
	leaderSession.Session = "fake-leader-session-" + nodeData.GetID()
	self.updateTopicLeaderSession(topic, partition, leaderSession)
	coordLog.Infof("leader session update to : %v", leaderSession)
	return nil
}

func (self *FakeNsqlookupLeadership) ReleaseTopicLeader(topic string, partition int, session *TopicLeaderSession) error {
	l, err := self.GetTopicLeaderSession(topic, partition)
	if err != nil {
		return err
	}
	coordLog.Infof("try release leader session with: %v", session)
	if !l.IsSame(session) {
		return ErrLeaderSessionMismatch
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
func startNsqLookupCoord(t *testing.T, id string) (*NsqLookupCoordinator, int, *NsqLookupdNodeInfo) {
	var n NsqLookupdNodeInfo
	n.ID = id
	n.NodeIp = "127.0.0.1"
	randPort := rand.Int31n(20000) + 30000
	n.RpcPort = strconv.Itoa(int(randPort))
	n.Epoch = 1
	coord := NewNsqLookupCoordinator("test-nsq-cluster", &n)
	coord.leadership = NewFakeNsqlookupLeadership()
	err := coord.Start()
	if err != nil {
		t.Fatal(err)
	}
	return coord, int(randPort), &n
}

func TestNsqLookupLeadershipChange(t *testing.T) {
	coord1, _, node1 := startNsqLookupCoord(t, "test-nsqlookup1")
	coord2, _, node2 := startNsqLookupCoord(t, "test-nsqlookup2")
	fakeLeadership1 := coord1.leadership.(*FakeNsqlookupLeadership)
	fakeLeadership1.changeLookupLeader(node1)
	time.Sleep(time.Second)
	fakeLeadership1.changeLookupLeader(node2)
	time.Sleep(time.Second)
	_ = coord1
	_ = coord2
}

func TestNsqLookupNsqdNodesChange(t *testing.T) {
	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}
	nsqdNodeInfoList := make(map[string]*NsqdNodeInfo)
	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqd3, randPort3, nodeInfo3, data3 := newNsqdNode(t, "id3")
	nsqd4, randPort4, nodeInfo4, data4 := newNsqdNode(t, "id4")
	nsqd5, randPort5, nodeInfo5, data5 := newNsqdNode(t, "id5")
	nsqdNodeInfoList[nodeInfo1.GetID()] = nodeInfo1
	nsqdNodeInfoList[nodeInfo2.GetID()] = nodeInfo2
	nsqdNodeInfoList[nodeInfo3.GetID()] = nodeInfo3
	nsqdNodeInfoList[nodeInfo4.GetID()] = nodeInfo4
	nsqdNodeInfoList[nodeInfo5.GetID()] = nodeInfo5

	nsqdCoord1 := startNsqdCoord(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1)
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	time.Sleep(time.Second)
	// start as isr
	nsqdCoord2 := startNsqdCoord(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2)
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	time.Sleep(time.Second)
	nsqdCoord3 := startNsqdCoord(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3)
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	time.Sleep(time.Second)
	nsqdCoord4 := startNsqdCoord(t, strconv.Itoa(int(randPort4)), data4, "id4", nsqd4)
	defer os.RemoveAll(data4)
	defer nsqd4.Exit()
	time.Sleep(time.Second)
	nsqdCoord5 := startNsqdCoord(t, strconv.Itoa(int(randPort5)), data5, "id5", nsqd5)
	defer os.RemoveAll(data5)
	defer nsqd5.Exit()

	topic := "test-nsqlookup-topic"
	lookupCoord1, _, lookupNode1 := startNsqLookupCoord(t, "test-nsqlookup1")

	nsqdCoord1.lookupLeader = lookupNode1
	nsqdCoord2.lookupLeader = lookupNode1
	nsqdCoord3.lookupLeader = lookupNode1
	nsqdCoord4.lookupLeader = lookupNode1
	nsqdCoord5.lookupLeader = lookupNode1

	fakeLeadership1 := lookupCoord1.leadership.(*FakeNsqlookupLeadership)
	fakeLeadership1.changeLookupLeader(lookupNode1)
	time.Sleep(time.Second)
	fakeLeadership1.addFakedNsqdNode(*nodeInfo1)
	time.Sleep(time.Second)
	fakeLeadership1.addFakedNsqdNode(*nodeInfo2)
	time.Sleep(time.Second)
	fakeLeadership1.addFakedNsqdNode(*nodeInfo3)
	time.Sleep(time.Second)
	fakeLeadership1.addFakedNsqdNode(*nodeInfo4)
	time.Sleep(time.Second)
	nsqdCoord1.leadership = fakeLeadership1
	nsqdCoord2.leadership = fakeLeadership1
	nsqdCoord3.leadership = fakeLeadership1
	nsqdCoord4.leadership = fakeLeadership1
	nsqdCoord5.leadership = fakeLeadership1
	nsqdCoord1.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoord2.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoord3.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoord4.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoord5.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoordList := make(map[string]*NsqdCoordinator)
	nsqdCoordList[nodeInfo1.GetID()] = nsqdCoord1
	nsqdCoordList[nodeInfo2.GetID()] = nsqdCoord2
	nsqdCoordList[nodeInfo3.GetID()] = nsqdCoord3
	nsqdCoordList[nodeInfo4.GetID()] = nsqdCoord4
	nsqdCoordList[nodeInfo5.GetID()] = nsqdCoord5
	// test new topic create
	err := lookupCoord1.CreateTopic(topic, 2, 2)
	test.Nil(t, err)
	time.Sleep(time.Second * 5)

	pn, err := fakeLeadership1.GetTopicPartitionNum(topic)
	test.Nil(t, err)
	test.Equal(t, pn, 2)
	t0, err := fakeLeadership1.GetTopicInfo(topic, 0)
	test.Nil(t, err)
	t1, err := fakeLeadership1.GetTopicInfo(topic, 1)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 2)
	test.Equal(t, len(t1.ISR), 2)
	t.Log(t0)
	t.Log(t1)
	test.NotEqual(t, t0.Leader, t1.Leader)

	t0LeaderCoord := nsqdCoordList[t0.Leader]
	test.NotNil(t, t0LeaderCoord)
	tc0, err := t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, err)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	test.Equal(t, len(tc0.topicInfo.ISR), 2)

	t1LeaderCoord := nsqdCoordList[t1.Leader]
	test.NotNil(t, t1LeaderCoord)
	tc1, err := t1LeaderCoord.getTopicCoord(topic, 1)
	test.Nil(t, err)
	test.Equal(t, tc1.topicInfo.Leader, t1.Leader)
	test.Equal(t, len(tc1.topicInfo.ISR), 2)

	// test isr node lost
	lostNodeID := t0.ISR[1]
	fakeLeadership1.removeFakedNsqdNode(lostNodeID)
	time.Sleep(time.Second * 5)

	t0, err = fakeLeadership1.GetTopicInfo(topic, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.CatchupList), 1)
	test.Equal(t, t0.CatchupList[0], lostNodeID)
	test.Equal(t, len(t0.ISR), 1)
	test.Equal(t, len(tc0.topicInfo.ISR), 1)
	test.Equal(t, t0.Leader, t0.ISR[0])

	oldTcNum := len(nsqdCoordList[lostNodeID].topicCoords)
	// clear topic info on failed node, test the reload for failed node
	nsqdCoordList[lostNodeID].topicCoords = make(map[string]map[int]*TopicCoordinator)

	// test new catchup and new isr
	fakeLeadership1.addFakedNsqdNode(*nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second * 5)

	test.Equal(t, len(nsqdCoordList[lostNodeID].topicCoords), oldTcNum)
	t0, _ = fakeLeadership1.GetTopicInfo(topic, 0)
	test.Equal(t, len(t0.CatchupList), 0)
	test.Equal(t, len(t0.ISR), 2)
	test.Equal(t, len(tc0.topicInfo.ISR), 2)
	test.Equal(t, t0.Leader, t0.ISR[0])

	// test leader node lost
	lostNodeID = t0.Leader
	fakeLeadership1.removeFakedNsqdNode(t0.Leader)
	fakeLeadership1.ReleaseTopicLeader(topic, 0, &tc0.topicLeaderSession)
	time.Sleep(time.Second * 5)
	t0, _ = fakeLeadership1.GetTopicInfo(topic, 0)
	t.Log(t0)
	test.Equal(t, len(t0.ISR), 1)
	test.Equal(t, t0.Leader, t0.ISR[0])
	test.NotEqual(t, t0.Leader, lostNodeID)
	test.Equal(t, len(t0.CatchupList), 1)
	t0LeaderCoord = nsqdCoordList[t0.Leader]
	test.NotNil(t, t0LeaderCoord)
	tc0, err = t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, err)
	test.Equal(t, len(tc0.topicInfo.ISR), 1)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)

	// test lost leader node rejoin
	fakeLeadership1.addFakedNsqdNode(*nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second * 5)
	t0, _ = fakeLeadership1.GetTopicInfo(topic, 0)
	t.Log(t0)

	test.Equal(t, len(t0.CatchupList), 0)
	test.Equal(t, len(t0.ISR), 2)
	t0LeaderCoord = nsqdCoordList[t0.Leader]
	test.NotNil(t, t0LeaderCoord)
	tc0, err = t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, err)
	test.Equal(t, len(tc0.topicInfo.ISR), 2)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	time.Sleep(time.Second * 3)

	// test old leader failed and begin elect new and then new leader failed
	lostNodeID = t0.Leader
	lostISRID := t0.ISR[1]
	fakeLeadership1.removeFakedNsqdNode(t0.Leader)
	fakeLeadership1.ReleaseTopicLeader(topic, 0, &tc0.topicLeaderSession)
	time.Sleep(time.Millisecond)
	fakeLeadership1.removeFakedNsqdNode(lostISRID)
	time.Sleep(time.Second * 5)
	fakeLeadership1.addFakedNsqdNode(*nsqdNodeInfoList[lostNodeID])
	fakeLeadership1.addFakedNsqdNode(*nsqdNodeInfoList[lostISRID])
	time.Sleep(time.Second * 5)
	t0, _ = fakeLeadership1.GetTopicInfo(topic, 0)
	test.Equal(t, len(t0.ISR), 2)
	test.Equal(t, t0.Leader == t0.ISR[0] || t0.Leader == t0.ISR[1], true)

	t0LeaderCoord = nsqdCoordList[t0.Leader]
	test.NotNil(t, t0LeaderCoord)
	tc0, err = t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, err)
	test.Equal(t, len(tc0.topicInfo.ISR), 2)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	time.Sleep(time.Second * 3)

	// test join isr timeout
	lostNodeID = t1.ISR[1]
	fakeLeadership1.removeFakedNsqdNode(lostNodeID)
	time.Sleep(time.Second * 3)
	fakeLeadership1.addFakedNsqdNode(*nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second)
	// with only 2 replica, the isr join fail should not change the isr list
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(true)
	go lookupCoord1.triggerCheckTopics(time.Second)
	time.Sleep(time.Second * 15)
	t1, _ = fakeLeadership1.GetTopicInfo(topic, 1)
	test.Equal(t, len(t1.ISR), 2)
	test.Equal(t, t1.Leader == t1.ISR[0] || t1.Leader == t1.ISR[1], true)
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(false)
	go lookupCoord1.triggerCheckTopics(time.Second)
	// test new topic create
	topic3 := topic + topic
	err = lookupCoord1.CreateTopic(topic3, 1, 3)
	test.Nil(t, err)
	time.Sleep(time.Second * 5)
	// with 3 replica, the isr join timeout will change the isr list if the isr has the quorum nodes
	t3, err := fakeLeadership1.GetTopicInfo(topic3, 0)
	test.Nil(t, err)
	test.Equal(t, len(t3.ISR), 3)
	lostNodeID = t3.ISR[1]
	fakeLeadership1.removeFakedNsqdNode(lostNodeID)
	time.Sleep(time.Second * 3)
	fakeLeadership1.addFakedNsqdNode(*nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Millisecond)
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(true)
	go lookupCoord1.triggerCheckTopics(time.Second)
	time.Sleep(time.Second * 10)
	t3, _ = fakeLeadership1.GetTopicInfo(topic3, 0)
	test.Equal(t, len(t3.ISR), 2)
	test.Equal(t, t3.Leader == t3.ISR[0] || t3.Leader == t3.ISR[1], true)
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(false)
	go lookupCoord1.triggerCheckTopics(time.Second)
	time.Sleep(time.Second * 10)
	glog.Flush()
	t0, _ = fakeLeadership1.GetTopicInfo(topic, 0)
	test.Equal(t, len(t0.ISR), 2)
	t1, _ = fakeLeadership1.GetTopicInfo(topic, 1)
	test.Equal(t, len(t1.ISR), 2)
	// before migrate really start, the isr should not reach the replica factor
	// catch up may start early while check leadership or enable topic write
	t3, _ = fakeLeadership1.GetTopicInfo(topic3, 0)
	test.Equal(t, len(t3.ISR)+len(t3.CatchupList), 3)
}

func TestNsqLookupNsqdMigrate(t *testing.T) {
}
