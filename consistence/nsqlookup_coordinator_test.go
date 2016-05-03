package consistence

import (
	"github.com/absolute8511/glog"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	testEtcdServers       = "192.168.66.202:2379"
	TEST_NSQ_CLUSTER_NAME = "test-nsq-cluster"
)

type fakeTopicData struct {
	metaInfo      *TopicPartionMetaInfo
	leaderSession *TopicLeaderSession
	leaderChanged chan struct{}
}

type FakeNsqlookupLeadership struct {
	fakeTopics           map[string]map[int]*fakeTopicData
	fakeTopicMetaInfo    map[string]TopicMetaInfo
	fakeNsqdNodes        map[string]NsqdNodeInfo
	nodeChanged          chan struct{}
	fakeEpoch            EpochType
	fakeLeader           *NsqLookupdNodeInfo
	leaderChanged        chan struct{}
	leaderSessionChanged chan *TopicLeaderSession
	clusterEpoch         EpochType
	exitChan             chan struct{}
}

func NewFakeNsqlookupLeadership() *FakeNsqlookupLeadership {
	return &FakeNsqlookupLeadership{
		fakeTopics:           make(map[string]map[int]*fakeTopicData),
		fakeTopicMetaInfo:    make(map[string]TopicMetaInfo),
		fakeNsqdNodes:        make(map[string]NsqdNodeInfo),
		nodeChanged:          make(chan struct{}, 1),
		leaderChanged:        make(chan struct{}, 1),
		leaderSessionChanged: make(chan *TopicLeaderSession, 1),
		exitChan:             make(chan struct{}),
	}
}

func (self *FakeNsqlookupLeadership) InitClusterID(id string) {
}

func (self *FakeNsqlookupLeadership) GetClusterEpoch() (EpochType, error) {
	return self.clusterEpoch, nil
}

func (self *FakeNsqlookupLeadership) Register(value *NsqLookupdNodeInfo) error {
	self.fakeLeader = value
	return nil
}

func (self *FakeNsqlookupLeadership) Unregister(v *NsqLookupdNodeInfo) error {
	self.fakeLeader = nil
	return nil
}

func (self *FakeNsqlookupLeadership) Stop() {
	close(self.exitChan)
}

func (self *FakeNsqlookupLeadership) changeLookupLeader(newLeader *NsqLookupdNodeInfo) {
	self.fakeLeader = newLeader
	select {
	case self.leaderChanged <- struct{}{}:
	case <-self.exitChan:
		return
	}
}

func (self *FakeNsqlookupLeadership) GetAllLookupdNodes() ([]NsqLookupdNodeInfo, error) {
	v := make([]NsqLookupdNodeInfo, 0)
	v = append(v, *self.fakeLeader)
	return v, nil
}

func (self *FakeNsqlookupLeadership) AcquireAndWatchLeader(leader chan *NsqLookupdNodeInfo, stopChan chan struct{}) {
	defer close(leader)
	for {
		select {
		case <-stopChan:
			return
		case <-self.leaderChanged:
			select {
			case leader <- self.fakeLeader:
			case <-self.exitChan:
				return
			}
		}
	}
}

func (self *FakeNsqlookupLeadership) CheckIfLeader(session string) bool {
	return true
}

func (self *FakeNsqlookupLeadership) UpdateLookupEpoch(oldGen EpochType) (EpochType, error) {
	self.fakeEpoch++
	return self.fakeEpoch, nil
}

func (self *FakeNsqlookupLeadership) addFakedNsqdNode(n NsqdNodeInfo) {
	self.fakeNsqdNodes[n.GetID()] = n
	coordLog.Infof("add fake node: %v", n)
	select {
	case self.nodeChanged <- struct{}{}:
	default:
	}

	self.clusterEpoch++
}

func (self *FakeNsqlookupLeadership) removeFakedNsqdNode(nid string) {
	delete(self.fakeNsqdNodes, nid)
	coordLog.Infof("remove fake node: %v", nid)
	select {
	case self.nodeChanged <- struct{}{}:
	default:
	}
	self.clusterEpoch++
}

func (self *FakeNsqlookupLeadership) WatchNsqdNodes(nsqds chan []NsqdNodeInfo, stop chan struct{}) {
	defer close(nsqds)
	for {
		select {
		case <-stop:
			return
		case <-self.nodeChanged:
			nodes := make([]NsqdNodeInfo, 0)
			for _, v := range self.fakeNsqdNodes {
				n := v
				nodes = append(nodes, n)
			}
			select {
			case nsqds <- nodes:
			case <-self.exitChan:
				return
			}
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
	newtp.TopicMetaInfo = self.fakeTopicMetaInfo[topic]
	newtp.Epoch = 0
	var fakeData fakeTopicData
	fakeData.metaInfo = &newtp
	fakeData.leaderChanged = make(chan struct{}, 1)
	t[partition] = &fakeData
	coordLog.Infof("topic partition init: %v-%v, %v", topic, partition, newtp)
	self.clusterEpoch++
	return nil
}

func (self *FakeNsqlookupLeadership) CreateTopic(topic string, meta *TopicMetaInfo) error {
	t, ok := self.fakeTopics[topic]
	if !ok {
		t = make(map[int]*fakeTopicData)
		self.fakeTopics[topic] = t
		self.fakeTopicMetaInfo[topic] = *meta
	}
	self.clusterEpoch++
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

func (self *FakeNsqlookupLeadership) GetTopicMetaInfo(topic string) (TopicMetaInfo, error) {
	t, ok := self.fakeTopics[topic]
	if !ok || len(t) == 0 {
		return TopicMetaInfo{}, nil
	}
	return t[0].metaInfo.TopicMetaInfo, nil
}

func (self *FakeNsqlookupLeadership) DeleteTopic(topic string, partition int) error {
	delete(self.fakeTopics[topic], partition)
	self.clusterEpoch++
	return nil
}

// update leader, isr, epoch
func (self *FakeNsqlookupLeadership) UpdateTopicNodeInfo(topic string, partition int, topicInfo *TopicPartitionReplicaInfo, oldGen EpochType) error {
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
	tp.metaInfo.TopicPartitionReplicaInfo = *topicInfo
	tp.metaInfo.Epoch = newEpoch + 1
	topicInfo.Epoch = tp.metaInfo.Epoch
	self.clusterEpoch++
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
		select {
		case self.leaderSessionChanged <- leader:
		default:
		}
		return
	}
	self.clusterEpoch++
	var newtp TopicLeaderSession
	newtp = *leader
	t[partition].leaderSession = &newtp
	select {
	case self.leaderSessionChanged <- &newtp:
	default:
	}
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

func (self *FakeNsqlookupLeadership) WatchTopicLeader(leader chan *TopicLeaderSession, stop chan struct{}) error {
	defer close(leader)
	for {
		select {
		case <-stop:
			return nil
		case s := <-self.leaderSessionChanged:
			if s == nil {
				continue
			}
			select {
			case leader <- s:
			case <-self.exitChan:
				return nil
			}
		}
	}
	return nil
}

func (self *FakeNsqlookupLeadership) RegisterNsqd(nodeData *NsqdNodeInfo) error {
	self.addFakedNsqdNode(*nodeData)
	return nil
}

func (self *FakeNsqlookupLeadership) UnregisterNsqd(nodeData *NsqdNodeInfo) error {
	self.removeFakedNsqdNode(nodeData.GetID())
	return nil
}

func (self *FakeNsqlookupLeadership) AcquireTopicLeader(topic string, partition int, nodeData *NsqdNodeInfo, epoch EpochType) error {
	l, err := self.GetTopicLeaderSession(topic, partition)
	if err == nil {
		if l.LeaderNode == nil {
			l.LeaderNode = nodeData
			l.Session = "fake-leader-session" + nodeData.GetID()
			l.LeaderEpoch++
			select {
			case self.leaderSessionChanged <- l:
			default:
			}
			coordLog.Infof("leader session update to : %v", l)
			return nil
		} else if *l.LeaderNode == *nodeData {
			// leader unchange.
			return nil
		}
		return ErrLeaderSessionAlreadyExist
	}
	leaderSession := &TopicLeaderSession{}
	leaderSession.Topic = topic
	leaderSession.Partition = partition
	leaderSession.LeaderNode = nodeData
	leaderSession.Session = "fake-leader-session-" + nodeData.GetID()
	self.updateTopicLeaderSession(topic, partition, leaderSession)
	coordLog.Infof("leader session update to : %v", leaderSession)
	self.clusterEpoch++
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
	select {
	case self.leaderSessionChanged <- l:
	default:
	}
	self.clusterEpoch++
	return nil
}

func (self *FakeNsqlookupLeadership) WatchLookupdLeader(leader chan *NsqLookupdNodeInfo, stop chan struct{}) error {
	for {
		select {
		case <-stop:
			close(leader)
			return nil
		}
	}
	return nil
}

func startNsqLookupCoord(t *testing.T, id string, useFakeLeadership bool) (*NsqLookupCoordinator, int, *NsqLookupdNodeInfo) {
	var n NsqLookupdNodeInfo
	n.NodeIp = "127.0.0.1"
	randPort := rand.Int31n(20000) + 30000
	n.RpcPort = strconv.Itoa(int(randPort))
	n.Epoch = 1
	n.ID = GenNsqLookupNodeID(&n, "")
	coord := NewNsqLookupCoordinator(TEST_NSQ_CLUSTER_NAME, &n)
	if useFakeLeadership {
		coord.leadership = NewFakeNsqlookupLeadership()
	} else {
		coord.SetLeadershipMgr(NewNsqLookupdEtcdMgr(testEtcdServers))
		coord.leadership.Unregister(&coord.myNode)
		//panic("not test")
	}
	err := coord.Start()
	if err != nil {
		t.Fatal(err)
	}
	return coord, int(randPort), &n
}

func TestNsqLookupLeadershipChange(t *testing.T) {
	coord1, _, node1 := startNsqLookupCoord(t, "test-nsqlookup1", true)
	coord2, _, node2 := startNsqLookupCoord(t, "test-nsqlookup2", true)
	fakeLeadership1 := coord1.leadership.(*FakeNsqlookupLeadership)
	fakeLeadership1.changeLookupLeader(node1)
	time.Sleep(time.Second)
	fakeLeadership1.changeLookupLeader(node2)
	time.Sleep(time.Second)
	_ = coord1
	_ = coord2
}

func TestFakeNsqLookupNsqdNodesChange(t *testing.T) {
	testNsqLookupNsqdNodesChange(t, true)
}

func TestNsqLookupNsqdNodesChange(t *testing.T) {
	testNsqLookupNsqdNodesChange(t, false)
}

func testNsqLookupNsqdNodesChange(t *testing.T, useFakeLeadership bool) {
	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}

	SetEtcdMgrLogger(log.New(os.Stderr, "go-x-lock", log.LstdFlags))

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

	nsqdCoord1 := startNsqdCoord(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, useFakeLeadership)
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	time.Sleep(time.Second)
	// start as isr
	nsqdCoord2 := startNsqdCoord(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, useFakeLeadership)
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	time.Sleep(time.Second)
	nsqdCoord3 := startNsqdCoord(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3, useFakeLeadership)
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	time.Sleep(time.Second)
	nsqdCoord4 := startNsqdCoord(t, strconv.Itoa(int(randPort4)), data4, "id4", nsqd4, useFakeLeadership)
	defer os.RemoveAll(data4)
	defer nsqd4.Exit()
	time.Sleep(time.Second)
	nsqdCoord5 := startNsqdCoord(t, strconv.Itoa(int(randPort5)), data5, "id5", nsqd5, useFakeLeadership)
	defer os.RemoveAll(data5)
	defer nsqd5.Exit()

	topic := "test-nsqlookup-topic"
	lookupCoord1, _, lookupNode1 := startNsqLookupCoord(t, "test-nsqlookup1", useFakeLeadership)
	lookupCoord1.leadership.DeleteTopic(topic, 0)
	lookupCoord1.leadership.DeleteTopic(topic, 1)
	topic3 := topic + topic
	lookupCoord1.leadership.DeleteTopic(topic3, 0)
	defer lookupCoord1.Stop()

	lookupLeadership := lookupCoord1.leadership
	if useFakeLeadership {
		fakeLeadership1 := lookupCoord1.leadership.(*FakeNsqlookupLeadership)
		nsqdCoord1.lookupLeader = *lookupNode1
		nsqdCoord2.lookupLeader = *lookupNode1
		nsqdCoord3.lookupLeader = *lookupNode1
		nsqdCoord4.lookupLeader = *lookupNode1
		nsqdCoord5.lookupLeader = *lookupNode1

		fakeLeadership1.changeLookupLeader(lookupNode1)
		time.Sleep(time.Second)
		nsqdCoord1.leadership = fakeLeadership1
		nsqdCoord2.leadership = fakeLeadership1
		nsqdCoord3.leadership = fakeLeadership1
		nsqdCoord4.leadership = fakeLeadership1
		nsqdCoord5.leadership = fakeLeadership1
	}

	time.Sleep(time.Second)
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
	//nsqdCoordList[nodeInfo5.GetID()] = nsqdCoord5
	for _, nsqdCoord := range nsqdCoordList {
		err := nsqdCoord.Start()
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
	// test new topic create
	err := lookupCoord1.CreateTopic(topic, 2, 2, 0)
	test.Nil(t, err)
	time.Sleep(time.Second * 5)

	pmeta, err := lookupLeadership.GetTopicMetaInfo(topic)
	pn := pmeta.PartitionNum
	test.Nil(t, err)
	test.Equal(t, pn, 2)
	t0, err := lookupLeadership.GetTopicInfo(topic, 0)
	test.Nil(t, err)
	t1, err := lookupLeadership.GetTopicInfo(topic, 1)
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
	nsqdCoordList[lostNodeID].leadership.UnregisterNsqd(nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second * 5)

	t0, err = lookupLeadership.GetTopicInfo(topic, 0)
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
	nsqdCoordList[lostNodeID].leadership.RegisterNsqd(nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second * 5)

	test.Equal(t, len(nsqdCoordList[lostNodeID].topicCoords), oldTcNum)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	test.Equal(t, len(t0.CatchupList), 0)
	test.Equal(t, len(t0.ISR), 2)
	test.Equal(t, len(tc0.topicInfo.ISR), 2)
	test.Equal(t, t0.Leader, t0.ISR[0])

	// test leader node lost
	lostNodeID = t0.Leader
	nsqdCoordList[lostNodeID].leadership.UnregisterNsqd(nsqdNodeInfoList[lostNodeID])
	nsqdCoordList[lostNodeID].leadership.ReleaseTopicLeader(topic, 0, &tc0.topicLeaderSession)
	time.Sleep(time.Second * 5)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
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
	nsqdCoordList[lostNodeID].leadership.RegisterNsqd(nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second * 5)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
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
	nsqdCoordList[lostNodeID].leadership.UnregisterNsqd(nsqdNodeInfoList[lostNodeID])
	nsqdCoordList[lostNodeID].leadership.ReleaseTopicLeader(topic, 0, &tc0.topicLeaderSession)
	time.Sleep(time.Millisecond)
	nsqdCoordList[lostISRID].leadership.UnregisterNsqd(nsqdNodeInfoList[lostISRID])
	time.Sleep(time.Second * 5)
	nsqdCoordList[lostNodeID].leadership.RegisterNsqd(nsqdNodeInfoList[lostNodeID])
	nsqdCoordList[lostISRID].leadership.RegisterNsqd(nsqdNodeInfoList[lostISRID])
	time.Sleep(time.Second * 5)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
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
	nsqdCoordList[lostNodeID].leadership.UnregisterNsqd(nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second * 3)
	nsqdCoordList[lostNodeID].leadership.RegisterNsqd(nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Millisecond * 5)
	// with only 2 replica, the isr join fail should not change the isr list
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(true)
	go lookupCoord1.triggerCheckTopics(time.Second)
	time.Sleep(time.Second * 15)
	t1, _ = lookupLeadership.GetTopicInfo(topic, 1)
	test.Equal(t, len(t1.ISR)+len(t1.CatchupList), 2)
	test.Equal(t, t1.Leader == t1.ISR[0] || t1.Leader == t1.ISR[1], true)
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(false)
	go lookupCoord1.triggerCheckTopics(time.Second)
	time.Sleep(time.Second * 3)
	// test new topic create
	err = lookupCoord1.CreateTopic(topic3, 1, 3, 0)
	test.Nil(t, err)
	time.Sleep(time.Second * 5)
	// with 3 replica, the isr join timeout will change the isr list if the isr has the quorum nodes
	t3, err := lookupLeadership.GetTopicInfo(topic3, 0)
	test.Nil(t, err)
	test.Equal(t, len(t3.ISR), 3)
	lostNodeID = t3.ISR[1]
	nsqdCoordList[lostNodeID].leadership.UnregisterNsqd(nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Second * 3)
	nsqdCoordList[lostNodeID].leadership.RegisterNsqd(nsqdNodeInfoList[lostNodeID])
	time.Sleep(time.Millisecond)
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(true)
	go lookupCoord1.triggerCheckTopics(time.Second)
	time.Sleep(time.Second * 10)
	t3, _ = lookupLeadership.GetTopicInfo(topic3, 0)
	test.Equal(t, len(t3.ISR), 2)
	test.Equal(t, t3.Leader == t3.ISR[0] || t3.Leader == t3.ISR[1], true)
	nsqdCoordList[lostNodeID].rpcServer.toggleDisableRpcTest(false)
	go lookupCoord1.triggerCheckTopics(time.Second)
	time.Sleep(time.Second * 10)
	glog.Flush()
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	test.Equal(t, len(t0.ISR), 2)
	t1, _ = lookupLeadership.GetTopicInfo(topic, 1)
	test.Equal(t, len(t1.ISR), 2)
	// before migrate really start, the isr should not reach the replica factor
	// however, catch up may start early while check leadership or enable topic write
	t3, _ = lookupLeadership.GetTopicInfo(topic3, 0)
	test.Equal(t, len(t3.ISR)+len(t3.CatchupList), 3)

	t0IsrNum := 2
	t1IsrNum := 2
	for _, nsqdCoord := range nsqdCoordList {
		failedID := nsqdCoord.myNode.GetID()
		nsqdCoord.Stop()
		if t0IsrNum > 1 {
			if FindSlice(t0.ISR, failedID) != -1 {
				t0IsrNum--
			}
		}
		if t1IsrNum > 1 {
			if FindSlice(t1.ISR, failedID) != -1 {
				t1IsrNum--
			}
		}

		time.Sleep(time.Second * 5)
		t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
		// we have no failed node in isr or we got the last failed node leaving in isr.
		test.Equal(t, FindSlice(t0.ISR, failedID) == -1 || (len(t0.ISR) == 1 && t0.ISR[0] == failedID), true)
		test.Equal(t, len(t0.ISR), t0IsrNum)
		t1, _ = lookupLeadership.GetTopicInfo(topic, 1)
		test.Equal(t, FindSlice(t1.ISR, failedID) == -1 || (len(t1.ISR) == 1 && t1.ISR[0] == failedID), true)
		test.Equal(t, len(t1.ISR), t1IsrNum)
		t.Log(t0)
		t.Log(t1)
	}
}

func TestNsqLookupNsqdMigrate(t *testing.T) {

}
