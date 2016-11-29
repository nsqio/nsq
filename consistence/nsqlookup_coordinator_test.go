package consistence

import (
	"errors"
	"github.com/absolute8511/glog"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testEtcdServers       = "10.9.3.13:2379"
	TEST_NSQ_CLUSTER_NAME = "test-nsq-cluster-unit-test"
)

type fakeTopicData struct {
	metaInfo      *TopicPartitionMetaInfo
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
	coordLog.Infof("unregistered nsqlookup: %v", v)
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

func (self *FakeNsqlookupLeadership) GetNsqdNodes() ([]NsqdNodeInfo, error) {
	nodes := make([]NsqdNodeInfo, 0)
	for _, v := range self.fakeNsqdNodes {
		n := v
		nodes = append(nodes, n)
	}

	return nodes, nil
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

func (self *FakeNsqlookupLeadership) ScanTopics() ([]TopicPartitionMetaInfo, error) {
	alltopics := make([]TopicPartitionMetaInfo, 0)
	for _, v := range self.fakeTopics {
		for _, topicInfo := range v {
			alltopics = append(alltopics, *topicInfo.metaInfo)
		}
	}
	return alltopics, nil
}

func (self *FakeNsqlookupLeadership) GetTopicInfo(topic string, partition int) (*TopicPartitionMetaInfo, error) {
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
	var newtp TopicPartitionMetaInfo
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
	} else {
		return errors.New("topic info already exist")
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

func (self *FakeNsqlookupLeadership) GetTopicMetaInfo(topic string) (TopicMetaInfo, EpochType, error) {
	t, ok := self.fakeTopics[topic]
	if !ok || len(t) == 0 {
		return TopicMetaInfo{}, 0, nil
	}
	return t[0].metaInfo.TopicMetaInfo, 0, nil
}

func (self *FakeNsqlookupLeadership) UpdateTopicMetaInfo(topic string, meta *TopicMetaInfo, oldGen EpochType) error {
	_, ok := self.fakeTopics[topic]
	if !ok {
		return errors.New("topic not exist")
	} else {
		self.fakeTopicMetaInfo[topic] = *meta
	}
	self.clusterEpoch++
	return nil
}

func (self *FakeNsqlookupLeadership) DeleteWholeTopic(topic string) error {
	delete(self.fakeTopics, topic)
	return nil
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
		return ErrEpochMismatch.ToErrorType()
	}
	newEpoch := tp.metaInfo.Epoch
	tp.metaInfo.TopicPartitionReplicaInfo = *topicInfo
	tp.metaInfo.Epoch = newEpoch + 1
	topicInfo.Epoch = tp.metaInfo.Epoch
	self.clusterEpoch++
	coordLog.Infof("topic %v-%v info updated: %v", topic, partition, self.fakeTopics[topic][partition].metaInfo)
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
		return nil, ErrLeaderSessionNotExist
	}
	tp, ok := t[partition]
	if !ok {
		return nil, ErrLeaderSessionNotExist
	}

	if tp.leaderSession == nil || tp.leaderSession.Session == "" {
		return nil, ErrLeaderSessionNotExist
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
	for name, tps := range self.fakeTopics {
		for pid, t := range tps {
			if t.leaderSession != nil && t.leaderSession.LeaderNode != nil {
				if t.leaderSession.LeaderNode.GetID() == nodeData.GetID() {
					coordLog.Infof("!!!!! releasing the topic leader %v while unregister.", t.leaderSession)
					self.ReleaseTopicLeader(name, pid, t.leaderSession)
				}
			}
		}
	}
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
		coordLog.Infof("failed release with mismatch session : %v", l)
		return ErrLeaderSessionMismatch.ToErrorType()
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

func startNsqLookupCoord(t *testing.T, useFakeLeadership bool) (*NsqLookupCoordinator, int, *NsqLookupdNodeInfo) {
	var n NsqLookupdNodeInfo
	n.NodeIP = "127.0.0.1"
	randPort := rand.Int31n(20000) + 30000
	n.RpcPort = strconv.Itoa(int(randPort))
	n.TcpPort = "0"
	n.Epoch = 1
	n.ID = GenNsqLookupNodeID(&n, "")
	opts := &Options{
		BalanceStart: 1,
		BalanceEnd:   23,
	}
	coord := NewNsqLookupCoordinator(TEST_NSQ_CLUSTER_NAME, &n, opts)
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

func prepareCluster(t *testing.T, nodeList []string, useFakeLeadership bool) (*NsqLookupCoordinator, map[string]*testClusterNodeInfo) {
	nsqdNodeInfoList := make(map[string]*testClusterNodeInfo)

	for _, id := range nodeList {
		var n testClusterNodeInfo
		n.localNsqd, n.randPort, n.nodeInfo, n.dataPath = newNsqdNode(t, id)
		n.nsqdCoord = startNsqdCoord(t, strconv.Itoa(int(n.randPort)), n.dataPath, id, n.localNsqd, useFakeLeadership)
		n.id = id
		time.Sleep(time.Second)
		nsqdNodeInfoList[n.nodeInfo.GetID()] = &n
	}
	lookupCoord, _, lookupNode := startNsqLookupCoord(t, useFakeLeadership)
	if useFakeLeadership {
		fakeLeadership := lookupCoord.leadership.(*FakeNsqlookupLeadership)
		for _, n := range nsqdNodeInfoList {
			n.nsqdCoord.lookupLeader = *lookupNode
		}
		fakeLeadership.changeLookupLeader(lookupNode)
		time.Sleep(time.Second)
		for _, n := range nsqdNodeInfoList {
			n.nsqdCoord.leadership = fakeLeadership
		}
	}
	time.Sleep(time.Second)

	for _, n := range nsqdNodeInfoList {
		n.nsqdCoord.lookupRemoteCreateFunc = NewNsqLookupRpcClient
		err := n.nsqdCoord.Start()
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
	return lookupCoord, nsqdNodeInfoList
}

func TestNsqLookupLeadershipChange(t *testing.T) {
	SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	coord1, _, node1 := startNsqLookupCoord(t, true)
	coord2, _, node2 := startNsqLookupCoord(t, true)
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
	if testing.Verbose() {
		SetCoordLogger(&levellogger.GLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}
	idList := []string{"id1", "id2", "id3", "id4", "id5"}
	lookupCoord1, nodeInfoList := prepareCluster(t, idList, useFakeLeadership)
	for _, n := range nodeInfoList {
		defer os.RemoveAll(n.dataPath)
		defer n.localNsqd.Exit()
		defer n.nsqdCoord.Stop()
	}

	topic := "test-nsqlookup-topic-unit-test"
	lookupLeadership := lookupCoord1.leadership

	lookupCoord1.DeleteTopic(topic, "**")
	topic3 := topic + topic
	lookupCoord1.DeleteTopic(topic3, "**")
	defer func() {
		lookupCoord1.DeleteTopic(topic, "**")
		lookupCoord1.DeleteTopic(topic3, "**")
		time.Sleep(time.Second * 3)
		lookupCoord1.Stop()
	}()

	// test new topic create
	err := lookupCoord1.CreateTopic(topic, TopicMetaInfo{2, 2, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second * 5)

	pmeta, _, err := lookupLeadership.GetTopicMetaInfo(topic)
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

	t0LeaderCoord := nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr := t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, coordErr)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	test.Equal(t, len(tc0.topicInfo.ISR), 2)

	t1LeaderCoord := nodeInfoList[t1.Leader].nsqdCoord
	test.NotNil(t, t1LeaderCoord)
	tc1, coordErr := t1LeaderCoord.getTopicCoord(topic, 1)
	test.Nil(t, coordErr)
	test.Equal(t, tc1.topicInfo.Leader, t1.Leader)
	test.Equal(t, len(tc1.topicInfo.ISR), 2)

	coordLog.Warningf("============= begin test isr node failed  ====")
	// test isr node lost
	lostNodeID := t0.ISR[1]
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 1)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.UnregisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Second * 5)

	t0, err = lookupLeadership.GetTopicInfo(topic, 0)
	test.Nil(t, err)
	test.Equal(t, FindSlice(t0.ISR, lostNodeID) == -1, true)
	test.Equal(t, len(t0.ISR), t0.Replica)
	test.Equal(t, t0.Leader, t0.ISR[0])

	// clear topic info on failed node, test the reload for failed node
	nodeInfoList[lostNodeID].nsqdCoord.topicCoords = make(map[string]map[int]*TopicCoordinator)

	// test new catchup and new isr
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 0)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.RegisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Second * 5)

	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	test.Equal(t, len(t0.CatchupList), 0)
	test.Equal(t, len(t0.ISR) >= t0.Replica, true)
	test.Equal(t, len(tc0.topicInfo.ISR), len(t0.ISR))
	test.Equal(t, t0.Leader, t0.ISR[0])
	lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	// should remove the unnecessary node
	test.Equal(t, len(t0.ISR), t0.Replica)

	coordLog.Warningf("============= begin test leader failed  ====")
	// test leader node lost
	lostNodeID = t0.Leader
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 1)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.UnregisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	go lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 5)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	t.Log(t0)
	test.Equal(t, t0.Replica, len(t0.ISR))
	test.Equal(t, t0.Leader, t0.ISR[0])
	test.NotEqual(t, t0.Leader, lostNodeID)
	//test.Equal(t, len(t0.CatchupList), 1)
	test.Equal(t, FindSlice(t0.ISR, lostNodeID) == -1, true)
	t0LeaderCoord = nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr = t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, coordErr)
	test.Equal(t, len(tc0.topicInfo.ISR), len(t0.ISR))
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)

	// test lost leader node rejoin
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 0)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.RegisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Second * 5)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	t.Log(t0)

	test.Equal(t, len(t0.CatchupList), 0)
	test.Equal(t, len(t0.ISR) >= t0.Replica, true)
	t0LeaderCoord = nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr = t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, coordErr)
	test.Equal(t, len(tc0.topicInfo.ISR), len(t0.ISR))
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 3)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	// should remove the unnecessary node
	test.Equal(t, len(t0.ISR), t0.Replica)

	// test old leader failed and begin elect new and then new leader failed
	coordLog.Warningf("============= begin test old leader failed and then new leader failed ====")
	lostNodeID = t0.Leader
	lostISRID := t0.ISR[1]
	if lostISRID == lostNodeID {
		lostISRID = t0.ISR[0]
	}
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 1)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.UnregisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Millisecond)
	atomic.StoreInt32(&nodeInfoList[lostISRID].nsqdCoord.stopping, 1)
	nodeInfoList[lostISRID].nsqdCoord.leadership.UnregisterNsqd(nodeInfoList[lostISRID].nodeInfo)
	time.Sleep(time.Second * 5)
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 0)
	atomic.StoreInt32(&nodeInfoList[lostISRID].nsqdCoord.stopping, 0)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.RegisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	nodeInfoList[lostISRID].nsqdCoord.leadership.RegisterNsqd(nodeInfoList[lostISRID].nodeInfo)
	go lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 5)
	go lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 5)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	test.Equal(t, true, len(t0.ISR) >= t0.Replica)
	test.Equal(t, t0.Leader == t0.ISR[0] || t0.Leader == t0.ISR[1], true)

	t0LeaderCoord = nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr = t0LeaderCoord.getTopicCoord(topic, 0)
	test.Nil(t, coordErr)
	test.Equal(t, len(tc0.topicInfo.ISR), len(t0.ISR))
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 3)
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	// should remove the unnecessary node
	test.Equal(t, t0.Replica, len(t0.ISR))

	// test join isr timeout
	lostNodeID = t1.ISR[1]
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 1)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.UnregisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Second * 3)
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 0)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.RegisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Millisecond * 5)
	// with only 2 replica, the isr join fail should not change the isr list
	nodeInfoList[lostNodeID].nsqdCoord.rpcServer.toggleDisableRpcTest(true)
	go lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 15)
	t1, _ = lookupLeadership.GetTopicInfo(topic, 1)
	test.Equal(t, true, len(t1.ISR)+len(t1.CatchupList) >= t1.Replica)
	test.Equal(t, t1.Leader == t1.ISR[0] || t1.Leader == t1.ISR[1], true)
	nodeInfoList[lostNodeID].nsqdCoord.rpcServer.toggleDisableRpcTest(false)
	go lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 3)
	// test new topic create
	coordLog.Warningf("============= begin test 3 replicas ====")
	err = lookupCoord1.CreateTopic(topic3, TopicMetaInfo{1, 3, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second * 5)
	// with 3 replica, the isr join timeout will change the isr list if the isr has the quorum nodes
	t3, err := lookupLeadership.GetTopicInfo(topic3, 0)
	test.Nil(t, err)
	test.Equal(t, len(t3.ISR), t3.Replica)
	lostNodeID = t3.ISR[1]
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 1)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.UnregisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Second * 3)
	atomic.StoreInt32(&nodeInfoList[lostNodeID].nsqdCoord.stopping, 0)
	nodeInfoList[lostNodeID].nsqdCoord.leadership.RegisterNsqd(nodeInfoList[lostNodeID].nodeInfo)
	time.Sleep(time.Millisecond)
	nodeInfoList[lostNodeID].nsqdCoord.rpcServer.toggleDisableRpcTest(true)
	go lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 5)
	t3, _ = lookupLeadership.GetTopicInfo(topic3, 0)
	test.Equal(t, true, len(t3.ISR) >= t3.Replica-1)
	test.Equal(t, true, len(t3.ISR) < t3.Replica)
	test.Equal(t, t3.Leader == t3.ISR[0] || t3.Leader == t3.ISR[1], true)
	nodeInfoList[lostNodeID].nsqdCoord.rpcServer.toggleDisableRpcTest(false)
	go lookupCoord1.triggerCheckTopics("", 0, time.Second)
	time.Sleep(time.Second * 5)
	glog.Flush()
	t0, _ = lookupLeadership.GetTopicInfo(topic, 0)
	test.Equal(t, true, len(t0.ISR) >= t0.Replica)
	t1, _ = lookupLeadership.GetTopicInfo(topic, 1)
	test.Equal(t, true, len(t1.ISR) >= t0.Replica)
	// before migrate really start, the isr should not reach the replica factor
	// however, catch up may start early while check leadership or enable topic write
	t3, _ = lookupLeadership.GetTopicInfo(topic3, 0)
	test.Equal(t, true, len(t3.ISR)+len(t3.CatchupList) >= t3.Replica)

	t0IsrNum := 2
	t1IsrNum := 2
	coordLog.Warningf("========== begin test quit ====")

	quitList := make([]*NsqdCoordinator, 0)
	quitList = append(quitList, nodeInfoList[t0.Leader].nsqdCoord)
	if t1.Leader != t0.Leader {
		quitList = append(quitList, nodeInfoList[t1.Leader].nsqdCoord)
	}
	if t3.Leader != t0.Leader && t3.Leader != t1.Leader {
		quitList = append(quitList, nodeInfoList[t3.Leader].nsqdCoord)
	}
	for id, n := range nodeInfoList {
		if id == t0.Leader || id == t1.Leader || id == t3.Leader {
			continue
		}
		quitList = append(quitList, n.nsqdCoord)
	}
	test.Equal(t, len(nodeInfoList), len(quitList))

	for _, nsqdCoord := range quitList {
		failedID := nsqdCoord.myNode.GetID()
		delete(nodeInfoList, failedID)
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
		t.Log(t0)
		test.Equal(t, FindSlice(t0.ISR, failedID) == -1 || (len(t0.ISR) == 1 && t0.ISR[0] == failedID), true)
		test.Equal(t, true, len(t0.ISR) >= t0IsrNum)
		t1, _ = lookupLeadership.GetTopicInfo(topic, 1)
		t.Log(t1)
		test.Equal(t, FindSlice(t1.ISR, failedID) == -1 || (len(t1.ISR) == 1 && t1.ISR[0] == failedID), true)
		test.Equal(t, true, len(t1.ISR) >= t1IsrNum)
		t3, _ = lookupLeadership.GetTopicInfo(topic3, 0)
		t.Log(t3)
		test.Equal(t, FindSlice(t3.ISR, failedID) == -1 || (len(t3.ISR) == 1 && t3.ISR[0] == failedID), true)
	}
}

func TestNsqLookupNsqdCreateTopic(t *testing.T) {
	// on 4 nodes, we should test follow cases
	// 1 partition 1 replica
	// 1 partition 3 replica
	// 3 partition 1 replica
	// 2 partition 2 replica
	if testing.Verbose() {
		SetCoordLogger(&levellogger.GLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}
	idList := []string{"id1", "id2", "id3", "id4"}
	lookupCoord1, nodeInfoList := prepareCluster(t, idList, false)
	for _, n := range nodeInfoList {
		defer os.RemoveAll(n.dataPath)
		defer n.localNsqd.Exit()
		defer n.nsqdCoord.Stop()
	}
	test.Equal(t, 4, len(nodeInfoList))

	topic_p1_r1 := "test-nsqlookup-topic-unit-testcreate-p1-r1"
	topic_p1_r3 := "test-nsqlookup-topic-unit-testcreate-p1-r3"
	topic_p3_r1 := "test-nsqlookup-topic-unit-testcreate-p3-r1"
	topic_p2_r2 := "test-nsqlookup-topic-unit-testcreate-p2-r2"

	lookupLeadership := lookupCoord1.leadership

	time.Sleep(time.Second)
	lookupCoord1.DeleteTopic(topic_p1_r1, "**")
	lookupCoord1.DeleteTopic(topic_p1_r3, "**")
	lookupCoord1.DeleteTopic(topic_p3_r1, "**")
	lookupCoord1.DeleteTopic(topic_p2_r2, "**")
	time.Sleep(time.Second * 3)
	defer func() {
		lookupCoord1.DeleteTopic(topic_p1_r1, "**")
		lookupCoord1.DeleteTopic(topic_p1_r3, "**")
		lookupCoord1.DeleteTopic(topic_p3_r1, "**")
		lookupCoord1.DeleteTopic(topic_p2_r2, "**")
		time.Sleep(time.Second * 3)

		lookupCoord1.Stop()
	}()

	// test new topic create
	err := lookupCoord1.CreateTopic(topic_p1_r1, TopicMetaInfo{1, 1, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)
	lookupCoord1.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	pmeta, _, err := lookupLeadership.GetTopicMetaInfo(topic_p1_r1)
	pn := pmeta.PartitionNum
	test.Nil(t, err)
	test.Equal(t, pn, 1)
	t0, err := lookupLeadership.GetTopicInfo(topic_p1_r1, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 1)

	t.Logf("t0 leader is: %v", t0.Leader)
	if nodeInfoList[t0.Leader] == nil {
		t.Fatalf("no leader: %v, %v", t0, nodeInfoList)
	}
	t0LeaderCoord := nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr := t0LeaderCoord.getTopicCoord(topic_p1_r1, 0)
	test.Nil(t, coordErr)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	test.Equal(t, len(tc0.topicInfo.ISR), 1)

	err = lookupCoord1.CreateTopic(topic_p1_r3, TopicMetaInfo{1, 3, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second * 5)
	lookupCoord1.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	pmeta, _, err = lookupLeadership.GetTopicMetaInfo(topic_p1_r3)
	pn = pmeta.PartitionNum
	test.Nil(t, err)
	test.Equal(t, pn, 1)
	t0, err = lookupLeadership.GetTopicInfo(topic_p1_r3, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 3)

	t.Logf("t0 leader is: %v", t0.Leader)
	if nodeInfoList[t0.Leader] == nil {
		t.Fatalf("no leader: %v, %v", t0, nodeInfoList)
	}

	t0LeaderCoord = nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr = t0LeaderCoord.getTopicCoord(topic_p1_r3, 0)
	test.Nil(t, coordErr)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	test.Equal(t, len(tc0.topicInfo.ISR), 3)

	err = lookupCoord1.CreateTopic(topic_p3_r1, TopicMetaInfo{3, 1, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second * 5)
	lookupCoord1.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	pmeta, _, err = lookupLeadership.GetTopicMetaInfo(topic_p3_r1)
	pn = pmeta.PartitionNum
	test.Nil(t, err)
	test.Equal(t, pn, 3)
	t0, err = lookupLeadership.GetTopicInfo(topic_p3_r1, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 1)

	t.Logf("t0 leader is: %v", t0.Leader)
	if nodeInfoList[t0.Leader] == nil {
		t.Fatalf("no leader: %v, %v", t0, nodeInfoList)
	}

	t0LeaderCoord = nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr = t0LeaderCoord.getTopicCoord(topic_p3_r1, 0)
	test.Nil(t, coordErr)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	test.Equal(t, len(tc0.topicInfo.ISR), 1)

	t1, err := lookupLeadership.GetTopicInfo(topic_p3_r1, 1)
	t1LeaderCoord := nodeInfoList[t1.Leader].nsqdCoord
	test.NotNil(t, t1LeaderCoord)
	tc1, coordErr := t1LeaderCoord.getTopicCoord(topic_p3_r1, 1)
	test.Nil(t, coordErr)
	test.Equal(t, tc1.topicInfo.Leader, t1.Leader)
	test.Equal(t, len(tc1.topicInfo.ISR), 1)

	err = lookupCoord1.CreateTopic(topic_p2_r2, TopicMetaInfo{2, 2, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second * 5)
	lookupCoord1.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	pmeta, _, err = lookupLeadership.GetTopicMetaInfo(topic_p2_r2)
	pn = pmeta.PartitionNum
	test.Nil(t, err)
	test.Equal(t, pn, 2)
	t0, err = lookupLeadership.GetTopicInfo(topic_p2_r2, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 2)

	t.Logf("t0 leader is: %v", t0.Leader)
	if nodeInfoList[t0.Leader] == nil {
		t.Fatalf("no leader: %v, %v", t0, nodeInfoList)
	}

	t0LeaderCoord = nodeInfoList[t0.Leader].nsqdCoord
	test.NotNil(t, t0LeaderCoord)
	tc0, coordErr = t0LeaderCoord.getTopicCoord(topic_p2_r2, 0)
	test.Nil(t, coordErr)
	test.Equal(t, tc0.topicInfo.Leader, t0.Leader)
	test.Equal(t, len(tc0.topicInfo.ISR), 2)

	t1, err = lookupLeadership.GetTopicInfo(topic_p2_r2, 1)
	t1LeaderCoord = nodeInfoList[t1.Leader].nsqdCoord
	test.NotNil(t, t1LeaderCoord)
	tc1, coordErr = t1LeaderCoord.getTopicCoord(topic_p2_r2, 1)
	test.Nil(t, coordErr)
	test.Equal(t, tc1.topicInfo.Leader, t1.Leader)
	test.Equal(t, len(tc1.topicInfo.ISR), 2)

	// test create on exist topic, create on partial partition
	oldMeta, _, err := lookupCoord1.leadership.GetTopicMetaInfo(topic_p2_r2)
	test.Nil(t, err)
	err = lookupCoord1.CreateTopic(topic_p2_r2, TopicMetaInfo{2, 2, 0, 0, 1, 1})
	test.NotNil(t, err)
	time.Sleep(time.Second * 5)
	lookupCoord1.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	newMeta, _, err := lookupCoord1.leadership.GetTopicMetaInfo(topic_p2_r2)
	test.Nil(t, err)
	test.Equal(t, oldMeta, newMeta)
}

func TestNsqLookupUpdateTopicMeta(t *testing.T) {
	if testing.Verbose() {
		SetCoordLogger(&levellogger.GLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

	idList := []string{"id1", "id2", "id3", "id4"}
	lookupCoord, nodeInfoList := prepareCluster(t, idList, false)
	for _, n := range nodeInfoList {
		defer os.RemoveAll(n.dataPath)
		defer n.localNsqd.Exit()
		defer n.nsqdCoord.Stop()
	}

	topic_p1_r1 := "test-nsqlookup-topic-unit-test-updatemeta-p1-r1"
	topic_p2_r1 := "test-nsqlookup-topic-unit-test-updatemeta-p2-r1"
	lookupLeadership := lookupCoord.leadership

	lookupCoord.DeleteTopic(topic_p1_r1, "**")
	lookupCoord.DeleteTopic(topic_p2_r1, "**")
	time.Sleep(time.Second * 3)
	defer func() {
		lookupCoord.DeleteTopic(topic_p1_r1, "**")
		lookupCoord.DeleteTopic(topic_p2_r1, "**")
		time.Sleep(time.Second * 3)
		lookupCoord.Stop()
	}()

	err := lookupCoord.CreateTopic(topic_p1_r1, TopicMetaInfo{1, 1, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)

	err = lookupCoord.CreateTopic(topic_p2_r1, TopicMetaInfo{2, 1, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second * 3)

	// test increase replicator and decrease the replicator
	err = lookupCoord.ChangeTopicMetaParam(topic_p1_r1, -1, -1, 3)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	tmeta, _, _ := lookupLeadership.GetTopicMetaInfo(topic_p1_r1)
	test.Equal(t, 3, tmeta.Replica)
	for i := 0; i < tmeta.PartitionNum; i++ {
		info, err := lookupLeadership.GetTopicInfo(topic_p1_r1, i)
		test.Nil(t, err)
		test.Equal(t, tmeta.Replica, len(info.ISR))
	}

	err = lookupCoord.ChangeTopicMetaParam(topic_p1_r1, -1, -1, 2)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	tmeta, _, _ = lookupLeadership.GetTopicMetaInfo(topic_p1_r1)
	test.Equal(t, 2, tmeta.Replica)
	for i := 0; i < tmeta.PartitionNum; i++ {
		info, err := lookupLeadership.GetTopicInfo(topic_p1_r1, i)
		test.Nil(t, err)
		test.Equal(t, tmeta.Replica, len(info.ISR))
	}

	err = lookupCoord.ChangeTopicMetaParam(topic_p2_r1, -1, -1, 2)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	tmeta, _, _ = lookupLeadership.GetTopicMetaInfo(topic_p2_r1)
	test.Equal(t, 2, tmeta.Replica)
	for i := 0; i < tmeta.PartitionNum; i++ {
		info, err := lookupLeadership.GetTopicInfo(topic_p2_r1, i)
		test.Nil(t, err)
		test.Equal(t, tmeta.Replica, len(info.ISR))
	}

	// should fail
	err = lookupCoord.ChangeTopicMetaParam(topic_p2_r1, -1, -1, 3)
	test.NotNil(t, err)

	err = lookupCoord.ChangeTopicMetaParam(topic_p2_r1, -1, -1, 1)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	tmeta, _, _ = lookupLeadership.GetTopicMetaInfo(topic_p2_r1)
	test.Equal(t, 1, tmeta.Replica)
	for i := 0; i < tmeta.PartitionNum; i++ {
		info, err := lookupLeadership.GetTopicInfo(topic_p2_r1, i)
		test.Nil(t, err)
		test.Equal(t, tmeta.Replica, len(info.ISR))
	}

	// test update the sync and retention , all partition and replica should be updated
	err = lookupCoord.ChangeTopicMetaParam(topic_p1_r1, 1234, 3, -1)
	tmeta, _, _ = lookupLeadership.GetTopicMetaInfo(topic_p1_r1)
	test.Equal(t, 1234, tmeta.SyncEvery)
	test.Equal(t, int32(3), tmeta.RetentionDay)
	for i := 0; i < tmeta.PartitionNum; i++ {
		info, err := lookupLeadership.GetTopicInfo(topic_p1_r1, i)
		test.Nil(t, err)
		for _, nid := range info.ISR {
			localNsqd := nodeInfoList[nid].localNsqd
			localTopic, err := localNsqd.GetExistingTopic(topic_p1_r1, i)
			test.Nil(t, err)
			dinfo := localTopic.GetDynamicInfo()
			test.Equal(t, int64(1234), dinfo.SyncEvery)
			test.Equal(t, int32(3), dinfo.RetentionDay)
		}
	}
}

func TestNsqLookupMarkNodeRemove(t *testing.T) {
	if testing.Verbose() {
		SetCoordLogger(&levellogger.GLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

	idList := []string{"id1", "id2", "id3", "id4", "id5"}
	lookupCoord, nodeInfoList := prepareCluster(t, idList, false)
	for _, n := range nodeInfoList {
		defer os.RemoveAll(n.dataPath)
		defer n.localNsqd.Exit()
		defer n.nsqdCoord.Stop()
	}

	topic_p4_r1 := "test-nsqlookup-topic-unit-test-removenode-p4-r1"
	topic_p2_r2 := "test-nsqlookup-topic-unit-test-removenode-p2-r2"
	topic_p1_r3 := "test-nsqlookup-topic-unit-test-removenode-p1-r3"
	lookupLeadership := lookupCoord.leadership

	lookupCoord.DeleteTopic(topic_p4_r1, "**")
	lookupCoord.DeleteTopic(topic_p2_r2, "**")
	lookupCoord.DeleteTopic(topic_p1_r3, "**")
	time.Sleep(time.Second * 3)
	defer func() {
		lookupCoord.DeleteTopic(topic_p4_r1, "**")
		lookupCoord.DeleteTopic(topic_p2_r2, "**")
		lookupCoord.DeleteTopic(topic_p1_r3, "**")
		time.Sleep(time.Second * 3)
		lookupCoord.Stop()
	}()

	err := lookupCoord.CreateTopic(topic_p4_r1, TopicMetaInfo{4, 1, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)

	err = lookupCoord.CreateTopic(topic_p2_r2, TopicMetaInfo{2, 2, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)

	err = lookupCoord.CreateTopic(topic_p1_r3, TopicMetaInfo{1, 3, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)

	nid := ""
	for _, n := range nodeInfoList {
		nid = n.nodeInfo.GetID()
		break
	}
	err = lookupCoord.MarkNodeAsRemoving(nid)
	test.Nil(t, err)
	checkStart := time.Now()
	for time.Since(checkStart) < time.Minute*2 {
		time.Sleep(time.Second)
		isDone := true
		for i := 0; i < 4; i++ {
			info, err := lookupLeadership.GetTopicInfo(topic_p4_r1, i)
			test.Nil(t, err)
			if FindSlice(info.ISR, nid) != -1 {
				t.Logf("still waiting remove: %v", info)
				isDone = false
				break
			}
		}
		if !isDone {
			continue
		}
		time.Sleep(time.Second)
		for i := 0; i < 2; i++ {
			info, err := lookupLeadership.GetTopicInfo(topic_p2_r2, i)
			test.Nil(t, err)
			if FindSlice(info.ISR, nid) != -1 {
				t.Logf("still waiting remove: %v", info)
				isDone = false
				break
			}
		}
		if !isDone {
			continue
		}
		time.Sleep(time.Second)
		info, err := lookupLeadership.GetTopicInfo(topic_p1_r3, 0)
		test.Nil(t, err)
		if FindSlice(info.ISR, nid) != -1 {
			t.Logf("still waiting remove: %v from removing node", info)
			isDone = false
		}
		t.Logf("all done")

		if isDone {
			break
		}
	}
	for time.Since(checkStart) < time.Minute*2 {
		lookupCoord.nodesMutex.Lock()
		state := lookupCoord.removingNodes[nid]
		lookupCoord.nodesMutex.Unlock()
		if state == "data_transfered" || state == "done" {
			break
		} else {
			t.Logf("still waiting state: %v ", state)
		}
		time.Sleep(time.Second)
	}
	if time.Since(checkStart) >= time.Minute*2 {
		t.Error("remove node timeout")
	}
}

func TestNsqLookupExpandPartition(t *testing.T) {
	if testing.Verbose() {
		SetCoordLogger(&levellogger.GLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

	idList := []string{"id1", "id2", "id3", "id4", "id5", "id6"}
	lookupCoord, nodeInfoList := prepareCluster(t, idList, false)
	for _, n := range nodeInfoList {
		defer os.RemoveAll(n.dataPath)
		defer n.localNsqd.Exit()
		defer n.nsqdCoord.Stop()
	}

	topic_p1_r1 := "test-nsqlookup-topic-unit-test-expand-p1-r1"
	topic_p1_r2 := "test-nsqlookup-topic-unit-test-expand-p1-r2"
	topic_p1_r3 := "test-nsqlookup-topic-unit-test-expand-p1-r3"
	lookupLeadership := lookupCoord.leadership

	lookupCoord.DeleteTopic(topic_p1_r1, "**")
	lookupCoord.DeleteTopic(topic_p1_r2, "**")
	lookupCoord.DeleteTopic(topic_p1_r3, "**")
	time.Sleep(time.Second * 3)
	defer func() {
		lookupCoord.DeleteTopic(topic_p1_r1, "**")
		lookupCoord.DeleteTopic(topic_p1_r2, "**")
		lookupCoord.DeleteTopic(topic_p1_r3, "**")
		time.Sleep(time.Second * 3)
		lookupCoord.Stop()
	}()

	err := lookupCoord.CreateTopic(topic_p1_r1, TopicMetaInfo{1, 1, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)

	err = lookupCoord.CreateTopic(topic_p1_r2, TopicMetaInfo{1, 2, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)

	err = lookupCoord.CreateTopic(topic_p1_r3, TopicMetaInfo{1, 3, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	err = lookupCoord.ExpandTopicPartition(topic_p1_r1, 3)
	test.Nil(t, err)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	t0, err := lookupLeadership.GetTopicInfo(topic_p1_r1, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 1)
	t1, err := lookupLeadership.GetTopicInfo(topic_p1_r1, 1)
	test.Nil(t, err)
	test.Equal(t, len(t1.ISR), 1)
	t2, err := lookupLeadership.GetTopicInfo(topic_p1_r1, 2)
	test.Nil(t, err)
	test.Equal(t, len(t2.ISR), 1)

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	err = lookupCoord.ExpandTopicPartition(topic_p1_r2, 2)
	test.Nil(t, err)
	time.Sleep(time.Second * 3)
	t0, err = lookupLeadership.GetTopicInfo(topic_p1_r2, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), t0.Replica)
	t1, err = lookupLeadership.GetTopicInfo(topic_p1_r2, 1)
	test.Nil(t, err)
	test.Equal(t, len(t1.ISR), t1.Replica)

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	err = lookupCoord.ExpandTopicPartition(topic_p1_r2, 3)
	test.Nil(t, err)
	time.Sleep(time.Second * 3)
	t0, err = lookupLeadership.GetTopicInfo(topic_p1_r2, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), t0.Replica)
	t1, err = lookupLeadership.GetTopicInfo(topic_p1_r2, 1)
	test.Nil(t, err)
	test.Equal(t, len(t1.ISR), t1.Replica)
	t2, err = lookupLeadership.GetTopicInfo(topic_p1_r2, 2)
	test.Nil(t, err)
	test.Equal(t, len(t2.ISR), t2.Replica)

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	// should fail
	err = lookupCoord.ExpandTopicPartition(topic_p1_r2, 4)
	test.NotNil(t, err)

	err = lookupCoord.ExpandTopicPartition(topic_p1_r3, 2)
	test.Nil(t, err)
	time.Sleep(time.Second * 3)
	t0, err = lookupLeadership.GetTopicInfo(topic_p1_r3, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), t0.Replica)
	t1, err = lookupLeadership.GetTopicInfo(topic_p1_r3, 1)
	test.Nil(t, err)
	test.Equal(t, len(t1.ISR), t1.Replica)

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	// should fail
	err = lookupCoord.ExpandTopicPartition(topic_p1_r3, 3)
	test.NotNil(t, err)
}

func TestNsqLookupMovePartition(t *testing.T) {
	if testing.Verbose() {
		SetCoordLogger(&levellogger.GLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

	idList := []string{"id1", "id2", "id3", "id4", "id5"}
	lookupCoord, nodeInfoList := prepareCluster(t, idList, false)
	for _, n := range nodeInfoList {
		defer os.RemoveAll(n.dataPath)
		defer n.localNsqd.Exit()
		defer n.nsqdCoord.Stop()
	}

	topic_p1_r1 := "test-nsqlookup-topic-unit-test-move-p1-r1"
	topic_p2_r2 := "test-nsqlookup-topic-unit-test-move-p2-r2"
	lookupLeadership := lookupCoord.leadership

	lookupCoord.DeleteTopic(topic_p1_r1, "**")
	lookupCoord.DeleteTopic(topic_p2_r2, "**")
	time.Sleep(time.Second * 3)
	defer func() {
		lookupCoord.DeleteTopic(topic_p1_r1, "**")
		lookupCoord.DeleteTopic(topic_p2_r2, "**")
		time.Sleep(time.Second * 3)
		lookupCoord.Stop()
	}()

	// test new topic create
	err := lookupCoord.CreateTopic(topic_p1_r1, TopicMetaInfo{1, 1, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)

	err = lookupCoord.CreateTopic(topic_p2_r2, TopicMetaInfo{2, 2, 0, 0, 0, 0})
	test.Nil(t, err)
	time.Sleep(time.Second)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	// test move leader to other isr;
	// test move leader to other catchup;
	// test move non-leader to other node;
	t0, err := lookupLeadership.GetTopicInfo(topic_p1_r1, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 1)

	// move p1_r1 leader to other node
	toNode := ""
	for _, node := range nodeInfoList {
		if node.nodeInfo.GetID() == t0.Leader {
			continue
		}
		toNode = node.nodeInfo.GetID()
		break
	}
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	err = lookupCoord.MoveTopicPartitionDataByManual(topic_p1_r1, 0, true, t0.Leader, toNode)
	test.Nil(t, err)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)

	t0, err = lookupLeadership.GetTopicInfo(topic_p1_r1, 0)
	test.Nil(t, err)
	// it may be two nodes in isr if the moved leader rejoin as isr
	test.Equal(t, len(t0.ISR) >= 1, true)
	test.Equal(t, t0.Leader, toNode)

	t0, err = lookupLeadership.GetTopicInfo(topic_p2_r2, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR), 2)

	toNode = ""
	for _, nid := range t0.ISR {
		if nid == t0.Leader {
			continue
		}
		toNode = nid
		break
	}
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 2)
	// move leader to other isr node
	oldLeader := t0.Leader
	err = lookupCoord.MoveTopicPartitionDataByManual(topic_p2_r2, 0, true, t0.Leader, toNode)
	test.Nil(t, err)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	t0, err = lookupLeadership.GetTopicInfo(topic_p2_r2, 0)
	test.Nil(t, err)
	test.Equal(t, len(t0.ISR) >= 2, true)
	test.NotEqual(t, t0.Leader, oldLeader)
	test.Equal(t, t0.Leader, toNode)

	// move leader to other non-isr node
	toNode = ""
	for _, node := range nodeInfoList {
		if FindSlice(t0.ISR, node.nodeInfo.GetID()) != -1 {
			continue
		}
		// check other partition
		t1, err := lookupLeadership.GetTopicInfo(topic_p2_r2, 1)
		if err == nil {
			if FindSlice(t1.ISR, node.nodeInfo.GetID()) != -1 {
				continue
			}
		}
		toNode = node.nodeInfo.GetID()
		break
	}

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	err = lookupCoord.MoveTopicPartitionDataByManual(topic_p2_r2, 0, true, t0.Leader, toNode)
	test.Nil(t, err)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	t0, err = lookupLeadership.GetTopicInfo(topic_p2_r2, 0)
	test.Nil(t, err)
	test.Equal(t, t0.Leader, toNode)

	// move non-leader to other non-isr node
	toNode = ""
	toNodeInvalid := ""
	fromNode := ""
	for _, nid := range t0.ISR {
		if nid != t0.Leader {
			fromNode = nid
		}
	}
	for _, node := range nodeInfoList {
		if FindSlice(t0.ISR, node.nodeInfo.GetID()) != -1 {
			continue
		}
		// check other partition
		t1, err := lookupLeadership.GetTopicInfo(topic_p2_r2, 1)
		if err == nil {
			toNodeInvalid = t1.Leader
			if FindSlice(t1.ISR, node.nodeInfo.GetID()) != -1 {
				continue
			}
		}
		toNode = node.nodeInfo.GetID()
		break
	}
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	err = lookupCoord.MoveTopicPartitionDataByManual(topic_p2_r2, 0, false, fromNode, toNodeInvalid)
	test.NotNil(t, err)
	test.Equal(t, ErrNodeIsExcludedForTopicData, err)

	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second)
	err = lookupCoord.MoveTopicPartitionDataByManual(topic_p2_r2, 0, false, fromNode, toNode)
	test.Nil(t, err)
	lookupCoord.triggerCheckTopics("", 0, 0)
	time.Sleep(time.Second * 3)
	t0, err = lookupLeadership.GetTopicInfo(topic_p2_r2, 0)
	test.Nil(t, err)
	test.Equal(t, FindSlice(t0.ISR, toNode) != -1, true)
	test.Equal(t, -1, FindSlice(t0.ISR, fromNode))

}
