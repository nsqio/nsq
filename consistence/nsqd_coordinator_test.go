package consistence

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	nsqdNs "github.com/absolute8511/nsq/nsqd"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type fakeLookupRemoteProxy struct {
	leaderSessions map[string]map[int]*TopicLeaderSession
	fakeNsqdCoords map[string]*NsqdCoordinator
	lookupEpoch    int
	t              *testing.T
}

func NewFakeLookupRemoteProxy(addr string, timeout time.Duration) (INsqlookupRemoteProxy, error) {
	return &fakeLookupRemoteProxy{
		leaderSessions: make(map[string]map[int]*TopicLeaderSession),
		fakeNsqdCoords: make(map[string]*NsqdCoordinator),
	}, nil
}

func (self *fakeLookupRemoteProxy) Reconnect() error {
	return nil
}

func (self *fakeLookupRemoteProxy) RequestJoinCatchup(topic string, partition int, nid string) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting join catchup")
	}
	return nil
}

func (self *fakeLookupRemoteProxy) RequestJoinTopicISR(topic string, partition int, nid string) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting join isr")
	}
	return nil
}

func (self *fakeLookupRemoteProxy) ReadyForTopicISR(topic string, partition int, nid string, leaderSession *TopicLeaderSession, isr []string) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting ready for isr")
	}
	localSession, ok := self.leaderSessions[topic]
	if !ok {
		return ErrMissingTopicLeaderSession
	}

	if leaderSession == nil {
		// need push the current leadership to the node
		req := RpcTopicLeaderSession{}
		req.LeaderNode = localSession[partition].LeaderNode
		req.LookupdEpoch = self.lookupEpoch
		req.TopicName = topic
		req.TopicPartition = partition
		req.WaitReady = false
		req.TopicEpoch = localSession[partition].LeaderEpoch
		req.TopicLeaderEpoch = req.TopicEpoch
		req.TopicLeaderSession = localSession[partition].Session
		ret := true
		self.fakeNsqdCoords[nid].rpcServer.NotifyTopicLeaderSession(req, &ret)
		return nil
	}
	if s, ok := localSession[partition]; ok {
		if s.IsSame(leaderSession) {
			return nil
		}
	}
	return ErrLeaderSessionMismatch
}

func (self *fakeLookupRemoteProxy) PrepareLeaveFromISR(topic string, partition int, nid string) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting prepare leave isr")
	}
	return nil
}

func (self *fakeLookupRemoteProxy) RequestLeaveFromISR(topic string, partition int, nid string) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting leave isr")
	}
	return nil
}

func (self *fakeLookupRemoteProxy) RequestLeaveFromISRByLeader(topic string, partition int, nid string, leaderSession *TopicLeaderSession) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting leave isr by leader")
	}
	if self.leaderSessions[topic][partition].IsSame(leaderSession) {
		return nil
	}
	return ErrNotTopicLeader
}

func mustStartNSQD(opts *nsqdNs.Options) *nsqdNs.NSQD {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}
	nsqd := nsqdNs.New(opts)
	return nsqd
}

func ensureTopicOnNsqdCoord(nsqdCoord *NsqdCoordinator, topicInfo RpcAdminTopicInfo) *TopicCoordinator {
	coords, ok := nsqdCoord.topicCoords[topicInfo.Name]
	if !ok {
		coords = make(map[int]*TopicCoordinator)
		nsqdCoord.topicCoords[topicInfo.Name] = coords
	}
	tpCoord, ok := coords[topicInfo.Partition]
	if !ok {
		path := GetTopicPartitionBasePath(nsqdCoord.dataRootPath, topicInfo.Name, topicInfo.Partition)
		tpCoord = NewTopicCoordinator(topicInfo.Name, topicInfo.Partition, path)
		coords[topicInfo.Partition] = tpCoord
	}
	err := nsqdCoord.updateTopicInfo(tpCoord, false, &topicInfo.TopicPartionMetaInfo)
	if err != nil {
		panic(err)
	}
	return tpCoord
}

func ensureTopicLeaderSession(nsqdCoord *NsqdCoordinator, session RpcTopicLeaderSession) {
	tc, err := nsqdCoord.getTopicCoord(session.TopicName, session.TopicPartition)
	if err != nil {
		panic(err)
	}
	newSession := &TopicLeaderSession{
		LeaderNode:  session.LeaderNode,
		Session:     session.TopicLeaderSession,
		LeaderEpoch: session.TopicLeaderEpoch,
	}
	err = nsqdCoord.updateTopicLeaderSession(tc, newSession, true)
	if err != nil {
		panic(err)
	}
}

func ensureTopicDisableWrite(nsqdCoord *NsqdCoordinator, topic string, partition int, disabled bool) {
	tc, err := nsqdCoord.getTopicCoord(topic, partition)
	if err != nil {
		panic(err)
	}
	tc.disableWrite = disabled
}

func ensureCatchupForTopic(nsqdCoord *NsqdCoordinator, topicInfo RpcAdminTopicInfo) {
	tc, err := nsqdCoord.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		panic(err)
	}
	tc.topicInfo.CatchupList = topicInfo.CatchupList
}

func TestNsqdCoordLookupdChanged(t *testing.T) {
	errInter := ErrLocalWriteFailed
	commonErr := reflect.ValueOf(errInter).Interface().(error)
	test.NotNil(t, commonErr)
	t.Log(commonErr.Error())
}

func newNsqdNode(t *testing.T, id string) (*nsqdNs.NSQD, int, *NsqdNodeInfo, string) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 1
	nsqd := mustStartNSQD(opts)
	randPort := rand.Int31n(60000-10000) + 10000
	nodeInfo := NsqdNodeInfo{
		NodeIp:  "127.0.0.1",
		TcpPort: "0",
		RpcPort: strconv.Itoa(int(randPort)),
	}
	nodeInfo.ID = GenNsqdNodeID(&nodeInfo, id)
	return nsqd, int(randPort), &nodeInfo, opts.DataPath
}

func TestNsqdCoordStartup(t *testing.T) {
	// first startup
	topic := "coordTestTopic"
	partition := 1

	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}
	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqd3, randPort3, nodeInfo3, data3 := newNsqdNode(t, "id3")
	nsqd4, randPort4, _, data4 := newNsqdNode(t, "id4")

	fakeLeadership := NewFakeNSQDLeadership().(*fakeNsqdLeadership)
	// start as leader
	fakeInfo := &TopicPartionMetaInfo{
		Name:        topic,
		Partition:   partition,
		Leader:      nodeInfo1.GetID(),
		ISR:         make([]string, 0),
		CatchupList: make([]string, 0),
		Epoch:       1,
		Replica:     3,
	}
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.CatchupList = append(fakeInfo.CatchupList, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartionMetaInfo)
	fakeLeadership.fakeTopicsInfo[topic] = tmp
	fakeLeadership.AcquireTopicLeader(topic, partition, *nodeInfo1)
	tmp[partition] = fakeInfo

	nsqd1.GetTopic(topic, partition)
	nsqd2.GetTopic(topic, partition)
	nsqd3.GetTopic(topic, partition)
	nsqd4.GetTopic(topic, partition)

	fakeLookupProxy, _ := NewFakeLookupRemoteProxy("127.0.0.1", 0)
	fakeSession, _ := fakeLeadership.GetTopicLeaderSession(topic, partition)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic] = make(map[int]*TopicLeaderSession)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic][partition] = fakeSession

	nsqdCoord1 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()

	// wait the node start and acquire leadership
	time.Sleep(time.Second)
	// leader startup
	test.Equal(t, true, fakeLeadership.IsNodeTopicLeader(topic, partition, nodeInfo1))
	tc, err := nsqdCoord1.getTopicCoord(topic, partition)
	test.Nil(t, err)
	test.Equal(t, tc.topicInfo, *fakeInfo)
	test.Equal(t, tc.topicLeaderSession, *fakeSession)
	test.Equal(t, tc.topicLeaderSession.IsSame(fakeSession), true)
	test.Equal(t, tc.GetLeaderID(), nodeInfo1.GetID())
	test.Equal(t, tc.GetLeaderSession(), fakeSession.Session)
	test.Equal(t, tc.GetLeaderEpoch(), fakeSession.LeaderEpoch)
	// start as isr
	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	time.Sleep(time.Second)

	tc, err = nsqdCoord2.getTopicCoord(topic, partition)
	test.Nil(t, err)
	test.Equal(t, tc.topicInfo, *fakeInfo)
	test.Equal(t, tc.topicLeaderSession.IsSame(fakeSession), true)
	test.Equal(t, tc.GetLeaderID(), nodeInfo1.GetID())
	test.Equal(t, tc.GetLeaderSession(), fakeSession.Session)
	test.Equal(t, tc.GetLeaderEpoch(), fakeSession.LeaderEpoch)

	// start as catchup
	nsqdCoord3 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	tc, err = nsqdCoord3.getTopicCoord(topic, partition)
	test.Nil(t, err)
	// wait catchup
	time.Sleep(time.Second * 3)
	tc, err = nsqdCoord3.getTopicCoord(topic, partition)
	test.Nil(t, err)
	test.Equal(t, tc.topicInfo, *fakeInfo)
	test.Equal(t, tc.topicLeaderSession, *fakeSession)
	test.Equal(t, tc.topicLeaderSession.IsSame(fakeSession), true)
	test.Equal(t, tc.GetLeaderID(), nodeInfo1.GetID())
	test.Equal(t, tc.GetLeaderSession(), fakeSession.Session)
	test.Equal(t, tc.GetLeaderEpoch(), fakeSession.LeaderEpoch)

	// start as not relevant
	nsqdCoord4 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort4)), data4, "id4", nsqd4, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data4)
	defer nsqd4.Exit()
	tc, err = nsqdCoord4.getTopicCoord(topic, partition)
	test.NotNil(t, err)
	// start as no remote topic
}

func TestNsqdCoordSyncToLeader(t *testing.T) {
}

func TestNsqdCoordNewISR(t *testing.T) {
}

func TestNsqdCoordLeaveFromISR(t *testing.T) {
}

func TestNsqdCoordNewCatchup(t *testing.T) {
}

func TestNsqdCoordPutMessageAndSyncChannelOffset(t *testing.T) {
	topic := "coordTestTopic"
	partition := 1

	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}

	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqdCoord1 := startNsqdCoord(t, strconv.Itoa(randPort1), data1, "id1", nsqd1)
	//defer nsqdCoord1.Stop()
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	time.Sleep(time.Second)

	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqdCoord2 := startNsqdCoord(t, strconv.Itoa(randPort2), data2, "id2", nsqd2)
	//defer nsqdCoord2.Stop()
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()

	var topicInitInfo RpcAdminTopicInfo
	topicInitInfo.Name = topic
	topicInitInfo.Partition = partition
	topicInitInfo.Epoch = 1
	topicInitInfo.ISR = append(topicInitInfo.ISR, nsqdCoord1.myNode.GetID())
	topicInitInfo.ISR = append(topicInitInfo.ISR, nsqdCoord2.myNode.GetID())
	topicInitInfo.Leader = nsqdCoord1.myNode.GetID()
	topicInitInfo.Replica = 2
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	var leaderSession RpcTopicLeaderSession
	leaderSession.LeaderNode = nodeInfo1
	leaderSession.TopicLeaderEpoch = 1
	leaderSession.TopicLeaderSession = "123"
	leaderSession.TopicName = topic
	leaderSession.TopicPartition = partition
	leaderSession.TopicEpoch = 1
	// normal test
	ensureTopicLeaderSession(nsqdCoord1, leaderSession)
	ensureTopicLeaderSession(nsqdCoord2, leaderSession)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
	topicData1 := nsqd1.GetTopicIgnPart(topic)
	err := nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.Nil(t, err)
	// message header is 26 bytes
	msgCnt := 1
	msgRawSize := int64(nsqdNs.MessageHeaderBytes() + 3 + 4)

	topicData2 := nsqd2.GetTopicIgnPart(topic)

	topicData1.ForceFlush()
	topicData2.ForceFlush()

	test.Equal(t, topicData1.TotalSize(), msgRawSize)
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	tc1, _ := nsqdCoord1.getTopicCoord(topic, partition)
	logs, err := tc1.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	logs[msgCnt-1].Epoch = 1

	test.Equal(t, topicData2.TotalSize(), msgRawSize)
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	tc2, _ := nsqdCoord2.getTopicCoord(topic, partition)
	logs, err = tc2.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	logs[msgCnt-1].Epoch = leaderSession.TopicLeaderEpoch

	test.Equal(t, tc1.IsMineLeaderSessionReady(nsqdCoord1.myNode.GetID()), true)
	test.Equal(t, tc2.IsMineLeaderSessionReady(nsqdCoord2.myNode.GetID()), false)
	// test write not leader
	err = nsqdCoord2.PutMessageToCluster(topicData2, []byte("123"))
	test.NotNil(t, err)
	t.Log(err)
	// test write disabled
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, true)
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	t.Log(err)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	// check isr not enough
	newISR := make([]string, 0)
	newISR = append(newISR, nsqdCoord1.myNode.GetID())
	oldISR := topicInitInfo.ISR
	topicInitInfo.ISR = newISR
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	t.Log(err)
	topicInitInfo.ISR = oldISR
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	// test write epoch mismatch
	leaderSession.TopicLeaderEpoch++
	ensureTopicLeaderSession(nsqdCoord2, leaderSession)

	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	// test leader epoch increase
	ensureTopicLeaderSession(nsqdCoord1, leaderSession)
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.Nil(t, err)
	msgCnt++
	topicData1.ForceFlush()
	topicData2.ForceFlush()
	test.Equal(t, topicData1.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	logs, err = tc1.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	logs[msgCnt-1].Epoch = leaderSession.TopicLeaderEpoch

	test.Equal(t, topicData2.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	logs, err = tc2.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	logs[msgCnt-1].Epoch = leaderSession.TopicLeaderEpoch

	channel1 := topicData1.GetChannel("ch1")
	channel2 := topicData2.GetChannel("ch1")
	test.Equal(t, channel1.Depth(), int64(msgCnt)*msgRawSize)
	test.Equal(t, channel2.Depth(), int64(msgCnt)*msgRawSize)
	msgConsumed := 0
	for i := msgConsumed; i < msgCnt; i++ {
		msg := <-channel1.GetClientMsgChan()
		channel1.StartInFlightTimeout(msg, 1, 10)
		err := nsqdCoord1.FinishMessageToCluster(channel1, 1, msg.ID)
		test.Nil(t, err)

		test.Equal(t, channel2.Depth(), msgRawSize*int64(msgCnt-i-1))
	}
	msgConsumed = msgCnt

	// test write session mismatch
	leaderSession.TopicLeaderSession = "1234new"
	ensureTopicLeaderSession(nsqdCoord1, leaderSession)
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	ensureTopicLeaderSession(nsqdCoord2, leaderSession)
	// test leader changed
	topicInitInfo.Leader = nsqdCoord2.myNode.GetID()
	leaderSession.LeaderNode = nodeInfo2
	leaderSession.TopicLeaderEpoch++
	leaderSession.TopicEpoch++
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord1, leaderSession)
	ensureTopicLeaderSession(nsqdCoord2, leaderSession)
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	err = nsqdCoord2.PutMessageToCluster(topicData2, []byte("123"))
	// leader switch will disable write by default
	test.NotNil(t, err)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
	test.Equal(t, tc1.IsMineLeaderSessionReady(nsqdCoord1.myNode.GetID()), false)
	test.Equal(t, tc2.IsMineLeaderSessionReady(nsqdCoord2.myNode.GetID()), true)
	for i := 0; i < 3; i++ {
		err = nsqdCoord2.PutMessageToCluster(topicData2, []byte("123"))
		test.Nil(t, err)
		msgCnt++
		topicData1.ForceFlush()
		topicData2.ForceFlush()
		test.Equal(t, topicData1.TotalSize(), msgRawSize*int64(msgCnt))
		test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
		logs, err = tc1.logMgr.GetCommitLogs(0, msgCnt)
		test.Nil(t, err)
		test.Equal(t, len(logs), msgCnt)
		logs[msgCnt-1].Epoch = leaderSession.TopicLeaderEpoch

		test.Equal(t, topicData2.TotalSize(), msgRawSize*int64(msgCnt))
		test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
		logs, err = tc2.logMgr.GetCommitLogs(0, msgCnt)
		test.Nil(t, err)
		test.Equal(t, len(logs), msgCnt)
		logs[msgCnt-1].Epoch = leaderSession.TopicLeaderEpoch
		t.Log(logs)
	}
	test.Equal(t, int64(channel1.GetChannelEnd()), int64(msgCnt)*msgRawSize)
	test.Equal(t, int64(channel2.GetChannelEnd()), int64(msgCnt)*msgRawSize)
	test.Equal(t, int64(channel1.GetConfirmedOffset()), int64(msgConsumed)*msgRawSize)
	test.Equal(t, int64(channel2.GetConfirmedOffset()), int64(msgConsumed)*msgRawSize)
	for i := msgConsumed; i < msgCnt; i++ {
		msg := <-channel2.GetClientMsgChan()
		channel2.StartInFlightTimeout(msg, 1, 10)
		err := nsqdCoord2.FinishMessageToCluster(channel2, 1, msg.ID)
		test.Nil(t, err)
		test.Equal(t, channel1.Depth(), msgRawSize*int64(msgCnt-i-1))
		test.Equal(t, channel2.Depth(), msgRawSize*int64(msgCnt-i-1))
	}
	msgConsumed = msgCnt
	// test retry write
}
