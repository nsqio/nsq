package consistence

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	nsqdNs "github.com/absolute8511/nsq/nsqd"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"
)

type fakeLookupRemoteProxy struct {
	leaderSession *TopicLeaderSession
	t             *testing.T
}

func NewFakeLookupRemoteProxy(addr string, timeout time.Duration) (INsqlookupRemoteProxy, error) {
	return &fakeLookupRemoteProxy{}, nil
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

func (self *fakeLookupRemoteProxy) ReadyForTopicISR(topic string, partition int, nid string, leaderSession *TopicLeaderSession) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting ready for isr")
	}
	return nil
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
	if self.leaderSession.IsSame(leaderSession) {
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

func TestNsqdCoordStartup(t *testing.T) {
	// first startup

	// start as leader
	// start as isr
	// start as catchup
	// start as no remote topic
	// start as not relevant
	// start as not relevant but request join isr
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
	opts1 := nsqdNs.NewOptions()
	opts1.Logger = newTestLogger(t)
	opts1.LogLevel = 1
	nsqd1 := mustStartNSQD(opts1)
	nsqdCoord1 := startNsqdCoord(t, "11111", opts1.DataPath, "id1", nsqd1)
	//defer nsqdCoord1.Stop()
	defer os.RemoveAll(opts1.DataPath)
	defer nsqd1.Exit()
	nodeInfo1 := NsqdNodeInfo{
		NodeIp:  "127.0.0.1",
		TcpPort: "0",
		RpcPort: "11111",
	}
	nodeInfo1.ID = GenNsqdNodeID(&nodeInfo1, "id1")

	time.Sleep(time.Second)
	opts2 := nsqdNs.NewOptions()
	opts2.Logger = newTestLogger(t)
	opts2.LogLevel = 1
	nsqd2 := mustStartNSQD(opts2)
	nsqdCoord2 := startNsqdCoord(t, "11112", opts2.DataPath, "id2", nsqd2)
	//defer nsqdCoord2.Stop()
	defer os.RemoveAll(opts2.DataPath)
	defer nsqd2.Exit()
	nodeInfo2 := NsqdNodeInfo{
		NodeIp:  "127.0.0.1",
		TcpPort: "0",
		RpcPort: "11112",
	}
	nodeInfo2.ID = GenNsqdNodeID(&nodeInfo2, "id2")

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
	leaderSession.LeaderNode = &nodeInfo1
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
	leaderSession.LeaderNode = &nodeInfo2
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
