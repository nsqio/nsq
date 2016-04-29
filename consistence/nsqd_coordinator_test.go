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
	lookupEpoch    EpochType
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

func (self *fakeLookupRemoteProxy) ReadyForTopicISR(topic string, partition int, nid string, leaderSession *TopicLeaderSession, joinISRSession string) *CoordErr {
	if self.t != nil {
		self.t.Log("requesting ready for isr")
	}
	localSession, ok := self.leaderSessions[topic]
	if !ok {
		return ErrMissingTopicLeaderSession
	}

	//if leaderSession == nil {
	//	// need push the current leadership to the node
	//	req := RpcTopicLeaderSession{}
	//	req.LeaderNode = localSession[partition].LeaderNode
	//	req.LookupdEpoch = self.lookupEpoch
	//	req.TopicName = topic
	//	req.TopicPartition = partition
	//	req.WaitReady = false
	//	req.TopicEpoch = localSession[partition].LeaderEpoch
	//	req.TopicLeaderEpoch = req.TopicEpoch
	//	req.TopicLeaderSession = localSession[partition].Session
	//	ret := true
	//	self.fakeNsqdCoords[nid].rpcServer.NotifyTopicLeaderSession(req, &ret)
	//	return nil
	//}
	if s, ok := localSession[partition]; ok {
		if s.IsSame(leaderSession) {
			return nil
		}
	}
	return ErrLeaderSessionMismatch
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
		tpCoord, _ = NewTopicCoordinator(topicInfo.Name, topicInfo.Partition, path)
		coords[topicInfo.Partition] = tpCoord
	}
	ensureTopicDisableWrite(nsqdCoord, topicInfo.Name, topicInfo.Partition, true)
	err := nsqdCoord.updateTopicInfo(tpCoord, false, &topicInfo.TopicPartionMetaInfo)
	if err != nil {
		panic(err)
	}
	return tpCoord
}

func ensureTopicLeaderSession(nsqdCoord *NsqdCoordinator, topic string, partition int, newSession *TopicLeaderSession) {
	tc, err := nsqdCoord.getTopicCoord(topic, partition)
	if err != nil {
		panic(err)
	}
	err = nsqdCoord.updateTopicLeaderSession(tc, newSession, "")
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
	opts.LogLevel = 2
	nsqd := mustStartNSQD(opts)
	randPort := rand.Int31n(30000) + 20000
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
	meta := TopicMetaInfo{
		Replica:      3,
		PartitionNum: 1,
	}
	fakeReplicaInfo := &TopicPartitionReplicaInfo{
		Leader:      nodeInfo1.GetID(),
		ISR:         make([]string, 0),
		CatchupList: make([]string, 0),
		Epoch:       1,
	}
	fakeInfo := &TopicPartionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.CatchupList = append(fakeInfo.CatchupList, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartionMetaInfo)
	fakeLeadership.fakeTopicsInfo[topic] = tmp
	fakeLeadership.AcquireTopicLeader(topic, partition, nodeInfo1, fakeInfo.Epoch)
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
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, fakeSession)
	test.Equal(t, true, fakeLeadership.IsNodeTopicLeader(topic, partition, nodeInfo1))
	tc, err := nsqdCoord1.getTopicCoord(topic, partition)
	test.Nil(t, err)
	test.Equal(t, tc.topicInfo, *fakeInfo)
	test.Equal(t, tc.topicLeaderSession, *fakeSession)
	test.Equal(t, tc.topicLeaderSession.IsSame(fakeSession), true)
	test.Equal(t, tc.GetLeaderSessionID(), nodeInfo1.GetID())
	test.Equal(t, tc.GetLeaderSession(), fakeSession.Session)
	test.Equal(t, tc.GetLeaderSessionEpoch(), fakeSession.LeaderEpoch)
	// start as isr
	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	time.Sleep(time.Second)

	ensureTopicLeaderSession(nsqdCoord2, topic, partition, fakeSession)
	tc, err = nsqdCoord2.getTopicCoord(topic, partition)
	test.Nil(t, err)
	test.Equal(t, tc.topicInfo, *fakeInfo)
	test.Equal(t, tc.topicLeaderSession.IsSame(fakeSession), true)
	test.Equal(t, tc.GetLeaderSessionID(), nodeInfo1.GetID())
	test.Equal(t, tc.GetLeaderSession(), fakeSession.Session)
	test.Equal(t, tc.GetLeaderSessionEpoch(), fakeSession.LeaderEpoch)

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
	test.Equal(t, tc.GetLeaderSessionID(), nodeInfo1.GetID())
	test.Equal(t, tc.GetLeaderSession(), fakeSession.Session)
	test.Equal(t, tc.GetLeaderSessionEpoch(), fakeSession.LeaderEpoch)

	// start as not relevant
	nsqdCoord4 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort4)), data4, "id4", nsqd4, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data4)
	defer nsqd4.Exit()
	tc, err = nsqdCoord4.getTopicCoord(topic, partition)
	test.NotNil(t, err)
	// start as no remote topic
}

func TestNsqdCoordLeaveFromISR(t *testing.T) {
	topic := "coordTestTopic"
	partition := 1

	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}
	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqd3, randPort3, nodeInfo3, data3 := newNsqdNode(t, "id3")

	fakeLeadership := NewFakeNSQDLeadership().(*fakeNsqdLeadership)
	meta := TopicMetaInfo{
		Replica:      3,
		PartitionNum: 1,
	}
	fakeReplicaInfo := &TopicPartitionReplicaInfo{
		Leader:      nodeInfo1.GetID(),
		ISR:         make([]string, 0),
		CatchupList: make([]string, 0),
		Epoch:       1,
	}
	fakeInfo := &TopicPartionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}

	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartionMetaInfo)
	fakeLeadership.fakeTopicsInfo[topic] = tmp
	fakeLeadership.AcquireTopicLeader(topic, partition, nodeInfo1, fakeInfo.Epoch)
	tmp[partition] = fakeInfo

	fakeLookupProxy, _ := NewFakeLookupRemoteProxy("127.0.0.1", 0)
	fakeSession, _ := fakeLeadership.GetTopicLeaderSession(topic, partition)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic] = make(map[int]*TopicLeaderSession)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic][partition] = fakeSession

	nsqdCoord1 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	time.Sleep(time.Second)

	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	time.Sleep(time.Second)

	nsqdCoord3 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	time.Sleep(time.Second)

	// create topic on nsqdcoord
	var topicInitInfo RpcAdminTopicInfo
	topicInitInfo.TopicPartionMetaInfo = *fakeInfo
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	// notify leadership to nsqdcoord
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, fakeSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, fakeSession)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	// TODO: leave as isr

	// leave as leader
}

func TestNsqdCoordCatchup(t *testing.T) {
	topic := "coordTestTopic"
	partition := 1

	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}
	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqd3, randPort3, nodeInfo3, data3 := newNsqdNode(t, "id3")

	fakeLeadership := NewFakeNSQDLeadership().(*fakeNsqdLeadership)
	meta := TopicMetaInfo{
		Replica:      3,
		PartitionNum: 1,
	}
	fakeReplicaInfo := &TopicPartitionReplicaInfo{
		Leader:      nodeInfo1.GetID(),
		ISR:         make([]string, 0),
		CatchupList: make([]string, 0),
		Epoch:       1,
	}
	fakeInfo := &TopicPartionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}

	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.CatchupList = append(fakeInfo.CatchupList, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartionMetaInfo)
	fakeLeadership.fakeTopicsInfo[topic] = tmp
	fakeLeadership.AcquireTopicLeader(topic, partition, nodeInfo1, fakeInfo.Epoch)
	tmp[partition] = fakeInfo

	fakeLookupProxy, _ := NewFakeLookupRemoteProxy("127.0.0.1", 0)
	fakeSession, _ := fakeLeadership.GetTopicLeaderSession(topic, partition)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic] = make(map[int]*TopicLeaderSession)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic][partition] = fakeSession

	nsqdCoord1 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	time.Sleep(time.Second)

	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	time.Sleep(time.Second)

	// create topic on nsqdcoord
	var topicInitInfo RpcAdminTopicInfo
	topicInitInfo.TopicPartionMetaInfo = *fakeInfo
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	// notify leadership to nsqdcoord
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, fakeSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, fakeSession)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)

	// message header is 26 bytes
	msgCnt := 0
	msgRawSize := int64(nsqdNs.MessageHeaderBytes() + 3 + 4)
	topicData1 := nsqd1.GetTopic(topic, partition)
	for i := 0; i < 20; i++ {
		err := nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
		test.Nil(t, err)
		msgCnt++
	}
	topicData2 := nsqd2.GetTopic(topic, partition)
	topicData1.ForceFlush()
	topicData2.ForceFlush()

	test.Equal(t, topicData1.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	tc1, _ := nsqdCoord1.getTopicCoord(topic, partition)
	logs1, err := tc1.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs1), msgCnt)

	test.Equal(t, topicData2.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	tc2, _ := nsqdCoord2.getTopicCoord(topic, partition)
	logs2, err := tc2.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs2), msgCnt)
	test.Equal(t, logs1, logs2)
	t.Log(logs2)

	// start as catchup
	// 3 kinds of catchup
	// 1. fall behind, 2. exact same, 3. data more than leader(need rollback)
	nsqdCoord3 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	// wait catchup
	time.Sleep(time.Second * 3)
	topicData3 := nsqd3.GetTopic(topic, partition)
	topicData3.ForceFlush()
	tc3, err := nsqdCoord3.getTopicCoord(topic, partition)
	test.Nil(t, err)
	test.Equal(t, topicData3.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err := tc3.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)
	// catchup again with exact same logs
	topicInitInfo.Epoch++
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	time.Sleep(time.Second * 3)
	topicData3.ForceFlush()
	test.Equal(t, topicData3.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)
	// change leader and make nsqd3 write more than old leader
	changedInfo := topicInitInfo
	changedInfo.Leader = nsqdCoord3.myNode.GetID()
	changedInfo.ISR = make([]string, 0)
	changedInfo.ISR = append(changedInfo.ISR, nodeInfo3.GetID())
	changedInfo.Replica = 1
	changedInfo.CatchupList = make([]string, 0)
	changedInfo.Epoch++
	fakeSession.LeaderNode = nodeInfo3
	fakeSession.Session = fakeSession.Session + fakeSession.Session
	fakeSession.LeaderEpoch++
	ensureTopicOnNsqdCoord(nsqdCoord3, changedInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	ensureTopicDisableWrite(nsqdCoord3, topic, partition, false)
	err = nsqdCoord3.PutMessageToCluster(topicData3, []byte("123"))
	test.Nil(t, err)
	err = nsqdCoord3.PutMessageToCluster(topicData3, []byte("123"))
	test.Nil(t, err)

	// test catchup again with more logs than leader
	fakeSession.LeaderNode = nodeInfo1
	fakeSession.Session = fakeSession.Session
	fakeSession.LeaderEpoch++
	topicInitInfo.Epoch = changedInfo.Epoch + 1
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	time.Sleep(time.Second * 3)
	topicData3.ForceFlush()

	test.Equal(t, topicData3.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)

	// move from catchup to isr
	topicInitInfo.ISR = append(topicInitInfo.ISR, nodeInfo3.GetID())
	topicInitInfo.CatchupList = make([]string, 0)
	topicInitInfo.DisableWrite = true
	topicInitInfo.Epoch++

	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, fakeSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, fakeSession)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)

	ensureTopicDisableWrite(nsqdCoord1, topic, partition, true)
	time.Sleep(time.Second * 3)

	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord3, topic, partition, false)
	for i := 0; i < 3; i++ {
		err := nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
		test.Nil(t, err)
		msgCnt++
	}

	topicData1.ForceFlush()
	topicData2.ForceFlush()
	topicData3.ForceFlush()

	test.Equal(t, topicData1.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	logs1, err = tc1.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs1), msgCnt)

	test.Equal(t, topicData2.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	logs2, err = tc2.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs2), msgCnt)
	test.Equal(t, logs1, logs2)

	test.Equal(t, topicData3.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)

	t.Log(logs3)
}

func TestNsqdCoordPutMessageAndSyncChannelOffset(t *testing.T) {
	topic := "coordTestTopic"
	partition := 1

	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}

	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	nsqdCoord1 := startNsqdCoord(t, strconv.Itoa(randPort1), data1, "id1", nsqd1, true)
	nsqdCoord1.Start()
	defer nsqdCoord1.Stop()
	time.Sleep(time.Second)

	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	nsqdCoord2 := startNsqdCoord(t, strconv.Itoa(randPort2), data2, "id2", nsqd2, true)
	nsqdCoord2.Start()
	defer nsqdCoord2.Stop()

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
	leaderSession := &TopicLeaderSession{
		LeaderNode:  nodeInfo1,
		LeaderEpoch: 1,
		Session:     "fake123",
	}
	// normal test
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
	topicData1 := nsqd1.GetTopic(topic, partition)
	err := nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.Nil(t, err)
	// message header is 26 bytes
	msgCnt := 1
	msgRawSize := int64(nsqdNs.MessageHeaderBytes() + 3 + 4)

	topicData2 := nsqd2.GetTopic(topic, partition)

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
	logs[msgCnt-1].Epoch = leaderSession.LeaderEpoch

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
	topicInitInfo.Epoch++
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	t.Log(err)
	topicInitInfo.ISR = oldISR
	topicInitInfo.Epoch++
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	// test write epoch mismatch
	leaderSession.LeaderEpoch++
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)

	waitDone := make(chan int)
	go func() {
		time.Sleep(time.Second)
		tc, _ := nsqdCoord1.getTopicCoord(topic, partition)
		tc.forceLeave = true
		time.Sleep(time.Second)
		tc.forceLeave = false
		close(waitDone)
	}()
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	<-waitDone
	// leader failed previously, so the leader is invalid
	// re-confirm the leader
	topicInitInfo.Epoch++
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
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
	logs[msgCnt-1].Epoch = leaderSession.LeaderEpoch

	test.Equal(t, topicData2.TotalSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	logs, err = tc2.logMgr.GetCommitLogs(0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	logs[msgCnt-1].Epoch = leaderSession.LeaderEpoch

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

		test.Equal(t, channel1.Depth(), msgRawSize*int64(msgCnt-i-1))
		test.Equal(t, channel2.Depth(), msgRawSize*int64(msgCnt-i-1))
	}
	msgConsumed = msgCnt

	// test write session mismatch
	leaderSession.Session = "1234new"
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)

	waitDone = make(chan int)
	go func() {
		time.Sleep(time.Second)
		tc, _ := nsqdCoord1.getTopicCoord(topic, partition)
		tc.forceLeave = true
		time.Sleep(time.Second)
		tc.forceLeave = false
		close(waitDone)
	}()

	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	<-waitDone

	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)
	// test leader changed
	topicInitInfo.Leader = nsqdCoord2.myNode.GetID()
	topicInitInfo.Epoch++
	leaderSession.LeaderNode = nodeInfo2
	leaderSession.LeaderEpoch++
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)
	err = nsqdCoord1.PutMessageToCluster(topicData1, []byte("123"))
	test.NotNil(t, err)
	err = nsqdCoord2.PutMessageToCluster(topicData2, []byte("123"))
	// leader switch will disable write by default
	test.NotNil(t, err)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
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
		logs[msgCnt-1].Epoch = leaderSession.LeaderEpoch

		test.Equal(t, topicData2.TotalSize(), msgRawSize*int64(msgCnt))
		test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
		logs, err = tc2.logMgr.GetCommitLogs(0, msgCnt)
		test.Nil(t, err)
		test.Equal(t, len(logs), msgCnt)
		logs[msgCnt-1].Epoch = leaderSession.LeaderEpoch
		t.Log(logs)
	}
	topicData2.ForceFlush()
	time.Sleep(time.Second)
	test.Equal(t, int64(channel1.GetChannelEnd()), int64(msgCnt)*msgRawSize)
	test.Equal(t, int64(channel2.GetChannelEnd()), int64(msgCnt)*msgRawSize)
	test.Equal(t, int64(channel1.GetConfirmedOffset()), int64(msgConsumed)*msgRawSize)
	test.Equal(t, int64(channel2.GetConfirmedOffset()), int64(msgConsumed)*msgRawSize)
	test.Equal(t, channel2.Depth(), msgRawSize*int64(msgCnt-msgConsumed))
	test.Equal(t, channel1.Depth(), msgRawSize*int64(msgCnt-msgConsumed))
	channel2.EnableTrace = true
	channel1.EnableTrace = true
	topicData1.EnableTrace = true
	topicData2.EnableTrace = true
	for i := msgConsumed; i < msgCnt; i++ {
		msg := <-channel2.GetClientMsgChan()
		channel2.StartInFlightTimeout(msg, 1, 10)
		err := nsqdCoord2.FinishMessageToCluster(channel2, 1, msg.ID)
		test.Nil(t, err)
		time.Sleep(time.Millisecond)
		test.Equal(t, channel2.Depth(), msgRawSize*int64(msgCnt-i-1))
		test.Equal(t, channel1.Depth(), msgRawSize*int64(msgCnt-i-1))
	}
	msgConsumed = msgCnt
	// TODO: test retry write
}

func TestNsqdCoordLeaderChangeWhileWrite(t *testing.T) {
	// old leader write and part of the isr got the write,
	// then leader failed, choose new leader from isr
	// RESULT: all new isr nodes should be synced
}

func TestNsqdCoordISRChangedWhileWrite(t *testing.T) {
	// leader write while network split, and part of isr agreed with write
	// leader need rollback (or just move leader out from isr and sync with new leader )
	// RESULT: all new isr nodes should be synced
}
