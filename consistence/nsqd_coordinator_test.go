package consistence

import (
	"fmt"
	"github.com/absolute8511/glog"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	nsqdNs "github.com/absolute8511/nsq/nsqd"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
	"github.com/absolute8511/nsq/internal/ext"
)

type fakeConsumer struct {
	cid int64
}

func NewFakeConsumer(id int64) *fakeConsumer {
	return &fakeConsumer{cid: id}
}

func (c *fakeConsumer) UnPause() {
}
func (c *fakeConsumer) Pause() {
}
func (c *fakeConsumer) TimedOutMessage() {
}
func (c *fakeConsumer) RequeuedMessage() {
}
func (c *fakeConsumer) FinishedMessage() {
}
func (c *fakeConsumer) Stats() nsqdNs.ClientStats {
	return nsqdNs.ClientStats{}
}
func (c *fakeConsumer) Exit() {
}
func (c *fakeConsumer) Empty() {
}
func (c *fakeConsumer) String() string {
	return ""
}
func (c *fakeConsumer) GetID() int64 {
	return c.cid
}

type testClusterNodeInfo struct {
	id        string
	localNsqd *nsqdNs.NSQD
	randPort  int
	nodeInfo  *NsqdNodeInfo
	dataPath  string
	nsqdCoord *NsqdCoordinator
}

type fakeLookupRemoteProxy struct {
	leaderSessions map[string]map[int]*TopicLeaderSession
	fakeNsqdCoords map[string]*NsqdCoordinator
	lookupEpoch    EpochType
	t              *testing.T
	addr           string
}

func NewFakeLookupRemoteProxy(addr string, timeout time.Duration) (INsqlookupRemoteProxy, error) {
	return &fakeLookupRemoteProxy{
		leaderSessions: make(map[string]map[int]*TopicLeaderSession),
		fakeNsqdCoords: make(map[string]*NsqdCoordinator),
		addr:           addr,
	}, nil
}

func (self *fakeLookupRemoteProxy) RemoteAddr() string {
	return self.addr
}

func (self *fakeLookupRemoteProxy) Reconnect() error {
	return nil
}

func (self *fakeLookupRemoteProxy) Close() {
}

func (self *fakeLookupRemoteProxy) RequestNotifyNewTopicInfo(topic string, partition int, nid string) {
	if self.t != nil {
		self.t.Log("requesting notify topic info")
	}
	return
}

func (self *fakeLookupRemoteProxy) RequestCheckTopicConsistence(topic string, partition int) {
	if self.t != nil {
		self.t.Log("requesting checking topic")
	}
	return
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
	nsqdCoord.coordMutex.Lock()
	coords, ok := nsqdCoord.topicCoords[topicInfo.Name]
	if !ok {
		coords = make(map[int]*TopicCoordinator)
		nsqdCoord.topicCoords[topicInfo.Name] = coords
	}
	tpCoord, ok := coords[topicInfo.Partition]
	if !ok {
		path := GetTopicPartitionBasePath(nsqdCoord.dataRootPath, topicInfo.Name, topicInfo.Partition)
		tpCoord, _ = NewTopicCoordinator(topicInfo.Name, topicInfo.Partition, path, 0, topicInfo.OrderedMulti)
		coords[topicInfo.Partition] = tpCoord
	}
	nsqdCoord.coordMutex.Unlock()
	ensureTopicDisableWrite(nsqdCoord, topicInfo.Name, topicInfo.Partition, true)
	err := nsqdCoord.updateTopicInfo(tpCoord, false, &topicInfo.TopicPartitionMetaInfo)
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
	tc.DisableWrite(disabled)
	tcData := tc.GetData()
	if tcData.IsMineLeaderSessionReady(nsqdCoord.myNode.GetID()) {
		topicData, err := nsqdCoord.localNsqd.GetExistingTopic(tcData.topicInfo.Name, tcData.topicInfo.Partition)
		if err != nil {
			coordLog.Infof("no topic on local: %v, %v", tcData.topicInfo.GetTopicDesp(), err)
		} else {
			// leader changed (maybe down), we make sure out data is flushed to keep data safe
			topicData.ForceFlush()
			tcData.logMgr.FlushCommitLogs()
			topicData.EnableForMaster()
		}
	}
}

func ensureCatchupForTopic(nsqdCoord *NsqdCoordinator, topicInfo RpcAdminTopicInfo) {
	tc, err := nsqdCoord.getTopicCoord(topicInfo.Name, topicInfo.Partition)
	if err != nil {
		panic(err)
	}
	tc.topicInfo.CatchupList = topicInfo.CatchupList
}

func newNsqdNode(t *testing.T, id string) (*nsqdNs.NSQD, int, *NsqdNodeInfo, string) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	if testing.Verbose() {
		opts.Logger = &levellogger.SimpleLogger{}
		opts.LogLevel = levellogger.LOG_INFO
		glog.StartWorker(time.Second)
	} else {
		opts.LogLevel = levellogger.LOG_INFO
	}
	if t == nil {
		opts.Logger = nil
	}
	nsqd := mustStartNSQD(opts)
	randPort := rand.Int31n(10000) + 20000
	nodeInfo := NsqdNodeInfo{
		NodeIP:  "127.0.0.1",
		TcpPort: "0",
		RpcPort: strconv.Itoa(int(randPort)),
	}
	nodeInfo.ID = GenNsqdNodeID(&nodeInfo, id)
	return nsqd, int(randPort), &nodeInfo, opts.DataPath
}

func TestNsqdCoordErr(t *testing.T) {
	test.Equal(t, ErrCommitLogLessThanSegmentStart.Error(), ErrTopicCommitLogLessThanSegmentStart.ErrMsg)
}

func TestNsqdCoordStartup(t *testing.T) {
	// first startup
	topic := "coordTestTopic"
	partition := 1

	if testing.Verbose() {
		SetCoordLogger(&levellogger.SimpleLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}
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
		Leader:        nodeInfo1.GetID(),
		ISR:           make([]string, 0),
		CatchupList:   make([]string, 0),
		Epoch:         1,
		EpochForWrite: 1,
	}
	fakeInfo := &TopicPartitionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.CatchupList = append(fakeInfo.CatchupList, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartitionMetaInfo)
	fakeLeadership.UpdateTopics(topic, tmp)
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
	defer nsqdCoord1.Stop()

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
	defer nsqdCoord2.Stop()
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
	defer nsqdCoord3.Stop()
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
	defer nsqdCoord4.Stop()
	tc, err = nsqdCoord4.getTopicCoord(topic, partition)
	test.NotNil(t, err)
	// start as no remote topic
}

func TestNsqdCoordLeaveFromISR(t *testing.T) {
	topic := "coordTestTopic"
	partition := 1
	if testing.Verbose() {
		SetCoordLogger(&levellogger.SimpleLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqd3, randPort3, nodeInfo3, data3 := newNsqdNode(t, "id3")

	fakeLeadership := NewFakeNSQDLeadership().(*fakeNsqdLeadership)
	meta := TopicMetaInfo{
		Replica:      3,
		PartitionNum: 1,
	}
	fakeReplicaInfo := &TopicPartitionReplicaInfo{
		Leader:        nodeInfo1.GetID(),
		ISR:           make([]string, 0),
		CatchupList:   make([]string, 0),
		Epoch:         1,
		EpochForWrite: 1,
	}
	fakeInfo := &TopicPartitionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}

	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartitionMetaInfo)
	fakeLeadership.UpdateTopics(topic, tmp)
	fakeLeadership.AcquireTopicLeader(topic, partition, nodeInfo1, fakeInfo.Epoch)
	tmp[partition] = fakeInfo

	fakeLookupProxy, _ := NewFakeLookupRemoteProxy("127.0.0.1", 0)
	fakeSession, _ := fakeLeadership.GetTopicLeaderSession(topic, partition)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic] = make(map[int]*TopicLeaderSession)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic][partition] = fakeSession

	nsqdCoord1 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	defer nsqdCoord1.Stop()
	time.Sleep(time.Second)

	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	defer nsqdCoord2.Stop()
	time.Sleep(time.Second)

	nsqdCoord3 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	defer nsqdCoord3.Stop()
	time.Sleep(time.Second)

	// create topic on nsqdcoord
	var topicInitInfo RpcAdminTopicInfo
	topicInitInfo.TopicPartitionMetaInfo = *fakeInfo
	topicInitInfo.EpochForWrite++
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
	if testing.Verbose() {
		SetCoordLogger(&levellogger.SimpleLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqd3, randPort3, nodeInfo3, data3 := newNsqdNode(t, "id3")

	fakeLeadership := NewFakeNSQDLeadership().(*fakeNsqdLeadership)
	meta := TopicMetaInfo{
		Replica:      3,
		PartitionNum: 1,
	}
	fakeReplicaInfo := &TopicPartitionReplicaInfo{
		Leader:        nodeInfo1.GetID(),
		ISR:           make([]string, 0),
		CatchupList:   make([]string, 0),
		Epoch:         1,
		EpochForWrite: 1,
	}
	fakeInfo := &TopicPartitionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}

	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.CatchupList = append(fakeInfo.CatchupList, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartitionMetaInfo)
	fakeLeadership.UpdateTopics(topic, tmp)
	fakeLeadership.AcquireTopicLeader(topic, partition, nodeInfo1, fakeInfo.Epoch)
	tmp[partition] = fakeInfo

	fakeLookupProxy, _ := NewFakeLookupRemoteProxy("127.0.0.1", 0)
	fakeSession, _ := fakeLeadership.GetTopicLeaderSession(topic, partition)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic] = make(map[int]*TopicLeaderSession)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic][partition] = fakeSession

	nsqdCoord1 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	defer nsqdCoord1.Stop()
	time.Sleep(time.Second)

	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	defer nsqdCoord2.Stop()
	time.Sleep(time.Second)

	// create topic on nsqdcoord
	var topicInitInfo RpcAdminTopicInfo
	topicInitInfo.TopicPartitionMetaInfo = *fakeInfo
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
		_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
		test.Nil(t, err)
		msgCnt++
	}
	topicData2 := nsqd2.GetTopic(topic, partition)
	topicData1.ForceFlush()
	topicData2.ForceFlush()

	test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	tc1, _ := nsqdCoord1.getTopicCoord(topic, partition)
	logs1, err := tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs1), msgCnt)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	tc2, _ := nsqdCoord2.getTopicCoord(topic, partition)
	logs2, err := tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	t.Log(logs2)
	test.Nil(t, err)
	test.Equal(t, len(logs2), msgCnt)
	test.Equal(t, logs1, logs2)

	// start as catchup
	// 3 kinds of catchup
	// 1. fall behind, 2. exact same, 3. data more than leader(need rollback)
	nsqdCoord3 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	defer nsqdCoord3.Stop()
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	// wait catchup
	time.Sleep(time.Second * 3)
	topicData3 := nsqd3.GetTopic(topic, partition)
	topicData3.ForceFlush()
	tc3, coordErr := nsqdCoord3.getTopicCoord(topic, partition)
	test.Nil(t, coordErr)
	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err := tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)
	// catchup again with exact same logs
	topicInitInfo.Epoch++
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	time.Sleep(time.Second * 3)
	topicData3.ForceFlush()
	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
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
	changedInfo.EpochForWrite++
	fakeSession.LeaderNode = nodeInfo3
	fakeSession.Session = fakeSession.Session + fakeSession.Session
	fakeSession.LeaderEpoch++
	ensureTopicOnNsqdCoord(nsqdCoord3, changedInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	ensureTopicDisableWrite(nsqdCoord3, topic, partition, false)
	_, _, _, _, err = nsqdCoord3.PutMessageBodyToCluster(topicData3, []byte("123"), 0)
	test.Nil(t, err)
	_, _, _, _, err = nsqdCoord3.PutMessageBodyToCluster(topicData3, []byte("123"), 0)
	test.Nil(t, err)

	// test catchup again with more logs than leader
	fakeSession.LeaderNode = nodeInfo1
	//fakeSession.Session = fakeSession.Session
	fakeSession.LeaderEpoch++
	topicInitInfo.Epoch = changedInfo.Epoch + 1
	topicInitInfo.EpochForWrite = changedInfo.EpochForWrite + 1
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	time.Sleep(time.Second * 3)
	topicData3.ForceFlush()

	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)

	// move from catchup to isr
	topicInitInfo.ISR = append(topicInitInfo.ISR, nodeInfo3.GetID())
	topicInitInfo.CatchupList = make([]string, 0)
	topicInitInfo.DisableWrite = true
	topicInitInfo.Epoch++
	topicInitInfo.EpochForWrite++

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
		_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
		test.Nil(t, err)
		msgCnt++
	}

	topicData1.ForceFlush()
	topicData2.ForceFlush()
	topicData3.ForceFlush()

	test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	logs1, err = tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs1), msgCnt)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	logs2, err = tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs2), msgCnt)
	test.Equal(t, logs1, logs2)

	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	t.Log(logs3)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)
}

// catchup from empty
// catchup with more data than leader
// catchup with same segment
// catchup with less segment
// catchup with more segment
func TestNsqdCoordCatchupMultiCommitSegment(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()
	topic := "coordTestTopic"
	partition := 1
	if testing.Verbose() {
		SetCoordLogger(&levellogger.SimpleLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

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
	fakeInfo := &TopicPartitionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}

	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.CatchupList = append(fakeInfo.CatchupList, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartitionMetaInfo)
	fakeLeadership.UpdateTopics(topic, tmp)
	fakeLeadership.AcquireTopicLeader(topic, partition, nodeInfo1, fakeInfo.Epoch)
	tmp[partition] = fakeInfo

	fakeLookupProxy, _ := NewFakeLookupRemoteProxy("127.0.0.1", 0)
	fakeSession, _ := fakeLeadership.GetTopicLeaderSession(topic, partition)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic] = make(map[int]*TopicLeaderSession)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic][partition] = fakeSession

	nsqdCoord1 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	defer nsqdCoord1.Stop()
	time.Sleep(time.Second)

	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	defer nsqdCoord2.Stop()
	time.Sleep(time.Second)

	// create topic on nsqdcoord
	var topicInitInfo RpcAdminTopicInfo
	topicInitInfo.TopicPartitionMetaInfo = *fakeInfo
	topicInitInfo.EpochForWrite = 1
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
	for i := 0; i < 30; i++ {
		_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
		test.Nil(t, err)
		msgCnt++
	}
	topicData2 := nsqd2.GetTopic(topic, partition)
	topicData1.ForceFlush()
	topicData2.ForceFlush()

	test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	tc1, _ := nsqdCoord1.getTopicCoord(topic, partition)
	logs1, err := tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs1), msgCnt)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	tc2, _ := nsqdCoord2.getTopicCoord(topic, partition)
	logs2, err := tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
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
	defer nsqdCoord3.Stop()
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	// wait catchup
	time.Sleep(time.Second * 3)
	topicData3 := nsqd3.GetTopic(topic, partition)
	topicData3.ForceFlush()
	tc3, coordErr := nsqdCoord3.getTopicCoord(topic, partition)
	test.Nil(t, coordErr)
	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err := tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)
	// catchup again with exact same logs
	topicInitInfo.Epoch++
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	time.Sleep(time.Second * 3)
	topicData3.ForceFlush()
	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
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
	changedInfo.EpochForWrite++
	fakeSession.LeaderNode = nodeInfo3
	fakeSession.Session = fakeSession.Session + fakeSession.Session
	fakeSession.LeaderEpoch++
	ensureTopicOnNsqdCoord(nsqdCoord3, changedInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	ensureTopicDisableWrite(nsqdCoord3, topic, partition, false)
	for i := 0; i < 20; i++ {
		_, _, _, _, err = nsqdCoord3.PutMessageBodyToCluster(topicData3, []byte("123"), 0)
		test.Nil(t, err)
	}

	// test catchup again with more logs than leader
	fakeSession.LeaderNode = nodeInfo1
	//fakeSession.Session = fakeSession.Session
	fakeSession.LeaderEpoch++
	topicInitInfo.Epoch = changedInfo.Epoch + 1
	topicInitInfo.EpochForWrite = changedInfo.EpochForWrite + 1
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	time.Sleep(time.Second * 3)
	topicData3.ForceFlush()

	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, len(logs1), msgCnt)
	test.Equal(t, logs1, logs3)

	// move from catchup to isr
	topicInitInfo.ISR = append(topicInitInfo.ISR, nodeInfo3.GetID())
	topicInitInfo.CatchupList = make([]string, 0)
	topicInitInfo.DisableWrite = true
	topicInitInfo.Epoch++
	topicInitInfo.EpochForWrite++

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
		_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
		test.Nil(t, err)
		msgCnt++
	}

	topicData1.ForceFlush()
	topicData2.ForceFlush()
	topicData3.ForceFlush()

	test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	logs1, err = tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs1), msgCnt)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	logs2, err = tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs2), msgCnt)
	test.Equal(t, logs1, logs2)

	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	t.Log(logs3)

	test.Nil(t, err)
	test.Equal(t, len(logs3), msgCnt)
	test.Equal(t, logs1, logs3)
}

func TestNsqdCoordCatchupCleanOldData(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()
	topic := "coordTestTopic"
	partition := 1
	if testing.Verbose() {
		SetCoordLogger(&levellogger.SimpleLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

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
	fakeInfo := &TopicPartitionMetaInfo{
		Name:                      topic,
		Partition:                 partition,
		TopicMetaInfo:             meta,
		TopicPartitionReplicaInfo: *fakeReplicaInfo,
	}

	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo1.GetID())
	fakeInfo.ISR = append(fakeInfo.ISR, nodeInfo2.GetID())
	fakeInfo.CatchupList = append(fakeInfo.CatchupList, nodeInfo3.GetID())

	tmp := make(map[int]*TopicPartitionMetaInfo)
	fakeLeadership.UpdateTopics(topic, tmp)
	fakeLeadership.AcquireTopicLeader(topic, partition, nodeInfo1, fakeInfo.Epoch)
	tmp[partition] = fakeInfo

	fakeLookupProxy, _ := NewFakeLookupRemoteProxy("127.0.0.1", 0)
	fakeSession, _ := fakeLeadership.GetTopicLeaderSession(topic, partition)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic] = make(map[int]*TopicLeaderSession)
	fakeLookupProxy.(*fakeLookupRemoteProxy).leaderSessions[topic][partition] = fakeSession

	nsqdCoord1 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort1)), data1, "id1", nsqd1, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	defer nsqdCoord1.Stop()
	time.Sleep(time.Second)

	nsqdCoord2 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort2)), data2, "id2", nsqd2, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data2)
	defer nsqd2.Exit()
	defer nsqdCoord2.Stop()
	time.Sleep(time.Second)

	// create topic on nsqdcoord
	var topicInitInfo RpcAdminTopicInfo
	topicInitInfo.TopicPartitionMetaInfo = *fakeInfo
	topicInitInfo.EpochForWrite = 1
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	// notify leadership to nsqdcoord
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, fakeSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, fakeSession)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)

	// message header is 26 bytes
	msgCnt := 0
	totalMsgNum := 50
	msgRawSize := int64(nsqdNs.MessageHeaderBytes() + 3 + 4)
	topicData1 := nsqd1.GetTopic(topic, partition)
	ch := topicData1.GetChannel("ch")
	for i := 0; i < totalMsgNum; i++ {
		_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
		test.Nil(t, err)
		msgCnt++
	}
	topicData2 := nsqd2.GetTopic(topic, partition)
	topicData1.ForceFlush()
	topicData2.ForceFlush()

	test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	tc1, _ := nsqdCoord1.getTopicCoord(topic, partition)
	logs1, err := tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs1), msgCnt)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	tc2, _ := nsqdCoord2.getTopicCoord(topic, partition)
	logs2, err := tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs2), msgCnt)
	test.Equal(t, logs1, logs2)
	t.Log(logs2)
	for i := 0; i < totalMsgNum-1; i++ {
		select {
		case msg := <- ch.GetClientMsgChan():
			ch.StartInFlightTimeout(msg, NewFakeConsumer(1), "", time.Second)
			err := nsqdCoord1.FinishMessageToCluster(ch, 1, "", msg.ID)
			test.Nil(t, err)
		case <-time.After(time.Second):
			t.Fatalf("failed to consume message")
		}
	}
	tc1.logMgr.CleanOldData(2, int64(GetLogDataSize()))
	newStart, _, err := tc1.logMgr.GetLogStartInfo()
	test.Nil(t, err)
	logs1, _ = tc1.logMgr.GetCommitLogsV2(newStart.SegmentStartIndex, newStart.SegmentStartOffset, msgCnt)
	test.Equal(t, msgCnt-int(newStart.SegmentStartCount), len(logs1))

	// start as catchup
	nsqdCoord3 := startNsqdCoordWithFakeData(t, strconv.Itoa(int(randPort3)), data3, "id3", nsqd3, fakeLeadership, fakeLookupProxy.(*fakeLookupRemoteProxy))
	defer os.RemoveAll(data3)
	defer nsqd3.Exit()
	defer nsqdCoord3.Stop()
	ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord3, topic, partition, fakeSession)
	// wait catchup
	time.Sleep(time.Second * 3)
	topicData3 := nsqd3.GetTopic(topic, partition)
	topicData3.ForceFlush()
	tc3, coordErr := nsqdCoord3.getTopicCoord(topic, partition)
	test.Nil(t, coordErr)
	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	newStart3, _, _ := tc3.logMgr.GetLogStartInfo()
	logs3, err := tc3.logMgr.GetCommitLogsV2(newStart3.SegmentStartIndex, newStart3.SegmentStartOffset, len(logs1))
	test.Nil(t, err)
	test.Equal(t, len(logs3), len(logs1))
	test.Equal(t, logs1, logs3)

	// move from catchup to isr
	topicInitInfo.ISR = append(topicInitInfo.ISR, nodeInfo3.GetID())
	topicInitInfo.CatchupList = make([]string, 0)
	topicInitInfo.DisableWrite = true
	topicInitInfo.Epoch++
	topicInitInfo.EpochForWrite++

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
		_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
		test.Nil(t, err)
		msgCnt++
	}

	topicData1.ForceFlush()
	topicData2.ForceFlush()
	topicData3.ForceFlush()

	test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	logs1, err = tc1.logMgr.GetCommitLogsV2(newStart.SegmentStartIndex, newStart.SegmentStartOffset, msgCnt-int(newStart.SegmentStartCount))
	test.Nil(t, err)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	logs2, err = tc2.logMgr.GetCommitLogsV2(newStart.SegmentStartIndex, newStart.SegmentStartOffset, len(logs1))
	test.Nil(t, err)
	test.Equal(t, len(logs2), len(logs1))
	test.Equal(t, logs1, logs2)

	test.Equal(t, topicData3.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData3.TotalMessageCnt(), uint64(msgCnt))
	logs3, err = tc3.logMgr.GetCommitLogsV2(newStart3.SegmentStartIndex, newStart3.SegmentStartOffset, len(logs1))
	test.Nil(t, err)
	test.Equal(t, len(logs3), len(logs1))
	test.Equal(t, logs1, logs3)

	t.Log(logs3)

}

func TestNsqdCoordPutMessageAndSyncChannelOffset(t *testing.T) {
	topic := "coordTestTopic"
	partition := 1

	if testing.Verbose() {
		SetCoordLogger(&levellogger.SimpleLogger{}, levellogger.LOG_DETAIL)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(newTestLogger(t), levellogger.LOG_DEBUG)
	}

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
	topicInitInfo.EpochForWrite = 1
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
	channel1 := topicData1.GetChannel("ch1")
	_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
	test.Nil(t, err)
	// message header is 26 bytes
	msgCnt := 1
	msgRawSize := int64(nsqdNs.MessageHeaderBytes() + 3 + 4)

	topicData2 := nsqd2.GetTopic(topic, partition)

	topicData1.ForceFlush()
	topicData2.ForceFlush()

	test.Equal(t, topicData1.TotalDataSize(), msgRawSize)
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	tc1, _ := nsqdCoord1.getTopicCoord(topic, partition)
	logs, err := tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	test.Equal(t, logs[msgCnt-1].Epoch, topicInitInfo.EpochForWrite)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize)
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	tc2, _ := nsqdCoord2.getTopicCoord(topic, partition)
	logs, err = tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	test.Equal(t, logs[msgCnt-1].Epoch, topicInitInfo.EpochForWrite)

	test.Equal(t, tc1.IsMineLeaderSessionReady(nsqdCoord1.myNode.GetID()), true)
	test.Equal(t, tc2.IsMineLeaderSessionReady(nsqdCoord2.myNode.GetID()), false)
	coordLog.Infof("==== test write not leader ====")
	// test write not leader
	_, _, _, _, err = nsqdCoord2.PutMessageBodyToCluster(topicData2, []byte("123"), 0)
	test.NotNil(t, err)
	t.Log(err)
	// test write disabled
	coordLog.Infof("==== test write disabled ====")
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, true)
	_, _, _, _, err = nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
	test.NotNil(t, err)
	t.Log(err)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	// check isr not enough
	newISR := make([]string, 0)
	newISR = append(newISR, nsqdCoord1.myNode.GetID())
	oldISR := topicInitInfo.ISR
	topicInitInfo.ISR = newISR
	topicInitInfo.Epoch++
	topicInitInfo.EpochForWrite++
	coordLog.Infof("==== test write while isr not enough ====")
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	_, _, _, _, err = nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
	test.NotNil(t, err)
	t.Log(err)
	topicInitInfo.ISR = oldISR
	topicInitInfo.Epoch++
	topicInitInfo.EpochForWrite++
	coordLog.Infof("==== test write with mismatch epoch ====")
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	// test write epoch mismatch
	leaderSession.LeaderEpoch++
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)

	waitDone := make(chan int)
	go func() {
		time.Sleep(time.Second)
		tc, _ := nsqdCoord1.getTopicCoord(topic, partition)
		tc.SetForceLeave(true)
		time.Sleep(time.Second)
		tc.SetForceLeave(false)
		close(waitDone)
	}()
	_, _, _, _, err = nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
	test.NotNil(t, err)
	<-waitDone
	// leader failed previously, so the leader is invalid
	// re-confirm the leader
	topicInitInfo.Epoch++
	topicInitInfo.EpochForWrite++
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
	time.Sleep(time.Second)
	coordLog.Infof("==== test write success ====")
	_, _, _, _, err = nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
	test.Nil(t, err)

	msgCnt++
	topicData1.ForceFlush()
	topicData2.ForceFlush()
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(topicData2.GetCommitted().TotalMsgCnt()))
	test.Equal(t, topicData2.TotalDataSize(), int64(topicData2.GetCommitted().Offset()))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(topicData1.GetCommitted().TotalMsgCnt()))
	test.Equal(t, topicData1.TotalDataSize(), int64(topicData1.GetCommitted().Offset()))
	test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
	logs, err = tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	test.Equal(t, logs[msgCnt-1].Epoch, topicInitInfo.EpochForWrite)

	test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
	test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
	logs, err = tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
	test.Nil(t, err)
	test.Equal(t, len(logs), msgCnt)
	test.Equal(t, logs[msgCnt-1].Epoch, topicInitInfo.EpochForWrite)

	channel2 := topicData2.GetChannel("ch1")
	// since the new channel on node2 is init to end , we need sync node1 channel offset to node2
	nsqdCoord1.SetChannelConsumeOffsetToCluster(channel1, int64(channel1.GetConfirmed().Offset()), channel1.GetConfirmed().TotalMsgCnt(), true)
	t.Logf("topic end: %v, channel %v, %v", topicData2.GetCommitted(), channel2.GetChannelEnd(), channel2.GetConfirmed())
	time.Sleep(time.Second)
	test.Equal(t, channel1.Depth(), int64(msgCnt))
	test.Equal(t, channel1.DepthSize(), int64(msgCnt)*msgRawSize)
	test.Equal(t, channel2.Depth(), int64(msgCnt))
	test.Equal(t, channel2.DepthSize(), int64(msgCnt)*msgRawSize)
	msgConsumed := 0
	time.Sleep(time.Second)
	coordLog.Infof("==== test client consume messages ====")
	for i := msgConsumed; i < msgCnt; i++ {
		msg := <- channel1.GetClientMsgChan()
		channel1.StartInFlightTimeout(msg, NewFakeConsumer(1), "", time.Second)
		err := nsqdCoord1.FinishMessageToCluster(channel1, 1, "", msg.ID)
		test.Nil(t, err)

		test.Equal(t, channel1.DepthSize(), int64(msgCnt-i-1)*msgRawSize)
		test.Equal(t, channel1.Depth(), int64(msgCnt-i-1))
		time.Sleep(time.Millisecond * 10)
		test.Equal(t, channel2.DepthSize(), msgRawSize*int64(msgCnt-i-1))
		test.Equal(t, channel2.Depth(), int64(msgCnt-i-1))
	}
	msgConsumed = msgCnt

	// test write session mismatch
	coordLog.Infof("==== test write with session mismatch ====")
	leaderSession.Session = "1234new"
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)

	waitDone = make(chan int)
	go func() {
		time.Sleep(time.Second)
		tc, _ := nsqdCoord1.getTopicCoord(topic, partition)
		tc.SetForceLeave(true)
		time.Sleep(time.Second)
		tc.SetForceLeave(false)
		close(waitDone)
	}()

	// session mismatch is not the test case
	//_, _, _, _, err = nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
	//test.NotNil(t, err)
	<-waitDone

	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)
	// test leader changed
	topicInitInfo.Leader = nsqdCoord2.myNode.GetID()
	topicInitInfo.Epoch++
	topicInitInfo.EpochForWrite++
	leaderSession.LeaderNode = nodeInfo2
	leaderSession.LeaderEpoch++
	ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
	ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
	ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)
	ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)

	coordLog.Infof("==== test write while leader changed ====")
	_, _, _, _, err = nsqdCoord1.PutMessageBodyToCluster(topicData1, []byte("123"), 0)
	test.NotNil(t, err)
	_, _, _, _, err = nsqdCoord2.PutMessageBodyToCluster(topicData2, []byte("123"), 0)
	// leader switch will disable write by default
	test.NotNil(t, err)
	ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
	ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
	test.Equal(t, tc1.IsMineLeaderSessionReady(nsqdCoord1.myNode.GetID()), false)
	test.Equal(t, tc2.IsMineLeaderSessionReady(nsqdCoord2.myNode.GetID()), true)
	coordLog.Infof("==== test write success ====")
	for i := 0; i < 3; i++ {
		_, _, _, _, err = nsqdCoord2.PutMessageBodyToCluster(topicData2, []byte("123"), 0)
		test.Nil(t, err)
		msgCnt++
		topicData1.ForceFlush()
		topicData2.ForceFlush()
		test.Equal(t, topicData1.TotalDataSize(), msgRawSize*int64(msgCnt))
		test.Equal(t, topicData1.TotalMessageCnt(), uint64(msgCnt))
		logs, err = tc1.logMgr.GetCommitLogsV2(0, 0, msgCnt)
		test.Nil(t, err)
		test.Equal(t, len(logs), msgCnt)
		test.Equal(t, logs[msgCnt-1].Epoch, topicInitInfo.EpochForWrite)

		test.Equal(t, topicData2.TotalDataSize(), msgRawSize*int64(msgCnt))
		test.Equal(t, topicData2.TotalMessageCnt(), uint64(msgCnt))
		logs, err = tc2.logMgr.GetCommitLogsV2(0, 0, msgCnt)
		test.Nil(t, err)
		test.Equal(t, len(logs), msgCnt)
		test.Equal(t, logs[msgCnt-1].Epoch, topicInitInfo.EpochForWrite)
		t.Log(logs)
	}
	topicData2.ForceFlush()
	time.Sleep(time.Second)
	test.Equal(t, int64(channel1.GetChannelEnd().Offset()), int64(msgCnt)*msgRawSize)
	test.Equal(t, int64(channel2.GetChannelEnd().Offset()), int64(msgCnt)*msgRawSize)
	test.Equal(t, int64(channel1.GetConfirmed().Offset()), int64(msgConsumed)*msgRawSize)
	test.Equal(t, int64(channel2.GetConfirmed().Offset()), int64(msgConsumed)*msgRawSize)
	test.Equal(t, channel2.Depth(), int64(msgCnt-msgConsumed))
	test.Equal(t, channel2.DepthSize(), msgRawSize*int64(msgCnt-msgConsumed))
	test.Equal(t, channel1.Depth(), int64(msgCnt-msgConsumed))
	test.Equal(t, channel1.DepthSize(), msgRawSize*int64(msgCnt-msgConsumed))
	channel2.SetTrace(true)
	channel1.SetTrace(true)
	topicData1.SetTrace(true)
	topicData2.SetTrace(true)
	coordLog.Infof("==== test client consume ====")
	for i := msgConsumed; i < msgCnt; i++ {
		msg := <- channel2.GetClientMsgChan()
		channel2.StartInFlightTimeout(msg, NewFakeConsumer(1), "", time.Second)
		err := nsqdCoord2.FinishMessageToCluster(channel2, 1, "", msg.ID)
		test.Nil(t, err)
		time.Sleep(time.Millisecond)
		test.Equal(t, channel2.Depth(), int64(msgCnt-i-1))
		test.Equal(t, channel2.DepthSize(), msgRawSize*int64(msgCnt-i-1))
		test.Equal(t, channel1.Depth(), int64(msgCnt-i-1))
		test.Equal(t, channel1.DepthSize(), msgRawSize*int64(msgCnt-i-1))
	}
	//msgConsumed = msgCnt
	// TODO: test retry write
}

func TestNsqdCoordLeaderChangeWhileWrite(t *testing.T) {
	// TODO: old leader write and part of the isr got the write,
	// then leader failed, choose new leader from isr
	// RESULT: all new isr nodes should be synced
}

func TestNsqdCoordISRChangedWhileWrite(t *testing.T) {
	// TODO: leader write while network split, and part of isr agreed with write
	// leader need rollback (or just move leader out from isr and sync with new leader )
	// RESULT: all new isr nodes should be synced
}

func benchmarkNsqdCoordPubWithArg(b *testing.B, replica int, size int) {
	b.StopTimer()
	topicBase := "coordBenchTestTopic"
	partition := 1

	if testing.Verbose() {
		SetCoordLogger(&levellogger.SimpleLogger{}, levellogger.LOG_WARN)
		glog.SetFlags(0, "", "", true, true, 1)
		glog.StartWorker(time.Second)
	} else {
		SetCoordLogger(nil, levellogger.LOG_WARN)
	}

	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(nil, "id1")
	coordLog.Warningf("data1: %v", data1)
	defer os.RemoveAll(data1)
	defer nsqd1.Exit()
	nsqdCoord1 := startNsqdCoord(nil, strconv.Itoa(randPort1), data1, "id1", nsqd1, true)
	nsqdCoord1.enableBenchCost = true
	nsqdCoord1.Start()
	defer nsqdCoord1.Stop()
	time.Sleep(time.Second)

	var nsqdCoord2 *NsqdCoordinator
	var nsqdCoord3 *NsqdCoordinator
	if replica >= 2 {
		nsqd2, randPort2, _, data2 := newNsqdNode(nil, "id2")

		defer os.RemoveAll(data2)
		defer nsqd2.Exit()
		nsqdCoord2 = startNsqdCoord(nil, strconv.Itoa(randPort2), data2, "id2", nsqd2, true)
		nsqdCoord2.enableBenchCost = true
		nsqdCoord2.Start()
		defer nsqdCoord2.Stop()
	}
	if replica >= 3 {
		nsqd3, randPort3, _, data3 := newNsqdNode(nil, "id3")
		defer os.RemoveAll(data3)
		defer nsqd3.Exit()
		nsqdCoord3 = startNsqdCoord(nil, strconv.Itoa(randPort3), data3, "id3", nsqd3, true)
		nsqdCoord3.enableBenchCost = true
		nsqdCoord3.Start()
		defer nsqdCoord3.Stop()

	}

	topicDataList := make([]*nsqdNs.Topic, 0)
	for tnum := 0; tnum < 20; tnum++ {
		var topicInitInfo RpcAdminTopicInfo
		topicInitInfo.Name = topicBase + "-" + strconv.Itoa(tnum)
		topic := topicInitInfo.Name
		topicInitInfo.Partition = partition
		topicInitInfo.Epoch = 1
		topicInitInfo.EpochForWrite = 1
		topicInitInfo.ISR = append(topicInitInfo.ISR, nsqdCoord1.myNode.GetID())
		if replica >= 2 {
			topicInitInfo.ISR = append(topicInitInfo.ISR, nsqdCoord2.myNode.GetID())
		}
		if replica >= 3 {
			topicInitInfo.ISR = append(topicInitInfo.ISR, nsqdCoord3.myNode.GetID())
		}
		topicInitInfo.Leader = nsqdCoord1.myNode.GetID()
		topicInitInfo.Replica = replica
		ensureTopicOnNsqdCoord(nsqdCoord1, topicInitInfo)
		if replica >= 2 {
			ensureTopicOnNsqdCoord(nsqdCoord2, topicInitInfo)
		}
		if replica >= 3 {
			ensureTopicOnNsqdCoord(nsqdCoord3, topicInitInfo)
		}
		leaderSession := &TopicLeaderSession{
			LeaderNode:  nodeInfo1,
			LeaderEpoch: 1,
			Session:     "fake123",
		}
		// normal test
		ensureTopicLeaderSession(nsqdCoord1, topic, partition, leaderSession)
		ensureTopicDisableWrite(nsqdCoord1, topic, partition, false)
		if replica >= 2 {
			ensureTopicLeaderSession(nsqdCoord2, topic, partition, leaderSession)
			ensureTopicDisableWrite(nsqdCoord2, topic, partition, false)
		}
		if replica >= 3 {
			ensureTopicLeaderSession(nsqdCoord3, topic, partition, leaderSession)
			ensureTopicDisableWrite(nsqdCoord3, topic, partition, false)
		}
		msg := make([]byte, size)
		// check if write ok
		topicData := nsqd1.GetTopic(topic, partition)
		_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(topicData, msg, 0)
		if err != nil {
			b.Logf("test put failed: %v", err)
			b.Fail()
		} else {
			topicDataList = append(topicDataList, topicData)
		}
	}
	msg := make([]byte, size)
	//b.SetParallelism(1)
	var wg sync.WaitGroup
	b.SetBytes(int64(len(msg)))
	b.StartTimer()

	num := b.N / len(topicDataList)

	for tnum := 0; tnum < len(topicDataList); tnum++ {
		wg.Add(1)
		topicData := topicDataList[tnum]
		go func(localTopic *nsqdNs.Topic) {
			defer wg.Done()
			for i := 0; i < num; i++ {
				_, _, _, _, err := nsqdCoord1.PutMessageBodyToCluster(localTopic, msg, 0)
				if err != nil {
					b.Errorf("put error: %v", err)
				}
			}
		}(topicData)
	}
	wg.Wait()
	b.StopTimer()
	b.Log(nsqdCoord1.rpcServer.rpcServer.Stats.Snapshot())
}

func BenchmarkNsqdCoordPub1Replicator128(b *testing.B) {
	benchmarkNsqdCoordPubWithArg(b, 1, 128)
}

func BenchmarkNsqdCoordPub2Replicator128(b *testing.B) {
	benchmarkNsqdCoordPubWithArg(b, 2, 128)
}

func BenchmarkNsqdCoordPub3Replicator128(b *testing.B) {
	benchmarkNsqdCoordPubWithArg(b, 3, 128)
}

func BenchmarkNsqdCoordPub1Replicator1024(b *testing.B) {
	benchmarkNsqdCoordPubWithArg(b, 1, 1024)
}

func BenchmarkNsqdCoordPub2Replicator1024(b *testing.B) {
	benchmarkNsqdCoordPubWithArg(b, 2, 1024)
}

func BenchmarkNsqdCoordPub3Replicator1024(b *testing.B) {
	benchmarkNsqdCoordPubWithArg(b, 3, 1024)
}
