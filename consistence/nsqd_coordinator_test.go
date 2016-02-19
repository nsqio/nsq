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
	err = nsqdCoord.updateTopicLeaderSession(tc, newSession)
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

func TestNsqdCoordSyncToLeader(t *testing.T) {
}

func TestNsqdCoordJoinISR(t *testing.T) {
}

func TestNsqdCoordLeaveFromISR(t *testing.T) {
}

func TestNsqdCoordTopicInfoChanged(t *testing.T) {
}

func TestNsqdCoordChangeTopicLeader(t *testing.T) {
}

func TestNsqdCoordPutMessage(t *testing.T) {
	topic := "coordTestTopic"
	partition := 0

	coordLog.level = 2
	coordLog.logger = &levellogger.GLogger{}
	opts := nsqdNs.NewOptions()
	nsqd1 := mustStartNSQD(opts)
	nsqdCoord1 := startNsqdCoord("11111", opts.DataPath, "id1", nsqd1)
	//defer nsqdCoord1.Stop()
	defer nsqd1.Exit()
	dir1 := opts.DataPath
	defer os.RemoveAll(dir1)
	nodeInfo1 := NsqdNodeInfo{
		NodeIp:  "127.0.0.1",
		TcpPort: "0",
		RpcPort: "11111",
	}
	nodeInfo1.ID = GenNsqdNodeID(&nodeInfo1, "id1")

	time.Sleep(time.Second)
	opts.DataPath = ""
	nsqd2 := mustStartNSQD(opts)
	nsqdCoord2 := startNsqdCoord("11112", opts.DataPath, "id2", nsqd2)
	//defer nsqdCoord2.Stop()
	defer nsqd2.Exit()
	dir2 := opts.DataPath
	defer os.RemoveAll(dir2)
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
	topicData := nsqd1.GetTopicIgnPart(topic)
	err := nsqdCoord1.PutMessageToCluster(topicData, []byte("123"))
	test.Nil(t, err)

	// test write not leader

	// test write epoch less

	// test write session mismatch

	// test write disabled
}

func TestNsqdCoordSyncChannelOffset(t *testing.T) {
}
