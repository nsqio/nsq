package consistence

import (
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func startNsqLookupCoord(t *testing.T, id string) (*NsqLookupCoordinator, int, *NsqLookupdNodeInfo) {
	var n NsqLookupdNodeInfo
	n.ID = id
	n.NodeIp = "127.0.0.1"
	randPort := rand.Int31n(60000-10000) + 20000
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

func returnErrTestFunc() *CoordErr {
	var ret CoordErr
	return convertRpcError(nil, &ret)
}

func callErrTestFunc() error {
	return returnErrTestFunc()
}

func TestErrTest(t *testing.T) {
	err := callErrTestFunc()
	test.Nil(t, err)
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
	nsqd1, randPort1, nodeInfo1, data1 := newNsqdNode(t, "id1")
	nsqd2, randPort2, nodeInfo2, data2 := newNsqdNode(t, "id2")
	nsqd3, randPort3, nodeInfo3, data3 := newNsqdNode(t, "id3")
	nsqd4, randPort4, nodeInfo4, data4 := newNsqdNode(t, "id4")

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

	topic := "test-nsqlookup-topic"
	lookupCoord1, _, lookupNode1 := startNsqLookupCoord(t, "test-nsqlookup1")

	nsqdCoord1.lookupLeader = lookupNode1
	nsqdCoord2.lookupLeader = lookupNode1
	nsqdCoord3.lookupLeader = lookupNode1
	nsqdCoord4.lookupLeader = lookupNode1

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
	nsqdCoord1.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoord2.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoord3.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	nsqdCoord4.lookupRemoteCreateFunc = NewNsqLookupRpcClient
	// test new topic create
	err := lookupCoord1.CreateTopic(topic, 2, 2)
	test.Nil(t, err)
	time.Sleep(time.Second * 5)

	// test new node Add, new isr, new catchup

	// test node lost, isr lost, leader lost

	// test nsqd leadership watch

	// test elect while new leader failed

	// test join isr timeout
	// test new leader confirm timeout
}

func TestNsqLookupNsqdMigrate(t *testing.T) {
}
