package consistence

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func startNsqLookupCoord(t *testing.T, id string) (*NsqLookupCoordinator, int, *NsqLookupdNodeInfo) {
	var n NsqLookupdNodeInfo
	n.ID = id
	n.NodeIp = "127.0.0.1"
	randPort := rand.Int31n(60000-10000) + 10000
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
	coord1, _, node1 := startNsqLookupCoord(t, "test-nsqlookup1")
	fakeLeadership1 := coord1.leadership.(*FakeNsqlookupLeadership)
	fakeLeadership1.changeLookupLeader(node1)
	time.Sleep(time.Second)
	fakeNsqdNode1 := &NsqdNodeInfo{}
	fakeNsqdNode2 := &NsqdNodeInfo{}
	fakeNsqdNode3 := &NsqdNodeInfo{}
	fakeNsqdNode4 := &NsqdNodeInfo{}
	fakeLeadership1.addFakedNsqdNode(*fakeNsqdNode1)
	fakeLeadership1.addFakedNsqdNode(*fakeNsqdNode2)
	fakeLeadership1.addFakedNsqdNode(*fakeNsqdNode3)
	fakeLeadership1.addFakedNsqdNode(*fakeNsqdNode4)
	// test new topic create

	// test new node Add, new isr, new catchup

	// test node lost, isr lost, leader lost

	// test nsqd leadership watch

	// test elect while new leader failed

	// test join isr timeout
	// test new leader confirm timeout
}

func TestNsqLookupNsqdMigrate(t *testing.T) {
}
