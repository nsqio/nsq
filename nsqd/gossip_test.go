package nsqd

import (
	"net"
	"sort"
	"testing"
	"time"
)

func TestGossip(t *testing.T) {
	var nsqds []*NSQD
	var seedNode *NSQD
	var tcpPorts []int

	num := 3
	for i := 0; i < num; i++ {
		// find an open port
		tmpl, err := net.Listen("tcp", "127.0.0.1:0")
		equal(t, err, nil)
		addr := tmpl.Addr().(*net.TCPAddr)
		tmpl.Close()

		opts := NewNSQDOptions()
		opts.ID = int64(i)
		opts.Logger = newTestLogger(t)
		opts.GossipAddress = addr.String()
		if seedNode != nil {
			opts.SeedNodeAddresses = []string{seedNode.opts.GossipAddress}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		defer nsqd.Exit()

		nsqds = append(nsqds, nsqd)
		tcpPorts = append(tcpPorts, tcpAddr.Port)

		if seedNode == nil {
			seedNode = nsqd
		}
	}
	// sort the ports for later comparison
	sort.Ints(tcpPorts)

	time.Sleep(250 * time.Millisecond)

	// all nodes in the cluster should have registrations
	for _, nsqd := range nsqds {
		producers := nsqd.rdb.FindProducers("client", "", "")
		var actTCPPorts []int
		for _, producer := range producers {
			actTCPPorts = append(actTCPPorts, producer.TCPPort)
		}
		sort.Ints(actTCPPorts)

		equal(t, len(producers), num)
		equal(t, tcpPorts, actTCPPorts)
	}

	// create a topic/channel on the first node
	topicName := "topic1"
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	firstPort := nsqds[0].tcpListener.Addr().(*net.TCPAddr).Port

	time.Sleep(250 * time.Millisecond)

	for _, nsqd := range nsqds {
		producers := nsqd.rdb.FindProducers("topic", topicName, "")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)

		producers = nsqd.rdb.FindProducers("channel", topicName, "ch")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)
	}
}
