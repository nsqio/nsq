package nsqd

import (
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"
)

func TestGossip(t *testing.T) {
	var nsqds []*NSQD
	var seedNodeAddr string
	var seedNode *NSQD
	var tcpPorts []int

	gossipNotifyCh := make(chan struct{})
	gossipNotify = func() {
		gossipNotifyCh <- struct{}{}
	}

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}

	num := 3
	for i := 0; i < num; i++ {
		var nsqd *NSQD

		// find an open port
		tmpl, err := net.Listen("tcp", hostname+":0")
		equal(t, err, nil)
		addr := tmpl.Addr().(*net.TCPAddr)
		tmpl.Close()

		opts := NewNSQDOptions()
		opts.ID = int64(i)
		//opts.Logger = newTestLogger(t)
		opts.GossipAddress = addr.String()
		if seedNode != nil {
			sn := net.JoinHostPort(seedNodeAddr, fmt.Sprint(seedNode.serf.Memberlist().LocalNode().Port))
			opts.SeedNodeAddresses = []string{sn}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		defer func(n *NSQD) {
			go nsqd.Exit()
		}(nsqd)

		nsqds = append(nsqds, nsqd)
		tcpPorts = append(tcpPorts, tcpAddr.Port)

		if seedNode == nil {
			seedNode = nsqd
			seedNodeAddr = addr.IP.String()
		}
	}
	// sort the ports for later comparison
	sort.Ints(tcpPorts)

	// wait for convergence
	converged := false
	timeout := time.NewTimer(5 * time.Second)
	for !converged {
		select {
		case <-timeout.C:
			converged = true
		case <-gossipNotifyCh:
			converged = true
			for _, nsqd := range nsqds {
				producers := nsqd.rdb.FindProducers("client", "", "")
				converged = converged && len(producers) == num
			}
		}
	}

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
