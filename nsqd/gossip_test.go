package nsqd

import (
	"net"
	"sort"
	"sync"
	"testing"
	"time"
)

var gossipNotifyCh = make(chan struct{}, 20)
var (
	converging     = false
	convergingLock = &sync.Mutex{}
)

func TestGossip(t *testing.T) {
	var nsqds []*NSQD
	var seedNodeAddr *net.TCPAddr
	var seedNode *NSQD
	var tcpPorts []int

	gossipNotify = func() {
		convergingLock.Lock()
		defer convergingLock.Unlock()
		if converging {
			gossipNotifyCh <- struct{}{}
		}
	}

	num := 3
	for i := 0; i < num; i++ {
		var nsqd *NSQD

		// find an open port
		tmpl, err := net.Listen("tcp", "0.0.0.0:0")
		equal(t, err, nil)
		addr := tmpl.Addr().(*net.TCPAddr)
		tmpl.Close()

		opts := NewNSQDOptions()
		opts.ID = int64(i)
		opts.Logger = newTestLogger(t)
		opts.GossipAddress = addr.String()
		if seedNode != nil {
			opts.SeedNodeAddresses = []string{seedNodeAddr.String()}
		}
		tcpAddr, _, nsqd := mustStartNSQD(opts)
		defer nsqd.Exit()

		nsqds = append(nsqds, nsqd)
		tcpPorts = append(tcpPorts, tcpAddr.Port)

		if seedNode == nil {
			seedNode = nsqd
			seedNodeAddr = addr
		}
	}
	// sort the ports for later comparison
	sort.Ints(tcpPorts)

	// wait for convergence
	converge(5*time.Second, nsqds, func() bool {
		converged := true
		for _, nsqd := range nsqds {
			producers := nsqd.rdb.FindProducers("client", "", "")
			converged = converged && len(producers) == num
		}
		return converged
	})

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

	converge(10*time.Second, nsqds, func() bool {
		converged := true
		for _, nsqd := range nsqds {
			converged = len(nsqd.rdb.FindProducers("topic", topicName, "")) == 1 && converged
			converged = len(nsqd.rdb.FindProducers("channel", topicName, "ch")) == 1 && converged
		}
		return converged
	})

	for _, nsqd := range nsqds {
		producers := nsqd.rdb.FindProducers("topic", topicName, "")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)

		producers = nsqd.rdb.FindProducers("channel", topicName, "ch")
		equal(t, len(producers), 1)
		equal(t, producers[0].TCPPort, firstPort)
	}
}

func converge(timeout time.Duration, nsqds []*NSQD, convergence func() bool) {
	convergingLock.Lock()
	converging = true
	convergingLock.Unlock()

	// wait for convergence
	converged := false
	t := time.NewTimer(timeout)
	for !converged {
		select {
		case <-t.C:
			converged = true
		case <-gossipNotifyCh:
			converged = convergence()
		}
	}

	convergingLock.Lock()
	converging = false
	convergingLock.Unlock()
}
