package main

import (
	"../util"
	"log"
	"net"
	"sync"
)

type NSQd struct {
	sync.RWMutex
	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	lookupAddrs  util.StringArray
	memQueueSize int
	dataPath     string
	topicMap     map[string]*Topic
	tcpListener  net.Listener
	httpListener net.Listener
}

var nsqd *NSQd

func NewNSQd(tcpAddr *net.TCPAddr, httpAddr *net.TCPAddr, lookupAddrs util.StringArray, memQueueSize int, dataPath string) *NSQd {
	return &NSQd{
		tcpAddr:      tcpAddr,
		httpAddr:     httpAddr,
		lookupAddrs:  lookupAddrs,
		memQueueSize: memQueueSize,
		dataPath:     dataPath,
		topicMap:     make(map[string]*Topic),
	}
}

func (n *NSQd) Main() {
	go LookupRouter(n.lookupAddrs)

	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr.String(), err.Error())
	}
	n.tcpListener = tcpListener
	go util.TcpServer(tcpListener, tcpClientHandler)

	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr.String(), err.Error())
	}
	n.httpListener = httpListener
	go HttpServer(httpListener)
}

func (n *NSQd) Exit() {
	n.tcpListener.Close()
	n.httpListener.Close()

	for _, topic := range n.topicMap {
		topic.Close()
	}
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQd) GetTopic(topicName string) *Topic {
	n.Lock()
	defer n.Unlock()

	topic, ok := n.topicMap[topicName]
	if !ok {
		topic = NewTopic(topicName, n.memQueueSize, n.dataPath)
		n.topicMap[topicName] = topic
		log.Printf("TOPIC(%s): created", topic.name)
	}

	return topic
}
