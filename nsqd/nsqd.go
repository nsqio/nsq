package main

import (
	"../util"
	"log"
	"net"
	"sync"
)

type NSQd struct {
	sync.RWMutex
	workerId        int64
	memQueueSize    int64
	dataPath        string
	maxBytesPerFile int64
	tcpAddr         *net.TCPAddr
	httpAddr        *net.TCPAddr
	lookupAddrs     util.StringArray
	topicMap        map[string]*Topic
	tcpListener     net.Listener
	httpListener    net.Listener
	idChan          chan []byte
	exitChan        chan int
}

var nsqd *NSQd

func NewNSQd(workerId int64, tcpAddr, httpAddr *net.TCPAddr, lookupAddrs util.StringArray, memQueueSize int64, dataPath string, maxBytesPerFile int64) *NSQd {
	n := &NSQd{
		workerId:        workerId,
		memQueueSize:    memQueueSize,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		tcpAddr:         tcpAddr,
		httpAddr:        httpAddr,
		lookupAddrs:     lookupAddrs,
		topicMap:        make(map[string]*Topic),
		idChan:          make(chan []byte, 4096),
		exitChan:        make(chan int),
	}
	go n.idPump()
	return n
}

func (n *NSQd) Main() {
	go LookupRouter(n.lookupAddrs, n.exitChan)

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

	close(n.exitChan)

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
		topic = NewTopic(topicName, n.memQueueSize, n.dataPath, n.maxBytesPerFile)
		n.topicMap[topicName] = topic
		log.Printf("TOPIC(%s): created", topic.name)
	}

	return topic
}

func (n *NSQd) idPump() {
	for {
		id, err := NewGUID(n.workerId)
		if err != nil {
			log.Printf("ERROR: %s", err.Error())
			continue
		}
		select {
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			return
		}
	}
}
