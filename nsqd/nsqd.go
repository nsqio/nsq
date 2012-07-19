package main

import (
	"../util"
	"log"
	"net"
	"os"
	"sync"
)

type NSQd struct {
	sync.RWMutex
	workerId        int64
	memQueueSize    int64
	dataPath        string
	maxBytesPerFile int64
	syncEvery       int64
	msgTimeout      int64
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

func NewNSQd(workerId int64) *NSQd {
	n := &NSQd{
		workerId:        workerId,
		memQueueSize:    10000,
		dataPath:        os.TempDir(),
		maxBytesPerFile: 104857600,
		syncEvery:       2500,
		msgTimeout:      60000,
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
		topic = NewTopic(topicName, n.memQueueSize, n.dataPath, n.maxBytesPerFile, n.syncEvery, n.msgTimeout)
		n.topicMap[topicName] = topic
		log.Printf("TOPIC(%s): created", topic.name)
	}

	return topic
}

func (n *NSQd) idPump() {
	var numErrors uint64
	for {
		id, err := NewGUID(n.workerId)
		if err != nil {
			numErrors++
			if numErrors > uint64(cap(n.idChan)) {
				log.Printf("ERROR: %s", err.Error())
			}
			continue
		}
		numErrors = 0
		select {
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			return
		}
	}
}
