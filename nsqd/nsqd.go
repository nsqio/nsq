package main

import (
	"../nsq"
	"../util"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"time"
)

var protocols = map[int32]nsq.Protocol{}

type NSQd struct {
	sync.RWMutex
	workerId        int64
	memQueueSize    int64
	dataPath        string
	maxBytesPerFile int64
	syncEvery       int64
	msgTimeout      time.Duration
	tcpAddr         *net.TCPAddr
	httpAddr        *net.TCPAddr
	lookupAddrs     util.StringArray
	topicMap        map[string]*Topic
	tcpListener     net.Listener
	httpListener    net.Listener
	idChan          chan []byte
	exitChan        chan int
	exitSyncChan    chan int
}

var nsqd *NSQd

func NewNSQd(workerId int64) *NSQd {
	n := &NSQd{
		workerId:        workerId,
		memQueueSize:    10000,
		dataPath:        os.TempDir(),
		maxBytesPerFile: 104857600,
		syncEvery:       2500,
		msgTimeout:      60 * time.Second,
		topicMap:        make(map[string]*Topic),
		idChan:          make(chan []byte, 4096),
		exitChan:        make(chan int),
		exitSyncChan:    make(chan int),
	}
	go n.idPump()
	return n
}

func (n *NSQd) Main() {
	go lookupRouter(n.lookupAddrs, n.exitChan, n.exitSyncChan)

	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err.Error())
	}
	n.tcpListener = tcpListener
	go util.TcpServer(tcpListener, &TcpProtocol{protocols: protocols}, n.exitSyncChan)

	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	go httpServer(httpListener, n.exitSyncChan)
}

func (n *NSQd) Exit() {
	n.tcpListener.Close()
	n.httpListener.Close()
	<-n.exitSyncChan
	<-n.exitSyncChan

	log.Printf("NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
	<-n.exitSyncChan
	<-n.exitSyncChan
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
	lastError := time.Now()
	for {
		id, err := NewGUID(n.workerId)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				log.Printf("ERROR: %s", err.Error())
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("ID: closing")
	n.exitSyncChan <- 1
}
