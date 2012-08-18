package main

import (
	"../nsq"
	"../util"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
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
	waitGroup       util.WaitGroupWrapper
	clientTimeout   time.Duration
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
		clientTimeout:   nsq.DefaultClientTimeout,
	}

	n.waitGroup.Wrap(func() { n.idPump() })

	return n
}

func (n *NSQd) Main() {
	n.waitGroup.Wrap(func() { lookupRouter(n.lookupAddrs, n.exitChan) })

	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err.Error())
	}
	n.tcpListener = tcpListener
	n.waitGroup.Wrap(func() { util.TcpServer(n.tcpListener, &TcpProtocol{protocols: protocols}) })

	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	n.waitGroup.Wrap(func() { httpServer(n.httpListener) })
}

func (n *NSQd) LoadMetadata() {
	fn := fmt.Sprintf(path.Join(n.dataPath, "nsqd.%d.dat"), n.workerId)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("ERROR: failed to read channel metadata from %s - %s", fn, err.Error())
		}
		return
	}

	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			parts := strings.Split(line, ":")
			if len(parts) < 2 {
				log.Printf("WARNING: skipping topic/channel creation for %s", line)
				continue
			}

			if !nsq.IsValidName(parts[0]) {
				log.Printf("WARNING: skipping creation of invalid topic %s", parts[0])
				continue
			}
			topic := n.GetTopic(parts[0])

			if !nsq.IsValidName(parts[1]) {
				log.Printf("WARNING: skipping creation of invalid channel %s", parts[1])
			}
			topic.GetChannel(parts[1])
		}
	}
}

func (n *NSQd) Exit() {
	n.tcpListener.Close()
	n.httpListener.Close()

	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fn := fmt.Sprintf(path.Join(n.dataPath, "nsqd.%d.dat"), n.workerId)
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Printf("ERROR: failed to open channel metadata file %s - %s", fn, err.Error())
	}

	log.Printf("NSQ: closing topics")
	n.Lock()
	for _, topic := range n.topicMap {
		if f != nil {
			topic.Lock()
			for _, channel := range topic.channelMap {
				fmt.Fprintf(f, "%s:%s\n", topic.name, channel.name)
			}
			topic.Unlock()
		}
		topic.Close()
	}
	n.Unlock()

	if f != nil {
		f.Sync()
		f.Close()
	}

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
	n.waitGroup.Wait()
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
}
