package main

import (
	"../nsq"
	"../util"
	"errors"
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

type NSQd struct {
	sync.RWMutex
	options         *nsqdOptions
	workerId        int64
	topicMap        map[string]*Topic
	lookupdTCPAddrs util.StringArray
	tcpAddr         *net.TCPAddr
	httpAddr        *net.TCPAddr
	tcpListener     net.Listener
	httpListener    net.Listener
	idChan          chan []byte
	exitChan        chan int
	waitGroup       util.WaitGroupWrapper
	lookupPeers     []*nsq.LookupPeer
}

type nsqdOptions struct {
	memQueueSize    int64
	dataPath        string
	maxBytesPerFile int64
	syncEvery       int64
	msgTimeout      time.Duration
	clientTimeout   time.Duration
}

func NewNsqdOptions() *nsqdOptions {
	return &nsqdOptions{
		memQueueSize:    10000,
		dataPath:        os.TempDir(),
		maxBytesPerFile: 104857600,
		syncEvery:       2500,
		msgTimeout:      60 * time.Second,
		clientTimeout:   nsq.DefaultClientTimeout,
	}
}

func NewNSQd(workerId int64, options *nsqdOptions) *NSQd {
	n := &NSQd{
		workerId: workerId,
		options:  options,
		topicMap: make(map[string]*Topic),
		idChan:   make(chan []byte, 4096),
		exitChan: make(chan int),
	}

	n.waitGroup.Wrap(func() { n.idPump() })

	return n
}

func (n *NSQd) Main() {
	n.waitGroup.Wrap(func() { n.lookupLoop() })

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
	fn := fmt.Sprintf(path.Join(n.options.dataPath, "nsqd.%d.dat"), n.workerId)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("ERROR: failed to read channel metadata from %s - %s", fn, err.Error())
		}
		return
	}

	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			parts := strings.SplitN(line, ":", 2)

			if !nsq.IsValidTopicName(parts[0]) {
				log.Printf("WARNING: skipping creation of invalid topic %s", parts[0])
				continue
			}
			topic := n.GetTopic(parts[0])

			if len(parts) < 2 {
				continue
			}
			if !nsq.IsValidChannelName(parts[1]) {
				log.Printf("WARNING: skipping creation of invalid channel %s", parts[1])
			}
			topic.GetChannel(parts[1])
		}
	}
}

func (n *NSQd) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fn := fmt.Sprintf(path.Join(n.options.dataPath, "nsqd.%d.dat"), n.workerId)
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Printf("ERROR: failed to open channel metadata file %s - %s", fn, err.Error())
	}

	log.Printf("NSQ: closing topics")
	n.Lock()
	for _, topic := range n.topicMap {
		if f != nil {
			topic.Lock()
			fmt.Fprintf(f, "%s\n", topic.name)
			for _, channel := range topic.channelMap {
				if !channel.ephemeralChannel {
					fmt.Fprintf(f, "%s:%s\n", topic.name, channel.name)
				}
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
	t, ok := n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	} else {
		t = NewTopic(topicName, n.options)
		n.topicMap[topicName] = t
		log.Printf("TOPIC(%s): created", t.name)

		// release our global nsqd lock, and switch to a more granular topic lock while we init our 
		// channels from lookupd. This blocks concurrent PutMessages to this topic.
		t.Lock()
		defer t.Unlock()
		n.Unlock()
		// if using lookupd, make a blocking call to get the topics, and immediately create them.
		// this makes sure that any message received is buffered to the right channels
		if len(n.lookupPeers) > 0 {
			channelNames, _ := util.GetChannelsForTopic(t.name, n.lookupHttpAddrs())
			for _, channelName := range channelNames {
				t.getOrCreateChannel(channelName)
			}
		}
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQd) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQd) DeleteExistingTopic(topicName string) error {
	n.Lock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.Unlock()
		return errors.New("topic does not exist")
	}
	delete(n.topicMap, topicName)
	// not defered so that we can continue while the topic async closes
	n.Unlock()

	log.Printf("TOPIC(%s): deleting", topic.name)

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	topic.Delete()

	return nil
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
