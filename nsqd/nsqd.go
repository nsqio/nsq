package main

import (
	"../nsq"
	"../util"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-simplejson"
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

type Notifier interface {
	Notify(v interface{})
}

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
	idChan          chan nsq.MessageID
	exitChan        chan int
	waitGroup       util.WaitGroupWrapper
	lookupPeers     []*nsq.LookupPeer
	notifyChan      chan interface{}
}

type nsqdOptions struct {
	memQueueSize    int64
	dataPath        string
	maxMessageSize  int64
	maxBodySize     int64
	maxBytesPerFile int64
	syncEvery       int64
	msgTimeout      time.Duration
	clientTimeout   time.Duration
}

func NewNsqdOptions() *nsqdOptions {
	return &nsqdOptions{
		memQueueSize:    10000,
		dataPath:        os.TempDir(),
		maxMessageSize:  1024768,
		maxBodySize:     5 * 1024768,
		maxBytesPerFile: 104857600,
		syncEvery:       2500,
		msgTimeout:      60 * time.Second,
		clientTimeout:   nsq.DefaultClientTimeout,
	}
}

func NewNSQd(workerId int64, options *nsqdOptions) *NSQd {
	n := &NSQd{
		workerId:   workerId,
		options:    options,
		topicMap:   make(map[string]*Topic),
		idChan:     make(chan nsq.MessageID, 4096),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
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

	// channels cannot start with '{' so we use that to introduce a way to pivot
	// and be backwards compatible.  The new format is JSON so that adding fields
	// is easy going forward.
	if data[0] == '{' {
		js, err := simplejson.NewJson(data)
		if err != nil {
			log.Printf("ERROR: failed to parse metadata - %s", err.Error())
			return
		}

		topics, err := js.Get("topics").Array()
		if err != nil {
			log.Printf("ERROR: failed to parse metadata - %s", err.Error())
			return
		}

		for ti, _ := range topics {
			topicJs := js.Get("topics").GetIndex(ti)

			topicName, err := topicJs.Get("name").String()
			if err != nil {
				log.Printf("ERROR: failed to parse metadata - %s", err.Error())
				return
			}
			if !nsq.IsValidTopicName(topicName) {
				log.Printf("WARNING: skipping creation of invalid topic %s", topicName)
				continue
			}
			topic := n.GetTopic(topicName)

			channels, err := topicJs.Get("channels").Array()
			if err != nil {
				log.Printf("ERROR: failed to parse metadata - %s", err.Error())
				return
			}

			for ci, _ := range channels {
				channelJs := topicJs.Get("channels").GetIndex(ci)

				channelName, err := channelJs.Get("name").String()
				if err != nil {
					log.Printf("ERROR: failed to parse metadata - %s", err.Error())
					return
				}
				if !nsq.IsValidChannelName(channelName) {
					log.Printf("WARNING: skipping creation of invalid channel %s", channelName)
					continue
				}
				channel := topic.GetChannel(channelName)

				paused, _ := channelJs.Get("paused").Bool()
				if paused {
					channel.Pause()
				}
			}
		}
	} else {
		// TODO: remove this in the next release
		// old line oriented, : separated, format
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
					continue
				}
				topic.GetChannel(parts[1])
			}
		}
	}
}

func (n *NSQd) PersistMetadata() {
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.options.dataPath, "nsqd.%d.dat"), n.workerId)
	log.Printf("NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := make([]interface{}, 0)
	for _, topic := range n.topicMap {
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		channels := make([]interface{}, 0)
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if !channel.ephemeralChannel {
				channelData := make(map[string]interface{})
				channelData["name"] = channel.name
				channelData["paused"] = channel.IsPaused()
				channels = append(channels, channelData)
			}
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = util.BINARY_VERSION
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		log.Printf("ERROR: failed to marshal JSON metadata - %s", err.Error())
		return
	}

	tmpFileName := fileName + ".tmp"
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Printf("ERROR: failed to open temporary metadata file %s - %s", tmpFileName, err.Error())
		return
	}

	_, err = f.Write(data)
	if err != nil {
		log.Printf("ERROR: failed to write to temporary metadata file %s - %s", tmpFileName, err.Error())
		f.Close()
		return
	}
	f.Sync()
	f.Close()

	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		log.Printf("ERROR: failed to rename temporary metadata file %s to %s - %s", tmpFileName, fileName, err.Error())
	}
}

func (n *NSQd) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	n.Lock()
	n.PersistMetadata()
	log.Printf("NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

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
		t = NewTopic(topicName, n.options, n)
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

	// since we are explicitly deleting a topic (not just at system exit time)
	// de-register this from the lookupd
	go n.Notify(topic)

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

func (n *NSQd) Notify(v interface{}) {
	// by selecting on exitChan we guarantee that
	// we do not block exit, see issue #123
	select {
	case <-n.exitChan:
	case n.notifyChan <- v:
	}
}
