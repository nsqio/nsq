package main

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/lookupd"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex

	options *nsqdOptions

	topicMap map[string]*Topic

	lookupPeers []*LookupPeer

	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	tlsConfig    *tls.Config

	idChan     chan nsq.MessageID
	notifyChan chan interface{}
	exitChan   chan int
	waitGroup  util.WaitGroupWrapper
}

func NewNSQD(options *nsqdOptions) *NSQD {
	var tlsConfig *tls.Config

	if options.MaxDeflateLevel < 1 || options.MaxDeflateLevel > 9 {
		log.Fatalf("--max-deflate-level must be [1,9]")
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", options.TCPAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", options.HTTPAddress)
	if err != nil {
		log.Fatal(err)
	}

	if options.StatsdPrefix != "" {
		statsdHostKey := util.StatsdHostKey(net.JoinHostPort(options.BroadcastAddress,
			strconv.Itoa(httpAddr.Port)))
		prefixWithHost := strings.Replace(options.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		options.StatsdPrefix = prefixWithHost
	}

	if options.TLSCert != "" || options.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(options.TLSCert, options.TLSKey)
		if err != nil {
			log.Fatalf("ERROR: failed to LoadX509KeyPair %s", err.Error())
		}
		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.VerifyClientCertIfGiven,
		}
		tlsConfig.BuildNameToCertificate()
	}

	n := &NSQD{
		options:    options,
		tcpAddr:    tcpAddr,
		httpAddr:   httpAddr,
		topicMap:   make(map[string]*Topic),
		idChan:     make(chan nsq.MessageID, 4096),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
		tlsConfig:  tlsConfig,
	}

	n.waitGroup.Wrap(func() { n.idPump() })

	return n
}

func (n *NSQD) Main() {
	context := &Context{n}

	n.waitGroup.Wrap(func() { n.lookupLoop() })

	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.tcpAddr, err.Error())
	}
	n.tcpListener = tcpListener
	tcpServer := &tcpServer{context: context}
	n.waitGroup.Wrap(func() { util.TCPServer(n.tcpListener, tcpServer) })

	httpListener, err := net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", n.httpAddr, err.Error())
	}
	n.httpListener = httpListener
	httpServer := &httpServer{context: context}
	n.waitGroup.Wrap(func() { util.HTTPServer(n.httpListener, httpServer) })

	if n.options.StatsdAddress != "" {
		n.waitGroup.Wrap(func() { n.statsdLoop() })
	}
}

func (n *NSQD) LoadMetadata() {
	fn := fmt.Sprintf(path.Join(n.options.DataPath, "nsqd.%d.dat"), n.options.ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("ERROR: failed to read channel metadata from %s - %s", fn, err.Error())
		}
		return
	}

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

	for ti := range topics {
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

		paused, _ := topicJs.Get("paused").Bool()
		if paused {
			topic.Pause()
		}

		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			log.Printf("ERROR: failed to parse metadata - %s", err.Error())
			return
		}

		for ci := range channels {
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

			paused, _ = channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}
		}
	}
}

func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.options.DataPath, "nsqd.%d.dat"), n.options.ID)
	log.Printf("NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := make([]interface{}, 0)
	for _, topic := range n.topicMap {
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
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
		return err
	}

	tmpFileName := fileName + ".tmp"
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = os.Rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		log.Printf("ERROR: failed to persist metadata - %s", err.Error())
	}
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
func (n *NSQD) GetTopic(topicName string) *Topic {
	n.Lock()
	t, ok := n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	} else {
		t = NewTopic(topicName, &Context{n})
		n.topicMap[topicName] = t

		log.Printf("TOPIC(%s): created", t.name)

		// release our global nsqd lock, and switch to a more granular topic lock while we init our
		// channels from lookupd. This blocks concurrent PutMessages to this topic.
		t.Lock()
		n.Unlock()
		// if using lookupd, make a blocking call to get the topics, and immediately create them.
		// this makes sure that any message received is buffered to the right channels
		if len(n.lookupPeers) > 0 {
			channelNames, _ := lookupd.GetLookupdTopicChannels(t.name, n.lookupHttpAddrs())
			for _, channelName := range channelNames {
				t.getOrCreateChannel(channelName)
			}
		}
		t.Unlock()

		// NOTE: I would prefer for this to only happen in topic.GetChannel() but we're special
		// casing the code above so that we can control the locks such that it is impossible
		// for a message to be written to a (new) topic while we're looking up channels
		// from lookupd...
		//
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

func (n *NSQD) idPump() {
	lastError := time.Now()
	for {
		id, err := NewGUID(n.options.ID)
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

func (n *NSQD) Notify(v interface{}) {
	// by selecting on exitChan we guarantee that
	// we do not block exit, see issue #123
	select {
	case <-n.exitChan:
	case n.notifyChan <- v:
		n.Lock()
		err := n.PersistMetadata()
		if err != nil {
			log.Printf("ERROR: failed to persist metadata - %s", err.Error())
		}
		n.Unlock()
	}
}
