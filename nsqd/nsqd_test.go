package nsqd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqlookupd"
)

const (
	ConnectTimeout = 2 * time.Second
	RequestTimeout = 5 * time.Second
)

func getMetadata(n *NSQD) (*meta, error) {
	fn := fmt.Sprintf(path.Join(n.getOpts().DataPath, "nsqd.%d.dat"), n.getOpts().ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	var m meta
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func TestStartup(t *testing.T) {
	var msg *Message

	iterations := 300
	doneExitChan := make(chan int)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	origDataPath := opts.DataPath

	topicName := "nsqd_test" + strconv.Itoa(int(time.Now().Unix()))

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	// verify nsqd metadata shows no topics
	err := nsqd.PersistMetadata()
	test.Nil(t, err)
	atomic.StoreInt32(&nsqd.isLoading, 1)
	nsqd.GetTopic(topicName) // will not persist if `flagLoading`
	m, err := getMetadata(nsqd)
	test.Nil(t, err)
	test.Equal(t, 0, len(m.Topics))
	nsqd.DeleteExistingTopic(topicName)
	atomic.StoreInt32(&nsqd.isLoading, 0)

	body := make([]byte, 256)
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < iterations; i++ {
		msg := NewMessage(<-nsqd.idChan, body)
		topic.PutMessage(msg)
	}

	t.Logf("pulling from channel")
	channel1 := topic.GetChannel("ch1")

	t.Logf("read %d msgs", iterations/2)
	for i := 0; i < iterations/2; i++ {
		select {
		case msg = <-channel1.memoryMsgChan:
		case b := <-channel1.backend.ReadChan():
			msg, _ = decodeMessage(b)
		}
		t.Logf("read message %d", i+1)
		test.Equal(t, body, msg.Body)
	}

	for {
		if channel1.Depth() == int64(iterations/2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// make sure metadata shows the topic
	m, err = getMetadata(nsqd)
	test.Nil(t, err)
	test.Equal(t, 1, len(m.Topics))
	test.Equal(t, topicName, m.Topics[0].Name)

	exitChan <- 1
	<-doneExitChan

	// start up a new nsqd w/ the same folder

	opts = NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	opts.DataPath = origDataPath
	_, _, nsqd = mustStartNSQD(opts)

	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic = nsqd.GetTopic(topicName)
	// should be empty; channel should have drained everything
	count := topic.Depth()
	test.Equal(t, int64(0), count)

	channel1 = topic.GetChannel("ch1")

	for {
		if channel1.Depth() == int64(iterations/2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// read the other half of the messages
	for i := 0; i < iterations/2; i++ {
		select {
		case msg = <-channel1.memoryMsgChan:
		case b := <-channel1.backend.ReadChan():
			msg, _ = decodeMessage(b)
		}
		t.Logf("read message %d", i+1)
		test.Equal(t, body, msg.Body)
	}

	// verify we drained things
	test.Equal(t, 0, len(topic.memoryMsgChan))
	test.Equal(t, int64(0), topic.backend.Depth())

	exitChan <- 1
	<-doneExitChan
}

func TestEphemeralTopicsAndChannels(t *testing.T) {
	// ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.MemQueueSize = 100
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)

	topicName := "ephemeral_topic" + strconv.Itoa(int(time.Now().Unix())) + "#ephemeral"
	doneExitChan := make(chan int)

	exitChan := make(chan int)
	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	body := []byte("an_ephemeral_message")
	topic := nsqd.GetTopic(topicName)
	ephemeralChannel := topic.GetChannel("ch1#ephemeral")
	client := newClientV2(0, nil, &context{nsqd})
	ephemeralChannel.AddClient(client.ID, client)

	msg := NewMessage(<-nsqd.idChan, body)
	topic.PutMessage(msg)
	msg = <-ephemeralChannel.memoryMsgChan
	test.Equal(t, body, msg.Body)

	ephemeralChannel.RemoveClient(client.ID)

	time.Sleep(100 * time.Millisecond)

	topic.Lock()
	numChannels := len(topic.channelMap)
	topic.Unlock()
	test.Equal(t, 0, numChannels)

	nsqd.Lock()
	numTopics := len(nsqd.topicMap)
	nsqd.Unlock()
	test.Equal(t, 0, numTopics)

	exitChan <- 1
	<-doneExitChan
}

func TestPauseMetadata(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// avoid concurrency issue of async PersistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "pause_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.PersistMetadata()

	var isPaused = func(n *NSQD, topicIndex int, channelIndex int) bool {
		m, _ := getMetadata(n)
		return m.Topics[topicIndex].Channels[channelIndex].Paused
	}

	test.Equal(t, false, isPaused(nsqd, 0, 0))

	channel.Pause()
	test.Equal(t, false, isPaused(nsqd, 0, 0))

	nsqd.PersistMetadata()
	test.Equal(t, true, isPaused(nsqd, 0, 0))

	channel.UnPause()
	test.Equal(t, true, isPaused(nsqd, 0, 0))

	nsqd.PersistMetadata()
	test.Equal(t, false, isPaused(nsqd, 0, 0))
}

func mustStartNSQLookupd(opts *nsqlookupd.Options) (*net.TCPAddr, *net.TCPAddr, *nsqlookupd.NSQLookupd) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	lookupd := nsqlookupd.New(opts)
	lookupd.Main()
	return lookupd.RealTCPAddr(), lookupd.RealHTTPAddr(), lookupd
}

func TestReconfigure(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = test.NewTestLogger(t)
	_, _, lookupd1 := mustStartNSQLookupd(lopts)
	defer lookupd1.Exit()
	_, _, lookupd2 := mustStartNSQLookupd(lopts)
	defer lookupd2.Exit()
	_, _, lookupd3 := mustStartNSQLookupd(lopts)
	defer lookupd3.Exit()

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	time.Sleep(50 * time.Millisecond)

	newOpts := *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd1.RealTCPAddr().String()}
	nsqd.swapOpts(&newOpts)
	nsqd.triggerOptsNotification()
	test.Equal(t, 1, len(nsqd.getOpts().NSQLookupdTCPAddresses))

	time.Sleep(50 * time.Millisecond)

	numLookupPeers := len(nsqd.lookupPeers.Load().([]*lookupPeer))
	test.Equal(t, 1, numLookupPeers)

	newOpts = *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd2.RealTCPAddr().String(), lookupd3.RealTCPAddr().String()}
	nsqd.swapOpts(&newOpts)
	nsqd.triggerOptsNotification()
	test.Equal(t, 2, len(nsqd.getOpts().NSQLookupdTCPAddresses))

	time.Sleep(50 * time.Millisecond)

	var lookupPeers []string
	for _, lp := range nsqd.lookupPeers.Load().([]*lookupPeer) {
		lookupPeers = append(lookupPeers, lp.addr)
	}
	test.Equal(t, 2, len(lookupPeers))
	test.Equal(t, newOpts.NSQLookupdTCPAddresses, lookupPeers)
}

func TestCluster(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = test.NewTestLogger(t)
	lopts.BroadcastAddress = "127.0.0.1"
	_, _, lookupd := mustStartNSQLookupd(lopts)

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.NSQLookupdTCPAddresses = []string{lookupd.RealTCPAddr().String()}
	opts.BroadcastAddress = "127.0.0.1"
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "cluster_test" + strconv.Itoa(int(time.Now().Unix()))

	hostname, err := os.Hostname()
	test.Nil(t, err)

	url := fmt.Sprintf("http://%s/topic/create?topic=%s", nsqd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url)
	test.Nil(t, err)

	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=ch", nsqd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url)
	test.Nil(t, err)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	var d map[string][]struct {
		Hostname         string `json:"hostname"`
		BroadcastAddress string `json:"broadcast_address"`
		TCPPort          int    `json:"tcp_port"`
		Tombstoned       bool   `json:"tombstoned"`
	}

	endpoint := fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr())
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &d)
	test.Nil(t, err)

	topicData := d["topic:"+topicName+":"]
	test.Equal(t, 1, len(topicData))

	test.Equal(t, hostname, topicData[0].Hostname)
	test.Equal(t, "127.0.0.1", topicData[0].BroadcastAddress)
	test.Equal(t, nsqd.RealTCPAddr().Port, topicData[0].TCPPort)
	test.Equal(t, false, topicData[0].Tombstoned)

	channelData := d["channel:"+topicName+":ch"]
	test.Equal(t, 1, len(channelData))

	test.Equal(t, hostname, channelData[0].Hostname)
	test.Equal(t, "127.0.0.1", channelData[0].BroadcastAddress)
	test.Equal(t, nsqd.RealTCPAddr().Port, channelData[0].TCPPort)
	test.Equal(t, false, channelData[0].Tombstoned)

	var lr struct {
		Producers []struct {
			Hostname         string `json:"hostname"`
			BroadcastAddress string `json:"broadcast_address"`
			TCPPort          int    `json:"tcp_port"`
		} `json:"producers"`
		Channels []string `json:"channels"`
	}

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &lr)
	test.Nil(t, err)

	test.Equal(t, 1, len(lr.Producers))
	test.Equal(t, hostname, lr.Producers[0].Hostname)
	test.Equal(t, "127.0.0.1", lr.Producers[0].BroadcastAddress)
	test.Equal(t, nsqd.RealTCPAddr().Port, lr.Producers[0].TCPPort)
	test.Equal(t, 1, len(lr.Channels))
	test.Equal(t, "ch", lr.Channels[0])

	url = fmt.Sprintf("http://%s/topic/delete?topic=%s", nsqd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(url)
	test.Nil(t, err)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &lr)
	test.Nil(t, err)

	test.Equal(t, 0, len(lr.Producers))

	var dd map[string][]interface{}
	endpoint = fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr())
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &dd)
	test.Nil(t, err)

	test.Equal(t, 0, len(dd["topic:"+topicName+":"]))
	test.Equal(t, 0, len(dd["channel:"+topicName+":ch"]))
}

func TestSetHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	nsqd := New(opts)

	test.Equal(t, nil, nsqd.GetError())
	test.Equal(t, true, nsqd.IsHealthy())

	nsqd.SetHealth(nil)
	test.Equal(t, nil, nsqd.GetError())
	test.Equal(t, true, nsqd.IsHealthy())

	nsqd.SetHealth(errors.New("health error"))
	test.NotNil(t, nsqd.GetError())
	test.Equal(t, "NOK - health error", nsqd.GetHealth())
	test.Equal(t, false, nsqd.IsHealthy())

	nsqd.SetHealth(nil)
	test.Nil(t, nsqd.GetError())
	test.Equal(t, "OK", nsqd.GetHealth())
	test.Equal(t, true, nsqd.IsHealthy())
}
