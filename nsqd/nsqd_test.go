package nsqd

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absolute8511/nsq/nsqlookupd"
	"github.com/bitly/go-simplejson"
	"github.com/nsqio/nsq/internal/http_api"
)

func assert(t *testing.T, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line}, v...)...)
		t.FailNow()
	}
}

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func nequal(t *testing.T, act, exp interface{}) {
	if reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
	level int32
}

func (tl *testLogger) SetLevel(l int32) {
	tl.level = l
}

func (tl *testLogger) Level() int32 {
	return tl.level
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}
func (tl *testLogger) OutputErr(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}
func (tl *testLogger) OutputWarning(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) logger {
	return &testLogger{tbl, 0}
}

func getMetadata(n *NSQD) (*simplejson.Json, error) {
	fn := fmt.Sprintf(path.Join(n.getOpts().DataPath, "nsqd.%d.dat"), n.getOpts().ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		return nil, err
	}
	return js, nil
}

func API(endpoint string) (data *simplejson.Json, err error) {
	d := make(map[string]interface{})
	err = http_api.NewClient(nil).NegotiateV1(endpoint, &d)
	data = simplejson.New()
	data.SetPath(nil, d)
	return
}

func TestStartup(t *testing.T) {
	iterations := 300
	doneExitChan := make(chan int)

	opts := NewOptions()
	opts.SyncEvery = 1
	opts.Logger = newTestLogger(t)
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
	equal(t, err, nil)
	atomic.StoreInt32(&nsqd.isLoading, 1)
	nsqd.GetTopic(topicName) // will not persist if `flagLoading`
	metaData, err := getMetadata(nsqd)
	equal(t, err, nil)
	topics, err := metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 0)
	nsqd.DeleteExistingTopic(topicName)
	atomic.StoreInt32(&nsqd.isLoading, 0)

	body := make([]byte, 256)
	tmpbuf := bytes.NewBuffer(make([]byte, 1024))
	tmpmsg := NewMessage(0, body)
	tmpbuf.Reset()
	msgRawSize, _ := tmpmsg.WriteTo(tmpbuf)
	msgRawSize += 4
	topic := nsqd.GetTopic(topicName)
	for i := 0; i < iterations; i++ {
		msg := NewMessage(topic.NextMsgID(), body)
		topic.PutMessage(msg)
	}

	topic.flush()
	backEnd := topic.backend.GetQueueReadEnd()
	equal(t, backEnd.GetOffset(), BackendOffset(int64(iterations)*msgRawSize))
	equal(t, backEnd.GetTotalMsgCnt(), int64(iterations))
	t.Logf("pulling from channel")
	channel1 := topic.GetChannel("ch1")
	channel1.UpdateQueueEnd(backEnd)

	t.Logf("read %d msgs", iterations/2)
	equal(t, channel1.Depth(), int64(iterations)*msgRawSize)
	equal(t, channel1.backend.(*diskQueueReader).virtualEnd,
		BackendOffset(channel1.Depth()))
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}
	channel1.backend.ConfirmRead(BackendOffset(int64(iterations/2) *
		msgRawSize))
	equal(t, channel1.backend.(*diskQueueReader).virtualConfirmedOffset,
		BackendOffset(int64(iterations/2)*msgRawSize))
	equal(t, channel1.Depth(), int64(iterations/2)*msgRawSize)

	for {
		if channel1.Depth() == int64(iterations/2)*msgRawSize {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// make sure metadata shows the topic
	metaData, err = getMetadata(nsqd)
	equal(t, err, nil)
	topics, err = metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 1)
	observedTopicName, err := metaData.Get("topics").GetIndex(0).Get("name").String()
	equal(t, observedTopicName, topicName)
	equal(t, err, nil)

	exitChan <- 1
	<-doneExitChan

	// start up a new nsqd w/ the same folder

	opts = NewOptions()
	opts.Logger = newTestLogger(t)
	opts.SyncEvery = 1
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
	backEnd = topic.backend.GetQueueReadEnd()
	equal(t, backEnd.GetOffset(), BackendOffset(int64(iterations)*msgRawSize))
	equal(t, backEnd.GetTotalMsgCnt(), int64(iterations))

	channel1 = topic.GetChannel("ch1")
	channel1.UpdateQueueEnd(backEnd)

	equal(t, channel1.backend.(*diskQueueReader).virtualConfirmedOffset,
		BackendOffset(int64(iterations/2)*msgRawSize))
	equal(t, channel1.backend.(*diskQueueReader).virtualEnd,
		backEnd.GetOffset())

	equal(t, channel1.Depth(), int64(iterations/2)*msgRawSize)

	// read the other half of the messages
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}

	exitChan <- 1
	<-doneExitChan
}

func metadataForChannel(n *NSQD, topicIndex int, channelIndex int) *simplejson.Json {
	metadata, _ := getMetadata(n)
	mChannels := metadata.Get("topics").GetIndex(topicIndex).Get("channels")
	return mChannels.GetIndex(channelIndex)
}

func TestPauseMetadata(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
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

	b, _ := metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, false)

	channel.Pause()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, false)

	nsqd.PersistMetadata()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, true)

	channel.UnPause()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, true)

	nsqd.PersistMetadata()
	b, _ = metadataForChannel(nsqd, 0, 0).Get("paused").Bool()
	equal(t, b, false)
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
	lopts.Logger = newTestLogger(t)
	_, _, lookupd1 := mustStartNSQLookupd(lopts)
	defer lookupd1.Exit()
	_, _, lookupd2 := mustStartNSQLookupd(lopts)
	defer lookupd2.Exit()
	_, _, lookupd3 := mustStartNSQLookupd(lopts)
	defer lookupd3.Exit()

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	time.Sleep(50 * time.Millisecond)

	newOpts := *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd1.RealTCPAddr().String()}
	nsqd.swapOpts(&newOpts)
	nsqd.triggerOptsNotification()
	equal(t, len(nsqd.getOpts().NSQLookupdTCPAddresses), 1)

	time.Sleep(50 * time.Millisecond)

	numLookupPeers := len(nsqd.lookupPeers.Load().([]*lookupPeer))
	equal(t, numLookupPeers, 1)

	newOpts = *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd2.RealTCPAddr().String(), lookupd3.RealTCPAddr().String()}
	nsqd.swapOpts(&newOpts)
	nsqd.triggerOptsNotification()
	equal(t, len(nsqd.getOpts().NSQLookupdTCPAddresses), 2)

	time.Sleep(50 * time.Millisecond)

	var lookupPeers []string
	for _, lp := range nsqd.lookupPeers.Load().([]*lookupPeer) {
		lookupPeers = append(lookupPeers, lp.addr)
	}
	equal(t, len(lookupPeers), 2)
	equal(t, lookupPeers, newOpts.NSQLookupdTCPAddresses)
}

func TestCluster(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = newTestLogger(t)
	lopts.BroadcastAddress = "127.0.0.1"
	_, _, lookupd := mustStartNSQLookupd(lopts)

	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.NSQLookupdTCPAddresses = []string{lookupd.RealTCPAddr().String()}
	opts.BroadcastAddress = "127.0.0.1"
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "cluster_test" + strconv.Itoa(int(time.Now().Unix()))

	hostname, err := os.Hostname()
	equal(t, err, nil)

	url := fmt.Sprintf("http://%s/topic/create?topic=%s", nsqd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil).POSTV1(url)
	equal(t, err, nil)

	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=ch", nsqd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil).POSTV1(url)
	equal(t, err, nil)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	endpoint := fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr())
	data, err := API(endpoint)
	equal(t, err, nil)

	t.Logf("debug data: %v", data)
	topicData := data.Get("topic:" + topicName + ":")
	producers, _ := topicData.Array()
	equal(t, len(producers), 1)

	producer := topicData.GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), nsqd.RealTCPAddr().Port)
	equal(t, producer.Get("tombstoned").MustBool(), false)

	channelData := data.Get("channel:" + topicName + ":ch")
	producers, _ = channelData.Array()
	equal(t, len(producers), 1)

	producer = topicData.GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), nsqd.RealTCPAddr().Port)
	equal(t, producer.Get("tombstoned").MustBool(), false)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName)
	data, err = API(endpoint)

	t.Logf("debug data: %v", data)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 1)

	producer = data.Get("producers").GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), nsqd.RealTCPAddr().Port)

	channels, _ := data.Get("channels").Array()
	equal(t, len(channels), 1)

	channel := channels[0].(string)
	equal(t, channel, "ch")

	url = fmt.Sprintf("http://%s/topic/delete?topic=%s", nsqd.RealHTTPAddr(), topicName)
	err = http_api.NewClient(nil).POSTV1(url)
	equal(t, err, nil)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName)
	data, err = API(endpoint)

	equal(t, err, nil)

	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 0)

	endpoint = fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr())
	data, err = API(endpoint)

	equal(t, err, nil)

	producers, _ = data.Get("topic:" + topicName + ":").Array()
	equal(t, len(producers), 0)

	producers, _ = data.Get("channel:" + topicName + ":ch").Array()
	equal(t, len(producers), 0)
}

func TestSetHealth(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	nsqd := New(opts)

	equal(t, nsqd.GetError(), nil)
	equal(t, nsqd.IsHealthy(), true)

	nsqd.SetHealth(nil)
	equal(t, nsqd.GetError(), nil)
	equal(t, nsqd.IsHealthy(), true)

	nsqd.SetHealth(errors.New("health error"))
	nequal(t, nsqd.GetError(), nil)
	equal(t, nsqd.GetHealth(), "NOK - health error")
	equal(t, nsqd.IsHealthy(), false)

	nsqd.SetHealth(nil)
	equal(t, nsqd.GetError(), nil)
	equal(t, nsqd.GetHealth(), "OK")
	equal(t, nsqd.IsHealthy(), true)
}
