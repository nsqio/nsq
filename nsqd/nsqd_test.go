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

	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/bitly/go-simplejson"
)

func init() {
	SetLogger(&levellogger.SimpleLogger{})
}

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

func newTestLogger(tbl tbLog) levellogger.Logger {
	return &testLogger{tbl, 0}
}

func mustStartNSQD(opts *Options) (*net.TCPAddr, *net.TCPAddr, *NSQD) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}
	nsqd := New(opts)
	nsqd.Start()
	return nil, nil, nsqd
}

func getMetadata(n *NSQD) (*simplejson.Json, error) {
	fn := fmt.Sprintf(path.Join(n.GetOpts().DataPath, "nsqd.%d.dat"), n.GetOpts().ID)
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
	opts.LogLevel = 2
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
	err := nsqd.PersistMetadata(nsqd.GetTopicMapCopy())
	equal(t, err, nil)
	atomic.StoreInt32(&nsqd.isLoading, 1)
	nsqd.GetTopicIgnPart(topicName) // will not persist if `flagLoading`
	metaData, err := getMetadata(nsqd)
	equal(t, err, nil)
	topics, err := metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 0)
	nsqd.DeleteExistingTopic(topicName, 0)
	atomic.StoreInt32(&nsqd.isLoading, 0)

	body := make([]byte, 256)
	tmpbuf := bytes.NewBuffer(make([]byte, 1024))
	tmpmsg := NewMessage(0, body)
	tmpbuf.Reset()
	msgRawSize, _ := tmpmsg.WriteTo(tmpbuf, false)
	msgRawSize += 4
	topic := nsqd.GetTopicIgnPart(topicName)
	channel1 := topic.GetChannel("ch1")
	for i := 0; i < iterations; i++ {
		msg := NewMessage(0, body)
		topic.PutMessage(msg)
	}

	topic.ForceFlush()
	t.Logf("topic: %v. %v", topic.GetCommitted(), topic.backend.GetQueueReadEnd())
	backEnd := topic.backend.GetQueueReadEnd()
	equal(t, backEnd.Offset(), BackendOffset(int64(iterations)*msgRawSize))
	equal(t, backEnd.TotalMsgCnt(), int64(iterations))
	channel2 := topic.GetChannel("ch2")

	err = nsqd.PersistMetadata(nsqd.GetTopicMapCopy())
	equal(t, err, nil)
	t.Logf("msgs: depth: %v. %v", channel1.Depth(), channel1.DepthSize())
	equal(t, channel1.Depth(), int64(iterations))
	equal(t, channel1.DepthSize(), int64(iterations)*msgRawSize)
	equal(t, channel1.backend.(*diskQueueReader).queueEndInfo.Offset(),
		BackendOffset(channel1.DepthSize()))

	// new channel should consume from end
	equal(t, channel2.Depth(), int64(0))
	equal(t, channel2.DepthSize(), int64(0)*msgRawSize)
	equal(t, channel2.backend.(*diskQueueReader).queueEndInfo.Offset(),
		BackendOffset(channel1.DepthSize()))
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		//t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}
	channel1.backend.ConfirmRead(BackendOffset(int64(iterations/2)*
		msgRawSize), int64(iterations/2))
	equal(t, channel1.backend.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(int64(iterations/2)*msgRawSize))
	equal(t, channel1.Depth(), int64(iterations/2))
	equal(t, channel1.DepthSize(), int64(iterations/2)*msgRawSize)

	// make sure metadata shows the topic
	metaData, err = getMetadata(nsqd)
	equal(t, err, nil)
	t.Logf("meta: %v", metaData)
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
	opts.LogLevel = 0
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

	topic = nsqd.GetTopicIgnPart(topicName)
	backEnd = topic.backend.GetQueueReadEnd()
	equal(t, backEnd.Offset(), BackendOffset(int64(iterations)*msgRawSize))
	equal(t, backEnd.TotalMsgCnt(), int64(iterations))

	channel1 = topic.GetChannel("ch1")
	channel1.UpdateQueueEnd(backEnd, false)

	equal(t, channel1.backend.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(int64(iterations/2)*msgRawSize))
	equal(t, channel1.backend.(*diskQueueReader).queueEndInfo.Offset(),
		backEnd.Offset())

	equal(t, channel1.Depth(), int64(iterations/2))
	equal(t, channel1.DepthSize(), int64(iterations/2)*msgRawSize)

	time.Sleep(time.Second)
	// read the other half of the messages
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		equal(t, msg.Body, body)
	}

	exitChan <- 1
	<-doneExitChan
}

func metadataForChannel(n *NSQD, topic *Topic, channelIndex int) *simplejson.Json {
	fn := topic.getChannelMetaFileName()
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		panic(err)
	}
	js, _ := simplejson.NewJson(data)

	mChannels := js
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
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("ch")
	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.PersistMetadata(nsqd.GetTopicMapCopy())

	b, _ := metadataForChannel(nsqd, topic, 0).Get("paused").Bool()
	equal(t, b, false)

	channel.Pause()
	b, _ = metadataForChannel(nsqd, topic, 0).Get("paused").Bool()
	equal(t, b, false)

	nsqd.PersistMetadata(nsqd.GetTopicMapCopy())
	b, _ = metadataForChannel(nsqd, topic, 0).Get("paused").Bool()
	equal(t, b, true)

	channel.UnPause()
	b, _ = metadataForChannel(nsqd, topic, 0).Get("paused").Bool()
	equal(t, b, true)

	nsqd.PersistMetadata(nsqd.GetTopicMapCopy())
	b, _ = metadataForChannel(nsqd, topic, 0).Get("paused").Bool()
	equal(t, b, false)
}

func TestSkipMetaData(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	// avoid concurrency issue of async PersistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "skip_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("ch")
	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.PersistMetadata(nsqd.GetTopicMapCopy())

	b, _ := metadataForChannel(nsqd, topic, 0).Get("skipped").Bool()
	equal(t, b, false)

	channel.Skip()
	b, _ = metadataForChannel(nsqd, topic, 0).Get("skipped").Bool()
	equal(t, b, false)

	nsqd.PersistMetadata(nsqd.GetTopicMapCopy())
	b, _ = metadataForChannel(nsqd, topic, 0).Get("skipped").Bool()
	equal(t, b, true)

	channel.UnSkip()
	b, _ = metadataForChannel(nsqd, topic, 0).Get("skipped").Bool()
	equal(t, b, true)

	nsqd.PersistMetadata(nsqd.GetTopicMapCopy())
	b, _ = metadataForChannel(nsqd, topic, 0).Get("skipped").Bool()
	equal(t, b, false)
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

func TestLoadTopicMetaExt(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	//defer nsqd.Exit()

	// avoid concurrency issue of async PersistMetadata() calls
	atomic.StoreInt32(&nsqd.isLoading, 1)
	topicName := "load_topic_meta" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")

	topicNameExt := "load_topic_meta_ext" + strconv.Itoa(int(time.Now().Unix()))
	topicExt := nsqd.GetTopicIgnPart(topicNameExt)
	topicDynConf := TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topicExt.SetDynamicInfo(topicDynConf, nil, nil)
	topicExt.GetChannel("ch")

	atomic.StoreInt32(&nsqd.isLoading, 0)
	nsqd.PersistMetadata(nsqd.GetTopicMapCopy())
	nsqd.Exit()

	_, _, nsqd = mustStartNSQD(opts)
	defer nsqd.Exit()
	nsqd.LoadMetadata(1)

	topic, err := nsqd.GetExistingTopic(topicName, 0)
	if err != nil {
		t.FailNow()
	}
	if topic.IsExt() {
		t.FailNow()
	}

	topicExt, err = nsqd.GetExistingTopic(topicNameExt, 0)
	if err != nil {
		t.FailNow()
	}
	if !topicExt.IsExt() {
		t.FailNow()
	}
}
