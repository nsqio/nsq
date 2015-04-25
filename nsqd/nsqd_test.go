package nsqd

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
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
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) logger {
	return &testLogger{tbl}
}

func getMetadata(n *NSQD) (*simplejson.Json, error) {
	fn := fmt.Sprintf(path.Join(n.opts.DataPath, "nsqd.%d.dat"), n.opts.ID)
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

func TestStartup(t *testing.T) {
	iterations := 300
	doneExitChan := make(chan int)

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	_, _, nsqd := mustStartNSQD(opts)

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
	nsqd.setFlag(flagLoading, true)
	nsqd.GetTopic(topicName) // will not persist if `flagLoading`
	metaData, err := getMetadata(nsqd)
	equal(t, err, nil)
	topics, err := metaData.Get("topics").Array()
	equal(t, err, nil)
	equal(t, len(topics), 0)
	nsqd.DeleteExistingTopic(topicName)
	nsqd.setFlag(flagLoading, false)

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
		msg := <-channel1.clientMsgChan
		t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}

	for {
		if channel1.Depth() == int64(iterations/2) {
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

	opts = NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	opts.MaxBytesPerFile = 10240
	_, _, nsqd = mustStartNSQD(opts)

	go func() {
		<-exitChan
		nsqd.Exit()
		doneExitChan <- 1
	}()

	topic = nsqd.GetTopic(topicName)
	// should be empty; channel should have drained everything
	count := topic.Depth()
	equal(t, count, int64(0))

	channel1 = topic.GetChannel("ch1")

	for {
		if channel1.Depth() == int64(iterations/2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// read the other half of the messages
	for i := 0; i < iterations/2; i++ {
		msg := <-channel1.clientMsgChan
		t.Logf("read message %d", i+1)
		equal(t, msg.Body, body)
	}

	// verify we drained things
	equal(t, len(topic.memoryMsgChan), 0)
	equal(t, topic.backend.Depth(), int64(0))

	exitChan <- 1
	<-doneExitChan
}

func TestEphemeralTopicsAndChannels(t *testing.T) {
	// ephemeral topics/channels are lazily removed after the last channel/client is removed
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.MemQueueSize = 100
	_, _, nsqd := mustStartNSQD(opts)

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
	msg = <-ephemeralChannel.clientMsgChan
	equal(t, msg.Body, body)

	ephemeralChannel.RemoveClient(client.ID)

	time.Sleep(100 * time.Millisecond)

	topic.Lock()
	numChannels := len(topic.channelMap)
	topic.Unlock()
	equal(t, numChannels, 0)

	nsqd.Lock()
	numTopics := len(nsqd.topicMap)
	nsqd.Unlock()
	equal(t, numTopics, 0)

	exitChan <- 1
	<-doneExitChan
}

func metadataForChannel(n *NSQD, topicIndex int, channelIndex int) *simplejson.Json {
	metadata, _ := getMetadata(n)
	mChannels := metadata.Get("topics").GetIndex(topicIndex).Get("channels")
	return mChannels.GetIndex(channelIndex)
}

func TestPauseMetadata(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	// avoid concurrency issue of async PersistMetadata() calls
	nsqd.setFlag(flagLoading, true)
	topicName := "pause_metadata" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("ch")
	nsqd.setFlag(flagLoading, false)
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
