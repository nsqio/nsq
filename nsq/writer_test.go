package nsq

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

type ReaderHandler struct {
	t              *testing.T
	q              *Reader
	messagesGood   int
	messagesFailed int
}

func (h *ReaderHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *ReaderHandler) HandleMessage(message *Message) error {
	msg := string(message.Body)
	if msg == "bad_test_case" {
		return errors.New("fail this message")
	}
	if msg != "multipublish_test_case" && msg != "publish_test_case" {
		h.t.Error("message 'action' was not correct:", msg)
	}
	h.messagesGood++
	return nil
}

func TestWriterConnection(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	w := NewWriter("127.0.0.1:4150")

	_, _, err := w.Publish("write_test", []byte("test"))
	if err != nil {
		t.Fatalf("should lazily connect")
	}

	w.Stop()

	_, _, err = w.Publish("write_test", []byte("fail test"))
	if err != ErrStopped {
		t.Fatalf("should not be able to write after Stop()")
	}
}

func TestWriterPublish(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	w := NewWriter("127.0.0.1:4150")
	defer w.Stop()

	for i := 0; i < msgCount; i++ {
		frameType, data, err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
		}
	}

	frameType, data, err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}

	readMessages(topicName, t, msgCount)
}

func TestWriterMultiPublish(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	w := NewWriter("127.0.0.1:4150")
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	frameType, data, err := w.MultiPublish(topicName, testData)
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}

	frameType, data, err = w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}

	readMessages(topicName, t, msgCount)
}

func TestWriterPublishAsync(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "async_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	w := NewWriter("127.0.0.1:4150")
	defer w.Stop()

	responseChan := make(chan *WriterTransaction, msgCount)
	for i := 0; i < msgCount; i++ {
		err := w.PublishAsync(topicName, []byte("publish_test_case"), responseChan, "test")
		if err != nil {
			t.Fatalf(err.Error())
		}
	}

	for i := 0; i < msgCount; i++ {
		trans := <-responseChan
		if trans.Error != nil {
			t.Fatalf(trans.Error.Error())
		}
		if trans.FrameType != int32(0) {
			t.Fatalf("FrameType %d != 0", trans.FrameType)
		}
		if trans.Args[0].(string) != "test" {
			t.Fatalf(`proxied arg "%s" != "test"`, trans.Args[0].(string))
		}
	}

	frameType, data, err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}

	readMessages(topicName, t, msgCount)
}

func TestWriterMultiPublishAsync(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "multi_publish" + strconv.Itoa(int(time.Now().Unix()))
	msgCount := 10

	w := NewWriter("127.0.0.1:4150")
	defer w.Stop()

	var testData [][]byte
	for i := 0; i < msgCount; i++ {
		testData = append(testData, []byte("multipublish_test_case"))
	}

	responseChan := make(chan *WriterTransaction)
	err := w.MultiPublishAsync(topicName, testData, responseChan, "test0", 1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	trans := <-responseChan
	if trans.Error != nil {
		t.Fatalf(trans.Error.Error())
	}
	if trans.FrameType != int32(0) {
		t.Fatalf("FrameType %d != 0", trans.FrameType)
	}
	if trans.Args[0].(string) != "test0" {
		t.Fatalf(`proxied arg "%s" != "test0"`, trans.Args[0].(string))
	}
	if trans.Args[1].(int) != 1 {
		t.Fatalf(`proxied arg %d != 1`, trans.Args[1].(int))
	}

	frameType, data, err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}

	readMessages(topicName, t, msgCount)
}

func TestWriterHeartbeat(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "heartbeat" + strconv.Itoa(int(time.Now().Unix()))

	w := NewWriter("127.0.0.1:4150")
	defer w.Stop()
	w.HeartbeatInterval = 1 * time.Millisecond

	_, _, err := w.Publish(topicName, []byte("publish_test_case"))
	if err == nil {
		t.Fatalf("error should not be nil")
	}
	if err.Error() != "E_BAD_BODY IDENTIFY heartbeat interval (1) is invalid" {
		t.Fatalf("wrong error - %s", err)
	}

	w = NewWriter("127.0.0.1:4150")
	defer w.Stop()
	w.HeartbeatInterval = 1000 * time.Millisecond

	_, _, err = w.Publish(topicName, []byte("publish_test_case"))
	if err != nil {
		t.Fatalf(err.Error())
	}

	time.Sleep(1100 * time.Millisecond)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		frameType, data, err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
		}
	}

	frameType, data, err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}

	readMessages(topicName, t, msgCount+1)
}

func readMessages(topicName string, t *testing.T, msgCount int) {
	q, _ := NewReader(topicName, "ch")
	q.VerboseLogging = true
	q.DefaultRequeueDelay = 0
	q.SetMaxBackoffDuration(time.Millisecond * 50)

	h := &ReaderHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)

	err := q.ConnectToNSQ("127.0.0.1:4150")
	if err != nil {
		t.Fatalf(err.Error())
	}
	<-q.ExitChan

	if h.messagesGood != msgCount {
		t.Fatalf("end of test. should have handled a diff number of messages %d != %d", h.messagesGood, msgCount)
	}

	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
}
