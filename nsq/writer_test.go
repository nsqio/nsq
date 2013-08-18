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
func TestConnection(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	addr := "127.0.0.1:4150"
	w := NewWriter(0)
	err := w.ConnectToNSQ(addr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	w.Stop()
	_, _, err = w.Publish("write_test", []byte("failed test"))
	if err.Error() != "not connected" {
		t.Fatalf("should not be able to write")
	}
	err = w.ConnectToNSQ(addr)
	if err.Error() != "writer stopped" {
		t.Fatalf("should not be able to connect to NSQ after Stop()")
	}
}

func TestWriteCommand(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	addr := "127.0.0.1:4150"
	topicName := "write_test" + strconv.Itoa(int(time.Now().Unix()))
	w := NewWriter(0)
	err := w.ConnectToNSQ(addr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	msg_count := 10
	for i := 0; i < msg_count; i++ {
		frameType, data, err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
		}
		frameType, data, err = w.Publish(topicName+"a", []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
		}
	}
	var test_data [][]byte
	test_data = append(test_data, []byte("multipublish_test_case"))
	test_data = append(test_data, []byte("multipublish_test_case"))
	for i := 0; i < msg_count; i++ {
		frameType, data, err := w.MultiPublish(topicName, test_data)
		if err != nil {
			t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
		}
		frameType, data, err = w.MultiPublish(topicName+"a", test_data)
		if err != nil {
			t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
		}
	}
	frameType, data, err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}
	frameType, data, err = w.Publish(topicName+"a", []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}
	ReadMessage(topicName, t, msg_count*3)
	ReadMessage(topicName+"a", t, msg_count*3)
}

func TestHeartbeat(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	addr := "127.0.0.1:4150"
	topicName := "write_heartbeat_test" + strconv.Itoa(int(time.Now().Unix()))
	w := NewWriter(1)
	err := w.ConnectToNSQ(addr)
	if err == nil {
		t.Fatalf(err.Error())
	}
	_, _, err = w.Publish(topicName, []byte("publish_test_case"))
	if err.Error() != "not connected" {
		t.Fatalf(err.Error())
	}
	w = NewWriter(1000)
	err = w.ConnectToNSQ(addr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	time.Sleep(1500 * time.Millisecond)
	msg_count := 10
	for i := 0; i < msg_count; i++ {
		frameType, data, err := w.Publish(topicName, []byte("publish_test_case"))
		if err != nil {
			t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
		}
	}
	frameType, data, err := w.Publish(topicName, []byte("bad_test_case"))
	if err != nil {
		t.Fatalf("frametype %d data %s error %s", frameType, string(data), err.Error())
	}
	ReadMessage(topicName, t, msg_count)
}

func ReadMessage(topicName string, t *testing.T, msg_count int) {
	addr := "127.0.0.1:4150"
	q, _ := NewReader(topicName, "ch")
	q.VerboseLogging = true
	q.DefaultRequeueDelay = 0
	q.SetMaxBackoffDuration(time.Millisecond * 50)

	h := &ReaderHandler{
		t: t,
		q: q,
	}
	q.AddHandler(h)
	err := q.ConnectToNSQ(addr)
	if err != nil {
		t.Fatalf(err.Error())
	}
	<-q.ExitChan
	log.Println(topicName, "got", h.messagesGood, "and sent", msg_count)
	if h.messagesGood != msg_count {
		t.Fatalf("end of test. should have handled a diff number of messages")
	}
	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
}
