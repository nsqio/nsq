package nsq

import (
	"bitly/simplejson"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"testing"
)

type MyTestHandler struct {
	t                *testing.T
	q                *Reader
	messagesSent     int
	messagesReceived int
	messagesFailed   int
}

func (h *MyTestHandler) LogFailedMessage(message *Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *MyTestHandler) HandleMessage(message *Message) error {
	if string(message.Body) == "TOBEFAILED" {
		h.messagesReceived++
		return errors.New("fail this message")
	}

	data, err := simplejson.NewJson(message.Body)
	if err != nil {
		return err
	}

	msg, _ := data.Get("msg").String()
	if msg != "single" && msg != "double" {
		h.t.Error("message 'action' was not correct: ", msg, data)
	}
	h.messagesReceived++
	return nil
}

func SendMessage(t *testing.T, port int, topic string, method string, body []byte) {
	httpclient := &http.Client{}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/%s?topic=%s", port, method, topic)
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	resp, err := httpclient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
		return
	}
	resp.Body.Close()
}

func TestQueuereader(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:4150")
	q, _ := NewReader("reader_test", "ch")
	q.DefaultRequeueDelay = 0 // so that the test can simulate reaching max requeues and a call to LogFailedMessage

	h := &MyTestHandler{t, q, 0, 0, 0}
	q.AddHandler(h)

	SendMessage(t, 4151, "reader_test", "put", []byte(`{"msg":"single"}`))
	SendMessage(t, 4151, "reader_test", "mput", []byte("{\"msg\":\"double\"}\n{\"msg\":\"double\"}"))
	SendMessage(t, 4151, "reader_test", "put", []byte("TOBEFAILED"))
	h.messagesSent = 4

	err := q.ConnectToNSQ(addr)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = q.ConnectToNSQ(addr)
	if err == nil {
		t.Fatalf("should not be able to connect to the same NSQ twice")
	}

	<-q.ExitChan

	log.Println("got", h.messagesReceived, "and sent", h.messagesSent)
	if h.messagesReceived != 9 || h.messagesSent != 4 {
		t.Fatalf("end of test. should have handled a diff number of messages")
	}
	if h.messagesFailed != 1 {
		t.Fatal("failed message not done")
	}
}
