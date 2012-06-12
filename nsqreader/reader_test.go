package nsqreader

import (
	"bytes"
	"github.com/bitly/go-simplejson"
	"log"
	"net"
	"net/http"
	"testing"
)

type MyTestHandler struct {
	t                *testing.T
	q                *Queue
	messagesSent     int
	messagesReceived int
}

func (h *MyTestHandler) HandleMessage(data *simplejson.Json) error {
	action, _ := data.Get("action").String()
	if action != "test1" {
		h.t.Error("message handled was not correct", data)
	}
	numeric_id, _ := data.Get("numeric_id").Int()
	if numeric_id != 12345678 {
		h.t.Error("message handled was not correct", data)
	}
	h.messagesReceived += 1
	if h.messagesReceived >= 4 {
		h.q.Stop()
	}
	return nil
}

func TestQueuereader(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:5151")
	q, _ := NewNSQReader("topic", "ch", addr)
	q.BatchSize = 2

	h := &MyTestHandler{t, q, 0, 0}
	q.AddHandler(h)

	// start a http client, and send in our messages
	httpclient := &http.Client{}
	endpoint := "http://127.0.0.1:5150/put?topic=topic"
	for i := 0; i < 4; i++ {
		body := []byte("{\"action\":\"test1\",\"numeric_id\":12345678}")
		req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
		log.Println("POST", endpoint, string(body))
		resp, err := httpclient.Do(req)
		if err != nil {
			log.Println(err)
		}
		resp.Body.Close()
	}

	q.Run()

	log.Println("got", h.messagesReceived, "and sent", h.messagesSent)
	if h.messagesReceived != 4 || h.messagesReceived != h.messagesSent {
		t.Fatalf("end of test. should have handled 2 messages", h.messagesReceived)
	}
}
