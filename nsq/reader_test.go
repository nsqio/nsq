package nsq

import (
	"bitly/simplejson"
	"bytes"
	"log"
	"net"
	"net/http"
	"strconv"
	"testing"
)

type MyTestHandler struct {
	t                *testing.T
	q                *Reader
	messagesSent     int
	messagesReceived int
}

func (h *MyTestHandler) HandleMessage(d []byte) error {
	data, err := simplejson.NewJson(d)
	if err != nil {
		return err
	}

	_, ok := data.CheckGet("__heartbeat__")
	if ok {
		return nil // Finish this message
	}

	action, _ := data.Get("action").String()
	if action != "test1" {
		h.t.Error("message 'action' was not correct: "+action, data)
	}
	numeric_id, _ := data.Get("numeric_id").Int()
	if numeric_id != 12345678 {
		h.t.Error("message 'numeric_id' was not correct: "+strconv.Itoa(numeric_id), data)
	}
	h.messagesReceived += 1
	if h.messagesReceived >= 4 {
		h.q.Stop()
	}
	return nil
}

func TestQueuereader(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:5150")
	q, _ := NewReader("reader_test", "ch")

	h := &MyTestHandler{t, q, 0, 0}
	q.AddHandler(h)

	// start a http client, and send in our messages
	httpclient := &http.Client{}
	endpoint := "http://127.0.0.1:5151/put?topic=reader_test"
	for i := 0; i < 2; i++ {
		body := []byte("{\"action\":\"test1\",\"numeric_id\":12345678}")
		req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
		log.Println("POST", endpoint, string(body))
		resp, err := httpclient.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		h.messagesSent += 1
		if resp != nil {
			resp.Body.Close()
			continue
		}
	}

	endpoint = "http://127.0.0.1:5151/mput?topic=reader_test"
	body := []byte("{\"action\":\"test1\",\"numeric_id\":12345678}\n{\"action\":\"test1\",\"numeric_id\":12345678}")
	req, err := http.NewRequest("POST", endpoint, bytes.NewBuffer(body))
	log.Println("POST", endpoint, string(body))
	resp, err := httpclient.Do(req)
	if err != nil {
		t.Fatalf(err.Error())
	}
	h.messagesSent += 2
	resp.Body.Close()

	err = q.ConnectToNSQ(addr)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = q.ConnectToNSQ(addr)
	if err == nil {
		t.Fatalf("should not be able to connect to the same NSQ twice")
	}

	<-q.ExitChan

	log.Println("got", h.messagesReceived, "and sent", h.messagesSent)
	if h.messagesReceived != 4 || h.messagesReceived != h.messagesSent {
		t.Fatalf("end of test. should have handled 2 messages", h.messagesReceived)
	}
}
