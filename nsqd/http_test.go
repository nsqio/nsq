package main

import (
	"bytes"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestHTTPput(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	_, httpAddr, nsqd := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	topicName := "test_http_put" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	assert.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, string(body), "OK")

	assert.Equal(t, topic.Depth(), int64(1))
}

func TestHTTPputEmpty(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	_, httpAddr, nsqd := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	topicName := "test_http_put_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte(""))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	assert.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, resp.StatusCode, 500)
	assert.Equal(t, string(body), `{"status_code":500,"status_txt":"MSG_EMPTY","data":null}`)

	assert.Equal(t, topic.Depth(), int64(0))
}

func TestHTTPmput(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	_, httpAddr, nsqd := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	topicName := "test_http_mput" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	msg := []byte("test message")
	msgs := make([][]byte, 0)
	for i := 0; i < 4; i++ {
		msgs = append(msgs, msg)
	}
	buf := bytes.NewBuffer(bytes.Join(msgs, []byte("\n")))

	url := fmt.Sprintf("http://%s/mput?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	assert.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, string(body), "OK")

	assert.Equal(t, topic.Depth(), int64(4))
}

func TestHTTPmputEmpty(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	_, httpAddr, nsqd := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	topicName := "test_http_mput_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	msg := []byte("test message")
	msgs := make([][]byte, 0)
	for i := 0; i < 4; i++ {
		msgs = append(msgs, msg)
	}
	buf := bytes.NewBuffer(bytes.Join(msgs, []byte("\n")))
	_, err := buf.Write([]byte("\n"))
	assert.Equal(t, err, nil)

	url := fmt.Sprintf("http://%s/mput?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	assert.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, string(body), "OK")

	assert.Equal(t, topic.Depth(), int64(4))
}

func TestHTTPmputBinary(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	_, httpAddr, nsqd := mustStartNSQd(NewNsqdOptions())
	defer nsqd.Exit()

	topicName := "test_http_mput_bin" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	mpub := make([][]byte, 0)
	for i := 0; i < 5; i++ {
		mpub = append(mpub, make([]byte, 100))
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	buf := bytes.NewBuffer(cmd.Body)

	url := fmt.Sprintf("http://%s/mput?topic=%s&binary=true", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	assert.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, string(body), "OK")

	assert.Equal(t, topic.Depth(), int64(5))
}

func BenchmarkHTTPput(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)
	options := NewNsqdOptions()
	options.memQueueSize = int64(b.N)
	_, httpAddr, nsqd := mustStartNSQd(options)
	msg := make([]byte, 256)
	topicName := "bench_http_put" + strconv.Itoa(int(time.Now().Unix()))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	client := &http.Client{}
	b.SetBytes(int64(len(msg)))
	b.StartTimer()

	for j := 0; j < runtime.GOMAXPROCS(0); j++ {
		wg.Add(1)
		go func() {
			num := b.N / runtime.GOMAXPROCS(0)
			for i := 0; i < num; i += 1 {
				buf := bytes.NewBuffer(msg)
				req, _ := http.NewRequest("POST", url, buf)
				resp, err := client.Do(req)
				if err != nil {
					panic(err.Error())
				}
				body, _ := ioutil.ReadAll(resp.Body)
				if !bytes.Equal(body, []byte("OK")) {
					panic("bad response")
				}
				resp.Body.Close()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	b.StopTimer()
	nsqd.Exit()
}
