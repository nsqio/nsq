package nsqd

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/version"
)

func TestHTTPput(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_put" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(1))
}

func TestHTTPputEmpty(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_put_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte(""))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 500)
	equal(t, string(body), `{"status_code":500,"status_txt":"MSG_EMPTY","data":null}`)

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(0))
}

func TestHTTPmput(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_mput" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	msg := []byte("test message")
	msgs := make([][]byte, 4)
	for i := range msgs {
		msgs[i] = msg
	}
	buf := bytes.NewBuffer(bytes.Join(msgs, []byte("\n")))

	url := fmt.Sprintf("http://%s/mput?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(4))
}

func TestHTTPmputEmpty(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_mput_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	msg := []byte("test message")
	msgs := make([][]byte, 4)
	for i := range msgs {
		msgs[i] = msg
	}
	buf := bytes.NewBuffer(bytes.Join(msgs, []byte("\n")))
	_, err := buf.Write([]byte("\n"))
	equal(t, err, nil)

	url := fmt.Sprintf("http://%s/mput?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(4))
}

func TestHTTPmputBinary(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_mput_bin" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	mpub := make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	buf := bytes.NewBuffer(cmd.Body)

	url := fmt.Sprintf("http://%s/mput?topic=%s&binary=true", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(5))
}

func TestHTTPSRequire(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_put_req" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, resp.StatusCode, 403)

	httpsAddr := nsqd.httpsListener.Addr().(*net.TCPAddr)
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	equal(t, err, nil)
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		MinVersion:         0,
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client := &http.Client{Transport: transport}

	buf = bytes.NewBuffer([]byte("test message"))
	url = fmt.Sprintf("https://%s/put?topic=%s", httpsAddr, topicName)
	resp, err = client.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(1))
}

func TestHTTPSRequireVerify(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"
	_, httpAddr, nsqd := mustStartNSQD(opts)
	httpsAddr := nsqd.httpsListener.Addr().(*net.TCPAddr)

	defer nsqd.Exit()

	topicName := "test_http_put_req_verf" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	// no cert
	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, resp.StatusCode, 403)

	// unsigned cert
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	equal(t, err, nil)
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client := &http.Client{Transport: transport}

	buf = bytes.NewBuffer([]byte("test message"))
	url = fmt.Sprintf("https://%s/put?topic=%s", httpsAddr, topicName)
	resp, err = client.Post(url, "application/octet-stream", buf)
	nequal(t, err, nil)

	// signed cert
	cert, err = tls.LoadX509KeyPair("./test/certs/client.pem", "./test/certs/client.key")
	equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	transport = &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client = &http.Client{Transport: transport}

	buf = bytes.NewBuffer([]byte("test message"))
	url = fmt.Sprintf("https://%s/put?topic=%s", httpsAddr, topicName)
	resp, err = client.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(1))
}

func TestTLSRequireVerifyExceptHTTP(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"
	opts.TLSRequired = TLSRequiredExceptHTTP
	_, httpAddr, nsqd := mustStartNSQD(opts)

	defer nsqd.Exit()

	topicName := "test_http_req_verf_except_http" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	// no cert
	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	equal(t, topic.Depth(), int64(1))
}

func TestHTTPDeprecatedTopicChannel(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_topic_channel" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "ch"

	url := fmt.Sprintf("http://%s/create_topic?topic=%s", httpAddr, topicName)
	resp, err := http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	url = fmt.Sprintf("http://%s/create_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	topic, err := nsqd.GetExistingTopic(topicName)
	equal(t, err, nil)
	nequal(t, topic, nil)

	channel, err := topic.GetExistingChannel(channelName)
	equal(t, err, nil)
	nequal(t, channel, nil)

	url = fmt.Sprintf("http://%s/pause_topic?topic=%s", httpAddr, topicName)
	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	equal(t, topic.IsPaused(), true)

	url = fmt.Sprintf("http://%s/unpause_topic?topic=%s", httpAddr, topicName)
	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	equal(t, topic.IsPaused(), false)

	url = fmt.Sprintf("http://%s/pause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	equal(t, channel.IsPaused(), true)

	url = fmt.Sprintf("http://%s/unpause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	equal(t, channel.IsPaused(), false)

	url = fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	_, err = topic.GetExistingChannel(channelName)
	nequal(t, err, nil)

	url = fmt.Sprintf("http://%s/delete_topic?topic=%s", httpAddr, topicName)
	resp, err = http.Get(url)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), `{"status_code":200,"status_txt":"OK","data":null}`)

	_, err = nsqd.GetExistingTopic(topicName)
	nequal(t, err, nil)
}

func TestHTTPTransitionTopicChannel(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	client := http.Client{}
	topicName := "test_http_topic_channel1" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "ch1"

	url := fmt.Sprintf("http://%s/create_topic?topic=%s", httpAddr, topicName)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	url = fmt.Sprintf("http://%s/create_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	topic, err := nsqd.GetExistingTopic(topicName)
	equal(t, err, nil)
	nequal(t, topic, nil)

	channel, err := topic.GetExistingChannel(channelName)
	equal(t, err, nil)
	nequal(t, channel, nil)

	url = fmt.Sprintf("http://%s/pause_topic?topic=%s", httpAddr, topicName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, topic.IsPaused(), true)

	url = fmt.Sprintf("http://%s/unpause_topic?topic=%s", httpAddr, topicName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, topic.IsPaused(), false)

	url = fmt.Sprintf("http://%s/pause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, channel.IsPaused(), true)

	url = fmt.Sprintf("http://%s/unpause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, channel.IsPaused(), false)

	url = fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	_, err = topic.GetExistingChannel(channelName)
	nequal(t, err, nil)

	url = fmt.Sprintf("http://%s/delete_topic?topic=%s", httpAddr, topicName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	_, err = nsqd.GetExistingTopic(topicName)
	nequal(t, err, nil)
}

func TestHTTPV1TopicChannel(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_http_topic_channel2" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "ch2"

	url := fmt.Sprintf("http://%s/topic/create?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	topic, err := nsqd.GetExistingTopic(topicName)
	equal(t, err, nil)
	nequal(t, topic, nil)

	channel, err := topic.GetExistingChannel(channelName)
	equal(t, err, nil)
	nequal(t, channel, nil)

	url = fmt.Sprintf("http://%s/topic/pause?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, topic.IsPaused(), true)

	url = fmt.Sprintf("http://%s/topic/unpause?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, topic.IsPaused(), false)

	url = fmt.Sprintf("http://%s/channel/pause?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, channel.IsPaused(), true)

	url = fmt.Sprintf("http://%s/channel/unpause?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	equal(t, channel.IsPaused(), false)

	url = fmt.Sprintf("http://%s/channel/delete?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	_, err = topic.GetExistingChannel(channelName)
	nequal(t, err, nil)

	url = fmt.Sprintf("http://%s/topic/delete?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	equal(t, string(body), "")
	equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	_, err = nsqd.GetExistingTopic(topicName)
	nequal(t, err, nil)
}

func BenchmarkHTTPput(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, httpAddr, nsqd := mustStartNSQD(opts)
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
			for i := 0; i < num; i++ {
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

func TestHTTPgetStatusJSON(t *testing.T) {
	testTime := time.Now()
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	nsqd.startTime = testTime
	expectedJSON := fmt.Sprintf(`{"status_code":200,"status_txt":"OK","data":{"version":"%v","health":"OK","start_time":%v,"topics":[]}}`, version.Binary, testTime.Unix())
	defer nsqd.Exit()

	url := fmt.Sprintf("http://%s/stats?format=json", httpAddr)
	resp, err := http.Get(url)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 200)
	equal(t, string(body), expectedJSON)
}

func TestHTTPgetStatusText(t *testing.T) {
	testTime := time.Now()
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	nsqd.startTime = testTime
	defer nsqd.Exit()

	url := fmt.Sprintf("http://%s/stats?format=text", httpAddr)
	resp, err := http.Get(url)
	equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 200)
	nequal(t, body, nil)
}
