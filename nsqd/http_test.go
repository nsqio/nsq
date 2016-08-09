package nsqd

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqlookupd"
)

type ErrMessage struct {
	Message string `json:"message"`
}

type Info struct {
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
	Hostname         string `json:"hostname"`
	HTTPPort         int    `json:"http_port"`
	TCPPort          int    `json:"tcp_port"`
	StartTime        int64  `json:"start_time"`
}

type InfoDoc struct {
	Code   int    `json:"status_code"`
	Status string `json:"status_txt"`
	Data   *Info  `json:"data"`
}

func TestHTTPput(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_http_put" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(1), topic.Depth())
}

func TestHTTPputEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_http_put_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte(""))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, 500, resp.StatusCode)
	test.Equal(t, `{"status_code":500,"status_txt":"MSG_EMPTY","data":null}`, string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(0), topic.Depth())
}

func TestHTTPmput(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(4), topic.Depth())
}

func TestHTTPmputEmpty(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	test.Nil(t, err)

	url := fmt.Sprintf("http://%s/mput?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(4), topic.Depth())
}

func TestHTTPmputBinary(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(5), topic.Depth())
}

func TestHTTPpubDefer(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_http_pub_defer" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	ch := topic.GetChannel("ch")

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/pub?topic=%s&defer=%d", httpAddr, topicName, 1000)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	ch.deferredMutex.Lock()
	numDef := len(ch.deferredMessages)
	ch.deferredMutex.Unlock()
	test.Equal(t, 1, numDef)
}

func TestHTTPSRequire(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_http_put_req" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, 403, resp.StatusCode)

	httpsAddr := nsqd.httpsListener.Addr().(*net.TCPAddr)
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Nil(t, err)
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
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(1), topic.Depth())
}

func TestHTTPSRequireVerify(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	httpsAddr := nsqd.httpsListener.Addr().(*net.TCPAddr)
	topicName := "test_http_put_req_verf" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	// no cert
	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, 403, resp.StatusCode)

	// unsigned cert
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Nil(t, err)
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
	test.NotNil(t, err)

	// signed cert
	cert, err = tls.LoadX509KeyPair("./test/certs/client.pem", "./test/certs/client.key")
	test.Nil(t, err)
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
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(1), topic.Depth())
}

func TestTLSRequireVerifyExceptHTTP(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"
	opts.TLSRequired = TLSRequiredExceptHTTP
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_http_req_verf_except_http" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)

	// no cert
	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/put?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, "OK", string(body))

	time.Sleep(5 * time.Millisecond)

	test.Equal(t, int64(1), topic.Depth())
}

func TestHTTPDeprecatedTopicChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_http_topic_channel" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "ch"

	url := fmt.Sprintf("http://%s/create_topic?topic=%s", httpAddr, topicName)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	url = fmt.Sprintf("http://%s/create_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	topic, err := nsqd.GetExistingTopic(topicName)
	test.Nil(t, err)
	test.NotNil(t, topic)

	channel, err := topic.GetExistingChannel(channelName)
	test.Nil(t, err)
	test.NotNil(t, channel)

	url = fmt.Sprintf("http://%s/pause_topic?topic=%s", httpAddr, topicName)
	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	test.Equal(t, true, topic.IsPaused())

	url = fmt.Sprintf("http://%s/unpause_topic?topic=%s", httpAddr, topicName)
	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	test.Equal(t, false, topic.IsPaused())

	url = fmt.Sprintf("http://%s/pause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	test.Equal(t, true, channel.IsPaused())

	url = fmt.Sprintf("http://%s/unpause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	test.Equal(t, false, channel.IsPaused())

	url = fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	_, err = topic.GetExistingChannel(channelName)
	test.NotNil(t, err)

	url = fmt.Sprintf("http://%s/delete_topic?topic=%s", httpAddr, topicName)
	resp, err = http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, `{"status_code":200,"status_txt":"OK","data":null}`, string(body))

	_, err = nsqd.GetExistingTopic(topicName)
	test.NotNil(t, err)
}

func TestHTTPTransitionTopicChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	client := http.Client{}
	topicName := "test_http_topic_channel1" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "ch1"

	url := fmt.Sprintf("http://%s/create_topic?topic=%s", httpAddr, topicName)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	url = fmt.Sprintf("http://%s/create_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	topic, err := nsqd.GetExistingTopic(topicName)
	test.Nil(t, err)
	test.NotNil(t, topic)

	channel, err := topic.GetExistingChannel(channelName)
	test.Nil(t, err)
	test.NotNil(t, channel)

	url = fmt.Sprintf("http://%s/pause_topic?topic=%s", httpAddr, topicName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, true, topic.IsPaused())

	url = fmt.Sprintf("http://%s/unpause_topic?topic=%s", httpAddr, topicName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, false, topic.IsPaused())

	url = fmt.Sprintf("http://%s/pause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, true, channel.IsPaused())

	url = fmt.Sprintf("http://%s/unpause_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, false, channel.IsPaused())

	url = fmt.Sprintf("http://%s/delete_channel?topic=%s&channel=%s", httpAddr, topicName, channelName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	_, err = topic.GetExistingChannel(channelName)
	test.NotNil(t, err)

	url = fmt.Sprintf("http://%s/delete_topic?topic=%s", httpAddr, topicName)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	_, err = nsqd.GetExistingTopic(topicName)
	test.NotNil(t, err)
}

func TestHTTPV1TopicChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_http_topic_channel2" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "ch2"

	url := fmt.Sprintf("http://%s/topic/create?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	topic, err := nsqd.GetExistingTopic(topicName)
	test.Nil(t, err)
	test.NotNil(t, topic)

	channel, err := topic.GetExistingChannel(channelName)
	test.Nil(t, err)
	test.NotNil(t, channel)

	em := ErrMessage{}

	url = fmt.Sprintf("http://%s/topic/pause", httpAddr)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	url = fmt.Sprintf("http://%s/topic/pause?topic=%s", httpAddr, topicName+"abc")
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 404, resp.StatusCode)
	test.Equal(t, "Not Found", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "TOPIC_NOT_FOUND", em.Message)

	url = fmt.Sprintf("http://%s/topic/pause?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, true, topic.IsPaused())

	url = fmt.Sprintf("http://%s/topic/unpause?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, false, topic.IsPaused())

	url = fmt.Sprintf("http://%s/channel/pause?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, true, channel.IsPaused())

	url = fmt.Sprintf("http://%s/channel/unpause?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	test.Equal(t, false, channel.IsPaused())

	url = fmt.Sprintf("http://%s/channel/delete?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	_, err = topic.GetExistingChannel(channelName)
	test.NotNil(t, err)

	url = fmt.Sprintf("http://%s/topic/delete?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, "", string(body))
	test.Equal(t, "nsq; version=1.0", resp.Header.Get("X-NSQ-Content-Type"))

	_, err = nsqd.GetExistingTopic(topicName)
	test.NotNil(t, err)
}

func BenchmarkHTTPput(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
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
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	nsqd.startTime = testTime
	expectedJSON := fmt.Sprintf(`{"status_code":200,"status_txt":"OK","data":{"version":"%v","health":"OK","start_time":%v,"topics":[]}}`, version.Binary, testTime.Unix())

	url := fmt.Sprintf("http://%s/stats?format=json", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, expectedJSON, string(body))
}

func TestHTTPgetStatusText(t *testing.T) {
	testTime := time.Now()
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	nsqd.startTime = testTime

	url := fmt.Sprintf("http://%s/stats?format=text", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	test.NotNil(t, body)
}

func TestHTTPconfig(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = test.NewTestLogger(t)
	_, _, lookupd1 := mustStartNSQLookupd(lopts)
	defer lookupd1.Exit()
	_, _, lookupd2 := mustStartNSQLookupd(lopts)
	defer lookupd2.Exit()

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	url := fmt.Sprintf("http://%s/config/nsqlookupd_tcp_addresses", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, "[]", string(body))

	client := http.Client{}
	addrs := fmt.Sprintf(`["%s","%s"]`, lookupd1.RealTCPAddr().String(), lookupd2.RealTCPAddr().String())
	url = fmt.Sprintf("http://%s/config/nsqlookupd_tcp_addresses", httpAddr)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(addrs)))
	test.Nil(t, err)
	resp, err = client.Do(req)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, addrs, string(body))
}

func TestHTTPerrors(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	url := fmt.Sprintf("http://%s/stats", httpAddr)
	resp, err := http.Post(url, "text/plain", nil)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, 405, resp.StatusCode)
	test.Equal(t, `{"message":"METHOD_NOT_ALLOWED"}`, string(body))

	url = fmt.Sprintf("http://%s/not_found", httpAddr)
	resp, err = http.Get(url)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	test.Equal(t, 404, resp.StatusCode)
	test.Equal(t, `{"message":"NOT_FOUND"}`, string(body))
}

func TestDeleteTopic(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	em := ErrMessage{}

	url := fmt.Sprintf("http://%s/topic/delete", httpAddr)
	resp, err := http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	topicName := "test_http_delete_topic" + strconv.Itoa(int(time.Now().Unix()))

	url = fmt.Sprintf("http://%s/topic/delete?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 404, resp.StatusCode)
	test.Equal(t, "Not Found", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "TOPIC_NOT_FOUND", em.Message)

	nsqd.GetTopic(topicName)

	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)
}

func TestEmptyTopic(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	em := ErrMessage{}

	url := fmt.Sprintf("http://%s/topic/empty", httpAddr)
	resp, err := http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	topicName := "test_http_empty_topic" + strconv.Itoa(int(time.Now().Unix()))

	url = fmt.Sprintf("http://%s/topic/empty?topic=%s", httpAddr, topicName+"$")
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "INVALID_TOPIC", em.Message)

	url = fmt.Sprintf("http://%s/topic/empty?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 404, resp.StatusCode)
	test.Equal(t, "Not Found", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "TOPIC_NOT_FOUND", em.Message)

	nsqd.GetTopic(topicName)

	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)
}

func TestEmptyChannel(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	em := ErrMessage{}

	url := fmt.Sprintf("http://%s/channel/empty", httpAddr)
	resp, err := http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	topicName := "test_http_empty_channel" + strconv.Itoa(int(time.Now().Unix()))

	url = fmt.Sprintf("http://%s/channel/empty?topic=%s", httpAddr, topicName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_CHANNEL", em.Message)

	channelName := "ch"

	url = fmt.Sprintf("http://%s/channel/empty?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 404, resp.StatusCode)
	test.Equal(t, "Not Found", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "TOPIC_NOT_FOUND", em.Message)

	topic := nsqd.GetTopic(topicName)

	url = fmt.Sprintf("http://%s/channel/empty?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 404, resp.StatusCode)
	test.Equal(t, "Not Found", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "CHANNEL_NOT_FOUND", em.Message)

	topic.GetChannel(channelName)

	resp, err = http.Post(url, "application/json", nil)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)
}

func TestInfo(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	info := InfoDoc{}

	url := fmt.Sprintf("http://%s/info", httpAddr)
	resp, err := http.Get(url)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &info)
	test.Nil(t, err)
	test.Equal(t, version.Binary, info.Data.Version)

}
