package nsqdserver

import (
	"bytes"
	"crypto/tls"
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

	"github.com/absolute8511/nsq/internal/test"
	"github.com/absolute8511/nsq/nsqd"
	"github.com/absolute8511/nsq/nsqlookupd"
	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

func TestHTTPpub(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.LogLevel = 2
	opts.Logger = newTestLogger(t)
	//opts.Logger = &levellogger.GLogger{}
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_pub" + strconv.Itoa(int(time.Now().Unix()))
	_ = nsqd.GetTopicIgnPart(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

}

func TestHTTPpubEmpty(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_pub_empty" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName)

	buf := bytes.NewBuffer([]byte(""))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, resp.StatusCode, 500)
	test.Equal(t, string(body), `{"status_code":500,"status_txt":"MSG_EMPTY","data":null}`)

	time.Sleep(5 * time.Millisecond)

}

func TestHTTPmpub(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_mpub" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName)

	msg := []byte("test message")
	msgs := make([][]byte, 4)
	for i := range msgs {
		msgs[i] = msg
	}
	buf := bytes.NewBuffer(bytes.Join(msgs, []byte("\n")))

	url := fmt.Sprintf("http://%s/mpub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

}

func TestHTTPmpubEmpty(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_mpub_empty" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName)

	msg := []byte("test message")
	msgs := make([][]byte, 4)
	for i := range msgs {
		msgs[i] = msg
	}
	buf := bytes.NewBuffer(bytes.Join(msgs, []byte("\n")))
	_, err := buf.Write([]byte("\n"))
	test.Equal(t, err, nil)

	url := fmt.Sprintf("http://%s/mpub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

}

func TestHTTPmpubBinary(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_mpub_bin" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName)

	mpub := make([][]byte, 5)
	for i := range mpub {
		mpub[i] = make([]byte, 100)
	}
	cmd, _ := nsq.MultiPublish(topicName, mpub)
	buf := bytes.NewBuffer(cmd.Body)

	url := fmt.Sprintf("http://%s/mpub?topic=%s&binary=true", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

}

func TestHTTPSRequire(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	//opts.LogLevel = 2
	//opts.Logger = &levellogger.GLogger{}
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSClientAuthPolicy = "require"
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_pub_req" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, resp.StatusCode, 403)

	httpsAddr := nsqdServer.httpsListener.Addr().(*net.TCPAddr)
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Equal(t, err, nil)
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
	url = fmt.Sprintf("https://%s/pub?topic=%s", httpsAddr, topicName)
	resp, err = client.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

}

func TestHTTPSRequireVerify(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	httpsAddr := nsqdServer.httpsListener.Addr().(*net.TCPAddr)
	topicName := "test_http_pub_req_verf" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName)

	// no cert
	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, resp.StatusCode, 403)

	// unsigned cert
	cert, err := tls.LoadX509KeyPair("./test/certs/cert.pem", "./test/certs/key.pem")
	test.Equal(t, err, nil)
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client := &http.Client{Transport: transport}

	buf = bytes.NewBuffer([]byte("test message"))
	url = fmt.Sprintf("https://%s/pub?topic=%s", httpsAddr, topicName)
	resp, err = client.Post(url, "application/octet-stream", buf)
	test.NotNil(t, err)

	// signed cert
	cert, err = tls.LoadX509KeyPair("./test/certs/client.pem", "./test/certs/client.key")
	test.Equal(t, err, nil)
	tlsConfig = &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
	}
	transport = &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client = &http.Client{Transport: transport}

	buf = bytes.NewBuffer([]byte("test message"))
	url = fmt.Sprintf("https://%s/pub?topic=%s", httpsAddr, topicName)
	resp, err = client.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

}

func TestTLSRequireVerifyExceptHTTP(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.TLSCert = "./test/certs/server.pem"
	opts.TLSKey = "./test/certs/server.key"
	opts.TLSRootCAFile = "./test/certs/ca.pem"
	opts.TLSClientAuthPolicy = "require-verify"
	opts.TLSRequired = TLSRequiredExceptHTTP
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_req_verf_except_http" + strconv.Itoa(int(time.Now().Unix()))
	nsqd.GetTopicIgnPart(topicName)

	// no cert
	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

}

func TestHTTPV1TopicChannel(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_topic_channel2" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "ch2"

	nsqd.GetTopicIgnPart(topicName)

	url := fmt.Sprintf("http://%s/channel/create?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err := http.Post(url, "application/json", nil)
	test.Equal(t, err, nil)
	test.Equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, string(body), "")
	test.Equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	topic, err := nsqd.GetExistingTopic(topicName)
	test.Equal(t, err, nil)
	test.NotNil(t, topic)

	channel, err := topic.GetExistingChannel(channelName)
	test.Equal(t, err, nil)
	test.NotNil(t, channel)

	url = fmt.Sprintf("http://%s/channel/pause?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Equal(t, err, nil)
	test.Equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, string(body), "")
	test.Equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	test.Equal(t, channel.IsPaused(), true)

	url = fmt.Sprintf("http://%s/channel/unpause?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Equal(t, err, nil)
	test.Equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, string(body), "")
	test.Equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	test.Equal(t, channel.IsPaused(), false)

	url = fmt.Sprintf("http://%s/channel/delete?topic=%s&channel=%s", httpAddr, topicName, channelName)
	resp, err = http.Post(url, "application/json", nil)
	test.Equal(t, err, nil)
	test.Equal(t, resp.StatusCode, 200)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, string(body), "")
	test.Equal(t, resp.Header.Get("X-NSQ-Content-Type"), "nsq; version=1.0")

	_, err = topic.GetExistingChannel(channelName)
	test.NotNil(t, err)

	nsqd.DeleteExistingTopic(topicName)
	_, err = nsqd.GetExistingTopic(topicName)
	test.NotNil(t, err)
}

func BenchmarkHTTPpub(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, httpAddr, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, 256)
	topicName := "bench_http_pub" + strconv.Itoa(int(time.Now().Unix()))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
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
	nsqdServer.Exit()
}

func TestHTTPgetStatusJSON(t *testing.T) {
	testTime := time.Now()
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	testTime = nsqd.GetStartTime()
	expectedJSON := fmt.Sprintf(`{"status_code":200,"status_txt":"OK","data":{"version":"%v","health":"OK","start_time":%v,"topics":[]}}`, version.Binary, testTime.Unix())

	url := fmt.Sprintf("http://%s/stats?format=json", httpAddr)
	resp, err := http.Get(url)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, resp.StatusCode, 200)
	test.Equal(t, string(body), expectedJSON)
}

func TestHTTPgetStatusText(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	url := fmt.Sprintf("http://%s/stats?format=text", httpAddr)
	resp, err := http.Get(url)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, resp.StatusCode, 200)
	test.NotNil(t, body)
}

func TestHTTPconfig(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = newTestLogger(t)
	_, _, lookupd1 := mustStartNSQLookupd(lopts)
	defer lookupd1.Exit()
	_, _, lookupd2 := mustStartNSQLookupd(lopts)
	defer lookupd2.Exit()

	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	url := fmt.Sprintf("http://%s/config/nsqlookupd_tcp_addresses", httpAddr)
	resp, err := http.Get(url)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, resp.StatusCode, 200)
	test.Equal(t, string(body), "[]")

	client := http.Client{}
	addrs := fmt.Sprintf(`["%s","%s"]`, lookupd1.RealTCPAddr().String(), lookupd2.RealTCPAddr().String())
	url = fmt.Sprintf("http://%s/config/nsqlookupd_tcp_addresses", httpAddr)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(addrs)))
	test.Equal(t, err, nil)
	resp, err = client.Do(req)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	test.Equal(t, resp.StatusCode, 200)
	test.Equal(t, string(body), addrs)
}

func TestHTTPerrors(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	_, httpAddr, _, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	url := fmt.Sprintf("http://%s/stats", httpAddr)
	resp, err := http.Post(url, "text/plain", nil)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, resp.StatusCode, 405)
	test.Equal(t, string(body), `{"message":"METHOD_NOT_ALLOWED"}`)

	url = fmt.Sprintf("http://%s/not_found", httpAddr)
	resp, err = http.Get(url)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	test.Equal(t, resp.StatusCode, 404)
	test.Equal(t, string(body), `{"message":"NOT_FOUND"}`)
}
