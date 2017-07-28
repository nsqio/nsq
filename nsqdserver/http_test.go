package nsqdserver

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

	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/test"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
	"github.com/absolute8511/nsq/nsqlookupd"
	"github.com/absolute8511/nsq/internal/ext"
	"net/url"
	"strings"
)

func TestHTTPpub(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.LogLevel = 2
	opts.Logger = newTestLogger(t)
	//opts.Logger = &levellogger.SimpleLogger{}
	tcpAddr, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_pub" + strconv.Itoa(int(time.Now().Unix()))
	_ = nsqd.GetTopicIgnPart(topicName)
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	for {
		resp, _ := nsq.ReadResponse(conn)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, []byte("test message"), msgOut.Body)
		_, err = nsq.Finish(msgOut.ID).WriteTo(conn)
		test.Nil(t, err)
		break
	}
	conn.Close()
}

func TestHTTPPubExt(t *testing.T) {
	topicName := "test_json_header_tag_http" + strconv.Itoa(int(time.Now().Unix()))

	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqdNs, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()
	topic := nsqdNs.GetTopicIgnPart(topicName)
	topicDynConf := nsqd.TopicDynamicConf{
		AutoCommit: 1,
		SyncEvery:  1,
		Ext:        true,
	}
	topic.SetDynamicInfo(topicDynConf, nil, nil)
	topic.GetChannel("ch")

	//subscribe tag client
	conn1, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn1.Close()
	client1Params := make(map[string]interface{})
	client1Params["client_id"] = "client_w_tag"
	client1Params["hostname"] = "client_w_tag"
	client1Params["desired_tag"] = "test_tag"
	identify(t, conn1, client1Params, frameTypeResponse)
	sub(t, conn1, topicName, "ch")
	_, err = nsq.Ready(1).WriteTo(conn1)
	test.Equal(t, err, nil)

	jsonHeaderTagStr := "{\"##client_dispatch_tag\":\"test_tag\",\"##channel_filter_tag\":\"test\",\"custome_header1\":\"test_header\",\"custome_h2\":\"test\"}"
	messageBody := "test message"
	aUrl, err := url.Parse(fmt.Sprintf("http://%s/pub_ext?topic=%s&ext=%s", httpAddr, topicName, url.QueryEscape(jsonHeaderTagStr)))
	if err != nil {
		t.FailNow()
	}
	req, err := http.NewRequest("POST", aUrl.String(), strings.NewReader(messageBody))
	if err != nil {
		t.FailNow()
	}
	req.Header.Set("X-NSQEXT-Key-Test", "val-http")
	req.Header.Set("accept", "application/vnd.nsq; version=1.0")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.FailNow()
	}
	defer resp.Body.Close()
	test.Equal(t, 200, resp.StatusCode)
	msgOut := recvNextMsgAndCheckExt(t, conn1, len(messageBody), 0, true, true)
	test.NotNil(t, msgOut)
	test.Equal(t, ext.JSON_HEADER_EXT_VER, msgOut.ExtVer)
	//parse json
	var jhe map[string]interface{}
	json.Unmarshal(msgOut.ExtBytes, &jhe)
	test.Equal(t, "val-http", jhe["key-test"])

	//publish with empty ext
	jsonHeaderTagEmptyStr := ""
	aUrl, err = url.Parse(fmt.Sprintf("http://%s/pub_ext?topic=%s&ext=%s", httpAddr, topicName, url.QueryEscape(jsonHeaderTagEmptyStr)))
	if err != nil {
		t.FailNow()
	}
	req, err = http.NewRequest("POST", aUrl.String(), strings.NewReader(messageBody))
	if err != nil {
		t.FailNow()
	}
	req.Header.Set("X-NSQEXT-Key-Test", "val-http")
	req.Header.Set("accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	if err != nil {
		t.FailNow()
	}
	defer resp.Body.Close()
	test.Equal(t, resp.StatusCode, 400)

	//publish with invalid ext
	jsonHeaderTagInvalidStr := "{this is invalid json"
	aUrl, err = url.Parse(fmt.Sprintf("http://%s/pub_ext?topic=%s&ext=%s", httpAddr, topicName, url.QueryEscape(jsonHeaderTagInvalidStr)))
	if err != nil {
		t.FailNow()
	}
	req, err = http.NewRequest("POST", aUrl.String(), strings.NewReader(messageBody))
	if err != nil {
		t.FailNow()
	}
	req.Header.Set("X-NSQEXT-Key-Test", "val-http")
	req.Header.Set("accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	if err != nil {
		t.FailNow()
	}
	defer resp.Body.Close()
	test.Equal(t, resp.StatusCode, 400)
}

func TestHTTPpubpartition(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.LogLevel = 2
	opts.Logger = newTestLogger(t)
	//opts.Logger = &levellogger.SimpleLogger{}
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_pub_partition" + strconv.Itoa(int(time.Now().Unix()))
	_ = nsqd.GetTopicIgnPart(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	// should failed pub to not exist partition
	url := fmt.Sprintf("http://%s/pub?topic=%s&partition=2", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.NotEqual(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)
}

func TestHTTPpubtrace(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.LogLevel = 2
	opts.Logger = newTestLogger(t)
	//opts.Logger = &levellogger.SimpleLogger{}
	_, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_pub_trace" + strconv.Itoa(int(time.Now().Unix()))
	_ = nsqd.GetTopicIgnPart(topicName)

	buf := bytes.NewBuffer([]byte("test message"))
	rawurl := fmt.Sprintf("http://%s/pubtrace?topic=%s", httpAddr, topicName)
	resp, err := http.Post(rawurl, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, resp.StatusCode, 400)
	test.Equal(t, string(body), `{"message":"INVALID_TRACE_ID"}`)

	time.Sleep(time.Second)
	// the buffer will be drained by the http post
	// so we need refill the buffer.
	buf = bytes.NewBuffer([]byte("test message 2"))
	rawurl = fmt.Sprintf("http://%s/pubtrace?topic=%s&partition=0&trace_id=11", httpAddr, topicName)
	resp, err = http.Post(rawurl, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	test.Equal(t, resp.StatusCode, 200)

	type tmpResp struct {
		Status      string `json:"status"`
		ID          uint64 `json:"id"`
		TraceID     string `json:"trace_id"`
		QueueOffset uint64 `json:"queue_offset"`
		DataRawSize uint32 `json:"rawsize"`
	}
	var ret tmpResp
	json.Unmarshal(body, &ret)
	test.Equal(t, ret.Status, "OK")
	test.Equal(t, ret.TraceID, "11")
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

func TestHTTPFinish(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.LogLevel = 2
	opts.Logger = newTestLogger(t)
	if testing.Verbose() {
		opts.LogLevel = 4
		nsqd.SetLogger(opts.Logger)
	}

	//opts.Logger = &levellogger.SimpleLogger{}
	tcpAddr, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_http_finish" + strconv.Itoa(int(time.Now().Unix()))
	_ = nsqd.GetTopicIgnPart(topicName)
	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	buf := bytes.NewBuffer([]byte("test message"))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	resp, err := http.Post(url, "application/octet-stream", buf)
	test.Equal(t, err, nil)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	test.Equal(t, string(body), "OK")

	time.Sleep(5 * time.Millisecond)

	_, err = nsq.Ready(1).WriteTo(conn)
	test.Equal(t, err, nil)
	// sleep to allow the RDY state to take effect
	time.Sleep(50 * time.Millisecond)

	for {
		resp, _ := nsq.ReadResponse(conn)
		frameType, data, err := nsq.UnpackResponse(resp)
		test.Nil(t, err)
		test.NotEqual(t, frameTypeError, frameType)
		if frameType == frameTypeResponse {
			t.Logf("got response data: %v", string(data))
			continue
		}
		msgOut, err := nsq.DecodeMessage(data)
		test.Equal(t, []byte("test message"), msgOut.Body)

		url := fmt.Sprintf("http://%s/message/finish?topic=%s&partition=0&channel=%s&msgid=%d", httpAddr,
			topicName, "ch", nsq.GetNewMessageID(msgOut.ID[:]))
		ret, err := http.Post(url, "", nil)
		test.Equal(t, err, nil)
		test.Equal(t, 200, ret.StatusCode)
		ioutil.ReadAll(ret.Body)
		ret.Body.Close()
		break
	}
	conn.Close()
}

func TestHTTPSRequire(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(t)
	//opts.LogLevel = 2
	//opts.Logger = &levellogger.SimpleLogger{}
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
	topicPart := 0
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

	topic, err := nsqd.GetExistingTopic(topicName, topicPart)
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

	nsqd.DeleteExistingTopic(topicName, topicPart)
	_, err = nsqd.GetExistingTopic(topicName, topicPart)
	test.NotNil(t, err)
}

func BenchmarkHTTPpub(b *testing.B) {
	var wg sync.WaitGroup
	b.StopTimer()
	opts := nsqd.NewOptions()
	opts.Logger = newTestLogger(b)
	opts.MemQueueSize = int64(b.N)
	_, httpAddr, nsqdData, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	msg := make([]byte, 256)
	topicName := "bench_http_pub" + strconv.Itoa(int(time.Now().Unix()))
	url := fmt.Sprintf("http://%s/pub?topic=%s", httpAddr, topicName)
	client := &http.Client{}
	nsqdData.GetTopic(topicName, 0)
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
					panic("bad response:" + string(body))
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
	_, _, lookupd2 := mustStartNSQLookupd(lopts)
	lookupd1.Main()
	lookupd2.Main()
	defer lookupd1.Exit()
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

func TestNSQDStatsFilter(t *testing.T) {

	t.Logf("Starts nsqd...")
	nsqdOpts := nsqd.NewOptions()

	nsqdOpts.Logger = newTestLogger(t)
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	nsqdOpts.DataPath = tmpDir

	_, nsqdHTTPAddr, nsqd, _ := mustStartNSQD(nsqdOpts)
	t.Logf("nsqd started")

	//delete existing topics if there is any
	topicCnt := 0
	for _, topic := range nsqd.GetStats(false) {
		parNum, _ := strconv.Atoi(topic.TopicPartition)
		nsqd.DeleteExistingTopic(topic.TopicName, parNum)
		topicCnt++
	}
	t.Logf("%d topic(s) deleted.", topicCnt)
	topicName := fmt.Sprintf("test_nsqd_stats_filter%d", time.Now().UnixNano())
	nsqd.GetTopic(topicName, 0)
	defer nsqd.DeleteExistingTopic(topicName, 0)

	topicNameAnother := fmt.Sprintf("test_nsqd_stats_filter_another%d", time.Now().UnixNano())
	nsqd.GetTopic(topicNameAnother, 0)
	topic_another, err := nsqd.GetExistingTopic(topicNameAnother, 0)
	topic_another.DisableForSlave()
	defer nsqd.DeleteExistingTopic(topicNameAnother, 0)

	time.Sleep(500 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/stats?format=json", nsqdHTTPAddr)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	resp, err := client.Do(req)
	test.Assert(t, err == nil, "error in first response.")
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	type NSQDStats struct {
		TopicStats []map[string]interface{} `json:"topics"`
		Version    string                   `json:"version"`
		Health     string                   `json:"health"`
		StartTime  int64                    `json:"start_time"`
	}

	var stats NSQDStats
	json.Unmarshal(body, &stats)
	t.Logf("topic len in first response: %d", len(stats.TopicStats))
	test.Assert(t, len(stats.TopicStats) == 2, "topic number does not match.")

	url = fmt.Sprintf("http://%s/stats?format=json&leaderOnly=true", nsqdHTTPAddr)
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Assert(t, err == nil, "error in second response.")
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	var statFiltered NSQDStats
	json.Unmarshal(body, &statFiltered)
	t.Logf("topic len: %d", len(statFiltered.TopicStats))
	test.Assert(t, len(statFiltered.TopicStats) == 1, "topic number is not empty.")
}

func BenchmarkFetchNodeHourlyPubsize(b *testing.B) {
	b.StopTimer()
	lgr := newTestLogger(b)

	nsqdOpts := nsqd.NewOptions()
	nsqdOpts.TCPAddress = "127.0.0.1:0"
	nsqdOpts.HTTPAddress = "127.0.0.1:0"
	nsqdOpts.BroadcastAddress = "127.0.0.1"
	nsqdOpts.Logger = lgr

	_, _, nsqd1, nsqd1Srv := mustStartNSQD(nsqdOpts)
	b.Logf("nsqd started")

	time.Sleep(100 * time.Millisecond)

	var topicNames []string
	for i := 0; i < 1000; i++ {
		//create sample topics
		topicName := fmt.Sprintf("Topic-Benchmark%04d", i)
		nsqd1.GetTopic(topicName, 0)
		topicNames = append(topicNames, topicName)
		b.Logf("Topic %s created.", topicName)
	}

	type TopicHourlyPubsizeStat struct {
		TopicName      string `json:"topic_name"`
		TopicPartition string `json:"topic_partition"`
		HourlyPubsize  int64  `json:"hourly_pub_size"`
	}

	type NodeHourlyPubsizeStats struct {
		TopicHourlyPubsizeList []*TopicHourlyPubsizeStat `json:"node_hourly_pub_size_stats"`
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		endpoint := fmt.Sprintf("http://%s/message/historystats", nsqd1Srv.ctx.httpAddr.String())
		req, err := http.NewRequest("GET", endpoint, nil)
		if err != nil {
			b.Errorf("Fail to get message history from %s - %s", endpoint, err)
			b.Fail()
		}
		client := http.Client{}
		resp, _ := client.Do(req)
		var nodeHourlyPubsizeStats NodeHourlyPubsizeStats
		body, _ := ioutil.ReadAll(resp.Body)
		json.Unmarshal(body, &nodeHourlyPubsizeStats)
		resp.Body.Close()
		b.Logf("Node topics hourly pub size list: %d", len(nodeHourlyPubsizeStats.TopicHourlyPubsizeList))
	}
	b.StopTimer()

	//cleanup
	for _, topicName := range topicNames {
		err := nsqd1.DeleteExistingTopic(topicName, 0)
		if err != nil {
			b.Logf("Fail to delete topic: %s", topicName)
		}
	}
}
