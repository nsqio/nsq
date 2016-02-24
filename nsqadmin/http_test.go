package nsqadmin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
	"github.com/nsqio/nsq/nsqlookupd"
)

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) logger {
	return &testLogger{tbl}
}

func assert(t *testing.T, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d: "+msg+"\033[39m\n\n",
			append([]interface{}{filepath.Base(file), line}, v...)...)
		t.FailNow()
	}
}

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func nequal(t *testing.T, act, exp interface{}) {
	if reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func bootstrapNSQCluster(t *testing.T) (string, []*nsqd.NSQD, []*nsqlookupd.NSQLookupd, *NSQAdmin) {
	lgr := newTestLogger(t)

	nsqlookupdOpts := nsqlookupd.NewOptions()
	nsqlookupdOpts.TCPAddress = "127.0.0.1:0"
	nsqlookupdOpts.HTTPAddress = "127.0.0.1:0"
	nsqlookupdOpts.BroadcastAddress = "127.0.0.1"
	nsqlookupdOpts.Logger = lgr
	nsqlookupd1 := nsqlookupd.New(nsqlookupdOpts)
	go nsqlookupd1.Main()

	time.Sleep(100 * time.Millisecond)

	nsqdOpts := nsqd.NewOptions()
	nsqdOpts.TCPAddress = "127.0.0.1:0"
	nsqdOpts.HTTPAddress = "127.0.0.1:0"
	nsqdOpts.BroadcastAddress = "127.0.0.1"
	nsqdOpts.NSQLookupdTCPAddresses = []string{nsqlookupd1.RealTCPAddr().String()}
	nsqdOpts.Logger = lgr
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	nsqdOpts.DataPath = tmpDir
	nsqd1 := nsqd.New(nsqdOpts)
	go nsqd1.Main()

	nsqadminOpts := NewOptions()
	nsqadminOpts.HTTPAddress = "127.0.0.1:0"
	nsqadminOpts.NSQLookupdHTTPAddresses = []string{nsqlookupd1.RealHTTPAddr().String()}
	nsqadminOpts.Logger = lgr
	nsqadmin1 := New(nsqadminOpts)
	go nsqadmin1.Main()

	time.Sleep(100 * time.Millisecond)

	return tmpDir, []*nsqd.NSQD{nsqd1}, []*nsqlookupd.NSQLookupd{nsqlookupd1}, nsqadmin1
}

func TestHTTPTopicsGET(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_topics_get" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetTopic(topicName)
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics", nsqadmin1.RealHTTPAddr())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	js, err := simplejson.NewJson(body)
	equal(t, err, nil)
	equal(t, len(js.Get("topics").MustArray()), 1)
	equal(t, js.Get("topics").GetIndex(0).MustString(), topicName)
}

func TestHTTPTopicGET(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_topic_get" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetTopic(topicName)
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	js, err := simplejson.NewJson(body)
	equal(t, err, nil)
	t.Logf("%s", body)
	equal(t, js.Get("topic_name").MustString(), topicName)
	equal(t, js.Get("depth").MustInt(), 0)
	equal(t, js.Get("memory_depth").MustInt(), 0)
	equal(t, js.Get("backend_depth").MustInt(), 0)
	equal(t, js.Get("message_count").MustInt(), 0)
	equal(t, js.Get("paused").MustBool(), false)
}

func TestHTTPNodesGET(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/nodes", nsqadmin1.RealHTTPAddr())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	hostname, _ := os.Hostname()

	js, err := simplejson.NewJson(body)
	equal(t, err, nil)
	equal(t, len(js.Get("nodes").MustArray()), 1)
	testNode := js.Get("nodes").GetIndex(0)
	equal(t, testNode.Get("hostname").MustString(), hostname)
	equal(t, testNode.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, testNode.Get("tcp_port").MustInt(), nsqds[0].RealTCPAddr().Port)
	equal(t, testNode.Get("http_port").MustInt(), nsqds[0].RealHTTPAddr().Port)
	equal(t, testNode.Get("version").MustString(), version.Binary)
	equal(t, len(testNode.Get("topics").MustArray()), 0)
}

func TestHTTPChannelGET(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_channel_get" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	js, err := simplejson.NewJson(body)
	equal(t, err, nil)
	equal(t, js.Get("topic_name").MustString(), topicName)
	equal(t, js.Get("channel_name").MustString(), "ch")
	equal(t, js.Get("depth").MustInt(), 0)
	equal(t, js.Get("memory_depth").MustInt(), 0)
	equal(t, js.Get("backend_depth").MustInt(), 0)
	equal(t, js.Get("message_count").MustInt(), 0)
	equal(t, js.Get("paused").MustBool(), false)
	equal(t, js.Get("in_flight_count").MustInt(), 0)
	equal(t, js.Get("deferred_count").MustInt(), 0)
	equal(t, js.Get("requeue_count").MustInt(), 0)
	equal(t, js.Get("timeout_count").MustInt(), 0)
	equal(t, len(js.Get("clients").MustArray()), 0)
}

func TestHTTPNodesSingleGET(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_nodes_single_get" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/nodes/%s", nsqadmin1.RealHTTPAddr(),
		nsqds[0].RealHTTPAddr().String())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	js, err := simplejson.NewJson(body)
	equal(t, err, nil)
	equal(t, js.Get("node").MustString(), nsqds[0].RealHTTPAddr().String())
	equal(t, len(js.Get("topics").MustArray()), 1)
	testTopic := js.Get("topics").GetIndex(0)
	equal(t, testTopic.Get("topic_name").MustString(), topicName)
	equal(t, testTopic.Get("depth").MustInt(), 0)
	equal(t, testTopic.Get("memory_depth").MustInt(), 0)
	equal(t, testTopic.Get("backend_depth").MustInt(), 0)
	equal(t, testTopic.Get("message_count").MustInt(), 0)
	equal(t, testTopic.Get("paused").MustBool(), false)
	equal(t, testTopic.Get("in_flight_count").MustInt(), 0)
	equal(t, testTopic.Get("deferred_count").MustInt(), 0)
	equal(t, testTopic.Get("requeue_count").MustInt(), 0)
	equal(t, testTopic.Get("timeout_count").MustInt(), 0)
}

func TestHTTPCreateTopicPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	time.Sleep(100 * time.Millisecond)

	topicName := "test_create_topic_post" + strconv.Itoa(int(time.Now().Unix()))

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics", nsqadmin1.RealHTTPAddr())
	body, _ := json.Marshal(map[string]interface{}{
		"topic": topicName,
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPCreateTopicChannelPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	time.Sleep(100 * time.Millisecond)

	topicName := "test_create_topic_channel_post" + strconv.Itoa(int(time.Now().Unix()))

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics", nsqadmin1.RealHTTPAddr())
	body, _ := json.Marshal(map[string]interface{}{
		"topic":   topicName,
		"channel": "ch",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPTombstoneTopicNodePOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_tombstone_topic_node_post" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetTopic(topicName)
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/nodes/%s", nsqadmin1.RealHTTPAddr(), nsqds[0].RealHTTPAddr())
	body, _ := json.Marshal(map[string]interface{}{
		"topic": topicName,
	})
	req, _ := http.NewRequest("DELETE", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPDeleteTopicPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_delete_topic_post" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetTopic(topicName)
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	req, _ := http.NewRequest("DELETE", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPDeleteChannelPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_delete_channel_post" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	req, _ := http.NewRequest("DELETE", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPPauseTopicPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_pause_topic_post" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetTopic(topicName)
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ := json.Marshal(map[string]interface{}{
		"action": "pause",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	body, _ = ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()

	url = fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ = json.Marshal(map[string]interface{}{
		"action": "unpause",
	})
	req, _ = http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPPauseChannelPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_pause_channel_post" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetTopic(topicName)
	topic.GetChannel("ch")
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ := json.Marshal(map[string]interface{}{
		"action": "pause",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	body, _ = ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()

	url = fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ = json.Marshal(map[string]interface{}{
		"action": "unpause",
	})
	req, _ = http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPEmptyTopicPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_empty_topic_post" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetTopic(topicName)
	topic.PutMessage(nsqd.NewMessage(nsqd.MessageID{}, []byte("1234")))
	equal(t, topic.Depth(), int64(1))
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ := json.Marshal(map[string]interface{}{
		"action": "empty",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	body, _ = ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()

	equal(t, topic.Depth(), int64(0))
}

func TestHTTPEmptyChannelPOST(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_empty_channel_post" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetTopic(topicName)
	channel := topic.GetChannel("ch")
	channel.PutMessage(nsqd.NewMessage(nsqd.MessageID{}, []byte("1234")))

	time.Sleep(100 * time.Millisecond)
	equal(t, channel.Depth(), int64(1))

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ := json.Marshal(map[string]interface{}{
		"action": "empty",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	body, _ = ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()

	equal(t, channel.Depth(), int64(0))
}
