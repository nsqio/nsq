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

	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
	"github.com/absolute8511/nsq/nsqdserver"
	"github.com/absolute8511/nsq/nsqlookupd"
	"github.com/bitly/go-simplejson"
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

func (tl *testLogger) OutputWarning(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}
func (tl *testLogger) OutputErr(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) *testLogger {
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

func bootstrapNSQCluster(t *testing.T) (string, []*nsqd.NSQD, []*nsqdserver.NsqdServer, []*nsqlookupd.NSQLookupd, *NSQAdmin) {
	lgr := newTestLogger(t)

	nsqlookupdOpts := nsqlookupd.NewOptions()
	nsqlookupdOpts.TCPAddress = "127.0.0.1:0"
	nsqlookupdOpts.HTTPAddress = "127.0.0.1:0"
	nsqlookupdOpts.BroadcastAddress = "127.0.0.1"
	nsqlookupdOpts.Logger = lgr
	nsqlookupd.SetLogger(nsqlookupdOpts.Logger, nsqlookupdOpts.LogLevel)
	nsqlookupd1 := nsqlookupd.New(nsqlookupdOpts)
	go nsqlookupd1.Main()

	// wait http server
	time.Sleep(time.Second)
	if nsqlookupd1.RealHTTPAddr().String() == "" {
		t.Fatal("lookupd should not empty")
	}

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
	nsqd1, nsqd1Server := nsqdserver.NewNsqdServer(nsqdOpts)
	go nsqd1Server.Main()
	t.Log("nsqd started")

	nsqadminOpts := NewOptions()
	nsqadminOpts.HTTPAddress = "127.0.0.1:0"
	nsqadminOpts.NSQLookupdHTTPAddresses = []string{nsqlookupd1.RealHTTPAddr().String()}
	nsqadminOpts.Logger = lgr
	nsqadmin1 := New(nsqadminOpts)
	go nsqadmin1.Main()

	time.Sleep(100 * time.Millisecond)

	return tmpDir, []*nsqd.NSQD{nsqd1}, []*nsqdserver.NsqdServer{nsqd1Server}, []*nsqlookupd.NSQLookupd{nsqlookupd1}, nsqadmin1
}

func TestHTTPTopicsGET(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_topics_get" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
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
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_topic_get" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
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
	equal(t, js.Get("skipped").MustBool(), false)
}

func TestHTTPNodesGET(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
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
	equal(t, testNode.Get("version").MustString(), version.Binary)
	equal(t, len(testNode.Get("topics").MustArray()), 0)
}

func TestHTTPChannelGET(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_channel_get" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
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

	t.Log(string(body))
	js, err := simplejson.NewJson(body)
	equal(t, err, nil)
	equal(t, js.Get("topic_name").MustString(), topicName)
	equal(t, js.Get("channel_name").MustString(), "ch")
	equal(t, js.Get("depth").MustInt(), 0)
	equal(t, js.Get("memory_depth").MustInt(), 0)
	equal(t, js.Get("backend_depth").MustInt(), 0)
	equal(t, js.Get("message_count").MustInt(), 0)
	equal(t, js.Get("paused").MustBool(), false)
	equal(t, js.Get("skipped").MustBool(), false)
	equal(t, js.Get("in_flight_count").MustInt(), 0)
	equal(t, js.Get("deferred_count").MustInt(), 0)
	equal(t, js.Get("requeue_count").MustInt(), 0)
	equal(t, js.Get("timeout_count").MustInt(), 0)
	equal(t, len(js.Get("clients").MustArray()), 0)
}

func TestHTTPCreateTopicPOST(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	time.Sleep(100 * time.Millisecond)

	topicName := "test_create_topic_post" + strconv.Itoa(int(time.Now().Unix()))

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics", nsqadmin1.RealHTTPAddr())
	body, _ := json.Marshal(map[string]interface{}{
		"topic":         topicName,
		"partition_num": "0",
		"replicator":    "1",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPCreateTopicChannelPOST(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	time.Sleep(100 * time.Millisecond)

	topicName := "test_create_topic_channel_post" + strconv.Itoa(int(time.Now().Unix()))

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics", nsqadmin1.RealHTTPAddr())
	body, _ := json.Marshal(map[string]interface{}{
		"topic":         topicName,
		"partition_num": "0",
		"replicator":    "1",
		"channel":       "ch",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	body, _ = ioutil.ReadAll(resp.Body)
	t.Log(string(body))
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPDeleteTopicPOST(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_delete_topic_post" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	req, _ := http.NewRequest("DELETE", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	body, _ := ioutil.ReadAll(resp.Body)
	t.Log(string(body))
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPDeleteChannelPOST(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_delete_channel_post" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
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

func TestHTTPSkipChannelPOST(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_skip_channel_post" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
	topic.GetChannel("ch")
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ := json.Marshal(map[string]interface{}{
		"action": "skip",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	equal(t, err, nil)
	body, _ = ioutil.ReadAll(resp.Body)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()

	url = fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ = json.Marshal(map[string]interface{}{
		"action": "unskip",
	})
	req, _ = http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err = client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)
	resp.Body.Close()
}

func TestHTTPPauseTopicPOST(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_pause_topic_post" + strconv.Itoa(int(time.Now().Unix()))
	nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
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
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	topicName := "test_pause_channel_post" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqds[0].GetNsqdInstance().GetTopic(topicName, 0)
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

func TestHTTPGetStatisticsRanks(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/statistics", nsqadmin1.RealHTTPAddr())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 200)

	type Filters struct {
		Filters []string `json:"filters"`
	}

	var filters Filters
	data, err := ioutil.ReadAll(resp.Body)
	if err == nil && data != nil {
		json.Unmarshal(data, &filters)
	}
	equal(t, len(filters.Filters), 2)
	for _, val := range filters.Filters {
		t.Logf("filter: %s", val)
		url = fmt.Sprintf("http://%s/api/statistics/%s", nsqadmin1.RealHTTPAddr(), val)
		req, _ := http.NewRequest("GET", url, nil)
		resp, err := client.Do(req)

		equal(t, err, nil)
		equal(t, resp.StatusCode, 200)

		type RankItem struct {
			TopicName         string `json:"topic_name"`
			TotalChannelDepth int64  `json:"total_channel_depth"`
			MessageCount      int64  `json:"message_count"`
			HourlyPubsize     int64  `json:"hourly_pubsize"`
		}

		type Rank struct {
			RankName string     `json:"rank_name"`
			Top10    []RankItem `json:"top10"`
		}

		var aRank Rank
		data, err := ioutil.ReadAll(resp.Body)
		assert(t, err == nil, "Fail to read response.", nil)
		if err == nil && data != nil {
			json.Unmarshal(data, &aRank)
		}
	}
}

func TestHTTPGetClusterStableInfoFail(t *testing.T) {
	dataPath, _, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/cluster/stats", nsqadmin1.RealHTTPAddr())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	equal(t, err, nil)
	equal(t, resp.StatusCode, 503)

	type NodeStat struct {
		Hostname         string  `json:"hostname"`
		BroadcastAddress string  `json:"broadcast_address"`
		TCPPort          int     `json:"tcp_port"`
		HTTPPort         int     `json:"http_port"`
		LeaderLoadFactor float64 `json:"leader_load_factor"`
		NodeLoadFactor   float64 `json:"node_load_factor"`
	}

	type ClusterInfo struct {
		Stable       bool        `json:"stable"`
		NodeStatList []*NodeStat `json:"node_stat_list"`
	}

	var aClusterStats ClusterInfo
	data, err := ioutil.ReadAll(resp.Body)
	assert(t, err == nil, "Fail to read form response.", nil)
	if err == nil && data != nil {
		json.Unmarshal(data, &aClusterStats)
	}
}
