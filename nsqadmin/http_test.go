package nsqadmin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"net/url"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
	"github.com/nsqio/nsq/nsqlookupd"
)

type TopicsDoc struct {
	Topics []interface{} `json:"topics"`
}

type TopicStatsDoc struct {
	*clusterinfo.TopicStats
	Message string `json:"message"`
}

type NodesDoc struct {
	Nodes   clusterinfo.Producers `json:"nodes"`
	Message string                `json:"message"`
}

type NodeStatsDoc struct {
	Node          string                    `json:"node"`
	TopicStats    []*clusterinfo.TopicStats `json:"topics"`
	TotalMessages int64                     `json:"total_messages"`
	TotalClients  int64                     `json:"total_clients"`
	Message       string                    `json:"message"`
}

type ChannelStatsDoc struct {
	*clusterinfo.ChannelStats
	Message string `json:"message"`
}

func mustStartNSQLookupd(opts *nsqlookupd.Options) (*net.TCPAddr, *net.TCPAddr, *nsqlookupd.NSQLookupd) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	lookupd, err := nsqlookupd.New(opts)
	if err != nil {
		panic(err)
	}
	go func() {
		err := lookupd.Main()
		if err != nil {
			panic(err)
		}
	}()
	return lookupd.RealTCPAddr(), lookupd.RealHTTPAddr(), lookupd
}

func bootstrapNSQCluster(t *testing.T) (string, []*nsqd.NSQD, []*nsqlookupd.NSQLookupd, *NSQAdmin) {
	return bootstrapNSQClusterWithAuth(t, false)
}

func bootstrapNSQClusterWithAuth(t *testing.T, withAuth bool) (string, []*nsqd.NSQD, []*nsqlookupd.NSQLookupd, *NSQAdmin) {
	lgr := test.NewTestLogger(t)

	nsqlookupdOpts := nsqlookupd.NewOptions()
	nsqlookupdOpts.TCPAddress = "127.0.0.1:0"
	nsqlookupdOpts.HTTPAddress = "127.0.0.1:0"
	nsqlookupdOpts.BroadcastAddress = "127.0.0.1"
	nsqlookupdOpts.Logger = lgr
	nsqlookupd1, err := nsqlookupd.New(nsqlookupdOpts)
	if err != nil {
		panic(err)
	}
	go func() {
		err := nsqlookupd1.Main()
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	nsqdOpts := nsqd.NewOptions()
	nsqdOpts.TCPAddress = "127.0.0.1:0"
	nsqdOpts.HTTPAddress = "127.0.0.1:0"
	nsqdOpts.BroadcastAddress = "127.0.0.1"
	nsqdOpts.NSQLookupdTCPAddresses = []string{nsqlookupd1.RealTCPAddr().String()}
	nsqdOpts.Logger = lgr
	tmpDir, err := os.MkdirTemp("", "nsq-test-")
	if err != nil {
		panic(err)
	}
	nsqdOpts.DataPath = tmpDir
	nsqd1, err := nsqd.New(nsqdOpts)
	if err != nil {
		panic(err)
	}
	go func() {
		err := nsqd1.Main()
		if err != nil {
			panic(err)
		}
	}()

	nsqadminOpts := NewOptions()
	nsqadminOpts.HTTPAddress = "127.0.0.1:0"
	nsqadminOpts.NSQLookupdHTTPAddresses = []string{nsqlookupd1.RealHTTPAddr().String()}
	nsqadminOpts.Logger = lgr
	if withAuth {
		nsqadminOpts.AdminUsers = []string{"matt"}
	}
	nsqadmin1, err := New(nsqadminOpts)
	if err != nil {
		panic(err)
	}
	go func() {
		err := nsqadmin1.Main()
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	return tmpDir, []*nsqd.NSQD{nsqd1}, []*nsqlookupd.NSQLookupd{nsqlookupd1}, nsqadmin1
}

func TestPing(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	client := http.Client{}
	url := fmt.Sprintf("http://%s/ping", nsqadmin1.RealHTTPAddr())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	test.Equal(t, []byte("OK"), body)
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	tr := TopicsDoc{}
	err = json.Unmarshal(body, &tr)
	test.Nil(t, err)
	test.Equal(t, 1, len(tr.Topics))
	test.Equal(t, topicName, tr.Topics[0])
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	ts := TopicStatsDoc{}
	err = json.Unmarshal(body, &ts)
	test.Nil(t, err)
	test.Equal(t, topicName, ts.TopicName)
	test.Equal(t, 0, int(ts.Depth))
	test.Equal(t, 0, int(ts.MemoryDepth))
	test.Equal(t, 0, int(ts.BackendDepth))
	test.Equal(t, 0, int(ts.MessageCount))
	test.Equal(t, false, ts.Paused)
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	hostname, _ := os.Hostname()

	t.Logf("%s", body)
	ns := NodesDoc{}
	err = json.Unmarshal(body, &ns)
	test.Nil(t, err)
	test.Equal(t, 1, len(ns.Nodes))
	testNode := ns.Nodes[0]
	test.Equal(t, hostname, testNode.Hostname)
	test.Equal(t, "127.0.0.1", testNode.BroadcastAddress)
	test.Equal(t, nsqds[0].RealTCPAddr().(*net.TCPAddr).Port, testNode.TCPPort)
	test.Equal(t, nsqds[0].RealHTTPAddr().(*net.TCPAddr).Port, testNode.HTTPPort)
	test.Equal(t, version.Binary, testNode.Version)
	test.Equal(t, 0, len(testNode.Topics))
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	cs := ChannelStatsDoc{}
	err = json.Unmarshal(body, &cs)
	test.Nil(t, err)
	test.Equal(t, topicName, cs.TopicName)
	test.Equal(t, "ch", cs.ChannelName)
	test.Equal(t, 0, int(cs.Depth))
	test.Equal(t, 0, int(cs.MemoryDepth))
	test.Equal(t, 0, int(cs.BackendDepth))
	test.Equal(t, 0, int(cs.MessageCount))
	test.Equal(t, false, cs.Paused)
	test.Equal(t, 0, int(cs.InFlightCount))
	test.Equal(t, 0, int(cs.DeferredCount))
	test.Equal(t, 0, int(cs.RequeueCount))
	test.Equal(t, 0, int(cs.TimeoutCount))
	test.Equal(t, 0, len(cs.Clients))
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	ns := NodeStatsDoc{}
	err = json.Unmarshal(body, &ns)
	test.Nil(t, err)
	test.Equal(t, nsqds[0].RealHTTPAddr().String(), ns.Node)
	test.Equal(t, 1, len(ns.TopicStats))
	testTopic := ns.TopicStats[0]
	test.Equal(t, topicName, testTopic.TopicName)
	test.Equal(t, 0, int(testTopic.Depth))
	test.Equal(t, 0, int(testTopic.MemoryDepth))
	test.Equal(t, 0, int(testTopic.BackendDepth))
	test.Equal(t, 0, int(testTopic.MessageCount))
	test.Equal(t, false, testTopic.Paused)
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
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
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
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
	test.Nil(t, err)
	_, _ = io.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	url = fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ = json.Marshal(map[string]interface{}{
		"action": "unpause",
	})
	req, _ = http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
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
	test.Nil(t, err)
	_, _ = io.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	url = fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ = json.Marshal(map[string]interface{}{
		"action": "unpause",
	})
	req, _ = http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
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
	test.Equal(t, int64(1), topic.Depth())
	time.Sleep(100 * time.Millisecond)

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ := json.Marshal(map[string]interface{}{
		"action": "empty",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	test.Nil(t, err)
	_, _ = io.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	test.Equal(t, int64(0), topic.Depth())
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
	test.Equal(t, int64(1), channel.Depth())

	client := http.Client{}
	url := fmt.Sprintf("http://%s/api/topics/%s/ch", nsqadmin1.RealHTTPAddr(), topicName)
	body, _ := json.Marshal(map[string]interface{}{
		"action": "empty",
	})
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	resp, err := client.Do(req)
	test.Nil(t, err)
	_, _ = io.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	test.Equal(t, int64(0), channel.Depth())
}

func TestHTTPconfig(t *testing.T) {
	dataPath, nsqds, nsqlookupds, nsqadmin1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupds[0].Exit()
	defer nsqadmin1.Exit()

	lopts := nsqlookupd.NewOptions()
	lopts.Logger = test.NewTestLogger(t)

	lopts1 := *lopts
	_, _, lookupd1 := mustStartNSQLookupd(&lopts1)
	defer lookupd1.Exit()
	lopts2 := *lopts
	_, _, lookupd2 := mustStartNSQLookupd(&lopts2)
	defer lookupd2.Exit()

	url := fmt.Sprintf("http://%s/config/nsqlookupd_http_addresses", nsqadmin1.RealHTTPAddr())
	resp, err := http.Get(url)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	origaddrs := fmt.Sprintf(`["%s"]`, nsqlookupds[0].RealHTTPAddr().String())
	test.Equal(t, origaddrs, string(body))

	client := http.Client{}
	addrs := fmt.Sprintf(`["%s","%s"]`, lookupd1.RealHTTPAddr().String(), lookupd2.RealHTTPAddr().String())
	url = fmt.Sprintf("http://%s/config/nsqlookupd_http_addresses", nsqadmin1.RealHTTPAddr())
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(addrs)))
	test.Nil(t, err)
	resp, err = client.Do(req)
	test.Nil(t, err)
	defer resp.Body.Close()
	body, _ = io.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, addrs, string(body))

	url = fmt.Sprintf("http://%s/config/log_level", nsqadmin1.RealHTTPAddr())
	req, err = http.NewRequest("PUT", url, bytes.NewBuffer([]byte(`fatal`)))
	test.Nil(t, err)
	resp, err = client.Do(req)
	test.Nil(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	test.Equal(t, 200, resp.StatusCode)
	test.Equal(t, LOG_FATAL, nsqadmin1.getOpts().LogLevel)

	url = fmt.Sprintf("http://%s/config/log_level", nsqadmin1.RealHTTPAddr())
	req, err = http.NewRequest("PUT", url, bytes.NewBuffer([]byte(`bad`)))
	test.Nil(t, err)
	resp, err = client.Do(req)
	test.Nil(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	test.Equal(t, 400, resp.StatusCode)
}

func TestHTTPconfigCIDR(t *testing.T) {
	opts := NewOptions()
	opts.HTTPAddress = "127.0.0.1:0"
	opts.NSQLookupdHTTPAddresses = []string{"127.0.0.1:4161"}
	opts.Logger = test.NewTestLogger(t)
	opts.AllowConfigFromCIDR = "10.0.0.0/8"
	nsqadmin, err := New(opts)
	test.Nil(t, err)
	go func() {
		err := nsqadmin.Main()
		if err != nil {
			panic(err)
		}
	}()
	defer nsqadmin.Exit()

	time.Sleep(100 * time.Millisecond)

	url := fmt.Sprintf("http://%s/config/nsqlookupd_http_addresses", nsqadmin.RealHTTPAddr())
	resp, err := http.Get(url)
	test.Nil(t, err)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	test.Equal(t, 403, resp.StatusCode)
}

// Test generated using Keploy
func TestMaybeWarnMsgWithMessages(t *testing.T) {
	msgs := []string{"message1", "message2"}
	expected := "WARNING: message1; message2"
	result := maybeWarnMsg(msgs)
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test generated using Keploy
func TestNewSingleHostReverseProxy(t *testing.T) {
	target, _ := url.Parse("http://example.com")
	proxy := NewSingleHostReverseProxy(target, 5*time.Second, 10*time.Second)

	req := &http.Request{
		URL:    &url.URL{},
		Header: make(http.Header),
	}
	target.User = url.UserPassword("user", "pass")
	proxy.Director(req)

	if req.URL.Scheme != "http" || req.URL.Host != "example.com" {
		t.Errorf("Expected scheme and host to be set, got %v and %v", req.URL.Scheme, req.URL.Host)
	}

	username, password, ok := req.BasicAuth()
	if !ok || username != "user" || password != "pass" {
		t.Errorf("Expected basic auth to be set, got %v:%v", username, password)
	}
}
