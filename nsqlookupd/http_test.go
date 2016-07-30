package nsqlookupd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type Version struct {
	Version string `json:"version"`
}

type InfoDoc struct {
	Code   int      `json:"status_code"`
	Status string   `json:"status_txt"`
	Data   *Version `json:"data"`
}

type ChannelsDoc struct {
	Channels []interface{} `json:"channels"`
}

type OldErrMessage struct {
	Message string `json:"status_txt"`
}

type ErrMessage struct {
	Message string `json:"message"`
}

func bootstrapNSQCluster(t *testing.T) (string, []*nsqd.NSQD, *NSQLookupd) {
	lgr := test.NewTestLogger(t)

	nsqlookupdOpts := NewOptions()
	nsqlookupdOpts.TCPAddress = "127.0.0.1:0"
	nsqlookupdOpts.HTTPAddress = "127.0.0.1:0"
	nsqlookupdOpts.BroadcastAddress = "127.0.0.1"
	nsqlookupdOpts.Logger = lgr
	nsqlookupd1 := New(nsqlookupdOpts)
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

	time.Sleep(100 * time.Millisecond)

	return tmpDir, []*nsqd.NSQD{nsqd1}, nsqlookupd1
}

func makeTopic(nsqlookupd *NSQLookupd, topicName string) {
	key := Registration{"topic", topicName, ""}
	nsqlookupd.DB.AddRegistration(key)
}

func makeChannel(nsqlookupd *NSQLookupd, topicName string, channelName string) {
	key := Registration{"channel", topicName, channelName}
	nsqlookupd.DB.AddRegistration(key)
	makeTopic(nsqlookupd, topicName)
}

func TestPing(t *testing.T) {
	dataPath, nsqds, nsqlookupd1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupd1.Exit()

	client := http.Client{}
	url := fmt.Sprintf("http://%s/ping", nsqlookupd1.RealHTTPAddr())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	test.Equal(t, []byte("OK"), body)
}

func TestInfo(t *testing.T) {
	dataPath, nsqds, nsqlookupd1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupd1.Exit()

	client := http.Client{}
	url := fmt.Sprintf("http://%s/info", nsqlookupd1.RealHTTPAddr())
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	info := InfoDoc{}
	err = json.Unmarshal(body, &info)
	test.Nil(t, err)
	test.Equal(t, version.Binary, info.Data.Version)
}

func TestCreateTopic(t *testing.T) {
	dataPath, nsqds, nsqlookupd1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupd1.Exit()

	em := ErrMessage{}
	client := http.Client{}
	url := fmt.Sprintf("http://%s/topic/create", nsqlookupd1.RealHTTPAddr())

	req, _ := http.NewRequest("POST", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	topicName := "sampletopicA" + strconv.Itoa(int(time.Now().Unix())) + "$"
	url = fmt.Sprintf("http://%s/topic/create?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "INVALID_ARG_TOPIC", em.Message)

	topicName = "sampletopicA" + strconv.Itoa(int(time.Now().Unix()))
	url = fmt.Sprintf("http://%s/topic/create?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)
}

func TestDeleteTopic(t *testing.T) {
	dataPath, nsqds, nsqlookupd1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupd1.Exit()

	em := ErrMessage{}
	client := http.Client{}
	url := fmt.Sprintf("http://%s/topic/delete", nsqlookupd1.RealHTTPAddr())

	req, _ := http.NewRequest("POST", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	topicName := "sampletopicA" + strconv.Itoa(int(time.Now().Unix()))
	makeTopic(nsqlookupd1, topicName)

	url = fmt.Sprintf("http://%s/topic/delete?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)

	topicName = "sampletopicB" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "foobar" + strconv.Itoa(int(time.Now().Unix()))
	makeChannel(nsqlookupd1, topicName, channelName)

	url = fmt.Sprintf("http://%s/topic/delete?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)
}

func TestGetChannels(t *testing.T) {
	dataPath, nsqds, nsqlookupd1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupd1.Exit()

	client := http.Client{}
	url := fmt.Sprintf("http://%s/channels", nsqlookupd1.RealHTTPAddr())

	oem := OldErrMessage{}
	// pre-version 1
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 500, resp.StatusCode)
	test.Equal(t, "Internal Server Error", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &oem)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", oem.Message)

	// version 1
	em := ErrMessage{}
	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	ch := ChannelsDoc{}
	topicName := "sampletopicA" + strconv.Itoa(int(time.Now().Unix()))
	makeTopic(nsqlookupd1, topicName)

	url = fmt.Sprintf("http://%s/channels?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &ch)
	test.Nil(t, err)
	test.Equal(t, 0, len(ch.Channels))

	topicName = "sampletopicB" + strconv.Itoa(int(time.Now().Unix()))
	channelName := "foobar" + strconv.Itoa(int(time.Now().Unix()))
	makeChannel(nsqlookupd1, topicName, channelName)

	url = fmt.Sprintf("http://%s/channels?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &ch)
	test.Nil(t, err)
	test.Equal(t, 1, len(ch.Channels))
	test.Equal(t, channelName, ch.Channels[0])
}

func TestCreateChannel(t *testing.T) {
	dataPath, nsqds, nsqlookupd1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupd1.Exit()

	em := ErrMessage{}
	client := http.Client{}
	url := fmt.Sprintf("http://%s/channel/create", nsqlookupd1.RealHTTPAddr())

	req, _ := http.NewRequest("POST", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	topicName := "sampletopicB" + strconv.Itoa(int(time.Now().Unix())) + "$"
	url = fmt.Sprintf("http://%s/channel/create?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "INVALID_ARG_TOPIC", em.Message)

	topicName = "sampletopicB" + strconv.Itoa(int(time.Now().Unix()))
	url = fmt.Sprintf("http://%s/channel/create?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_CHANNEL", em.Message)

	channelName := "foobar" + strconv.Itoa(int(time.Now().Unix())) + "$"
	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=%s", nsqlookupd1.RealHTTPAddr(), topicName, channelName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "INVALID_ARG_CHANNEL", em.Message)

	channelName = "foobar" + strconv.Itoa(int(time.Now().Unix()))
	url = fmt.Sprintf("http://%s/channel/create?topic=%s&channel=%s", nsqlookupd1.RealHTTPAddr(), topicName, channelName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)
}

func TestDeleteChannel(t *testing.T) {
	dataPath, nsqds, nsqlookupd1 := bootstrapNSQCluster(t)
	defer os.RemoveAll(dataPath)
	defer nsqds[0].Exit()
	defer nsqlookupd1.Exit()

	em := ErrMessage{}
	client := http.Client{}
	url := fmt.Sprintf("http://%s/channel/delete", nsqlookupd1.RealHTTPAddr())

	req, _ := http.NewRequest("POST", url, nil)
	resp, err := client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_TOPIC", em.Message)

	topicName := "sampletopicB" + strconv.Itoa(int(time.Now().Unix())) + "$"
	url = fmt.Sprintf("http://%s/channel/delete?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "INVALID_ARG_TOPIC", em.Message)

	topicName = "sampletopicB" + strconv.Itoa(int(time.Now().Unix()))
	url = fmt.Sprintf("http://%s/channel/delete?topic=%s", nsqlookupd1.RealHTTPAddr(), topicName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "MISSING_ARG_CHANNEL", em.Message)

	channelName := "foobar" + strconv.Itoa(int(time.Now().Unix())) + "$"
	url = fmt.Sprintf("http://%s/channel/delete?topic=%s&channel=%s", nsqlookupd1.RealHTTPAddr(), topicName, channelName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 400, resp.StatusCode)
	test.Equal(t, "Bad Request", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "INVALID_ARG_CHANNEL", em.Message)

	channelName = "foobar" + strconv.Itoa(int(time.Now().Unix()))
	url = fmt.Sprintf("http://%s/channel/delete?topic=%s&channel=%s", nsqlookupd1.RealHTTPAddr(), topicName, channelName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 404, resp.StatusCode)
	test.Equal(t, "Not Found", http.StatusText(resp.StatusCode))
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	err = json.Unmarshal(body, &em)
	test.Nil(t, err)
	test.Equal(t, "CHANNEL_NOT_FOUND", em.Message)

	makeChannel(nsqlookupd1, topicName, channelName)

	req, _ = http.NewRequest("POST", url, nil)
	resp, err = client.Do(req)
	test.Nil(t, err)
	test.Equal(t, 200, resp.StatusCode)
	body, _ = ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	t.Logf("%s", body)
	test.Equal(t, []byte(""), body)
}
