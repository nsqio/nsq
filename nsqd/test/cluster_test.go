package test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/bitly/nsq/internal/http_api"
)

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func TestNsqdToLookupd(t *testing.T) {
	topicName := "cluster_test" + strconv.Itoa(int(time.Now().Unix()))

	hostname, err := os.Hostname()
	equal(t, err, nil)

	url := fmt.Sprintf("http://127.0.0.1:4151/topic/create?topic=%s", topicName)
	_, err = http_api.NegotiateV1("POST", url, nil)
	equal(t, err, nil)

	url = fmt.Sprintf("http://127.0.0.1:4151/channel/create?topic=%s&channel=ch", topicName)
	_, err = http_api.NegotiateV1("POST", url, nil)
	equal(t, err, nil)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	data, err := http_api.NegotiateV1("GET", "http://127.0.0.1:4161/debug", nil)
	equal(t, err, nil)

	topicData := data.Get("topic:" + topicName + ":")
	producers, _ := topicData.Array()
	equal(t, len(producers), 1)

	producer := topicData.GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), 4150)
	equal(t, producer.Get("tombstoned").MustBool(), false)

	channelData := data.Get("channel:" + topicName + ":ch")
	producers, _ = channelData.Array()
	equal(t, len(producers), 1)

	producer = topicData.GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), 4150)
	equal(t, producer.Get("tombstoned").MustBool(), false)

	data, err = http_api.NegotiateV1("GET", "http://127.0.0.1:4161/lookup?topic="+topicName, nil)
	equal(t, err, nil)

	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 1)

	producer = data.Get("producers").GetIndex(0)
	equal(t, producer.Get("hostname").MustString(), hostname)
	equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	equal(t, producer.Get("tcp_port").MustInt(), 4150)

	channels, _ := data.Get("channels").Array()
	equal(t, len(channels), 1)

	channel := channels[0].(string)
	equal(t, channel, "ch")

	data, err = http_api.NegotiateV1("POST", "http://127.0.0.1:4151/topic/delete?topic="+topicName, nil)
	equal(t, err, nil)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	data, err = http_api.NegotiateV1("GET", "http://127.0.0.1:4161/lookup?topic="+topicName, nil)
	equal(t, err, nil)

	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 0)

	data, err = http_api.NegotiateV1("GET", "http://127.0.0.1:4161/debug", nil)
	equal(t, err, nil)

	producers, _ = data.Get("topic:" + topicName + ":").Array()
	equal(t, len(producers), 0)

	producers, _ = data.Get("channel:" + topicName + ":ch").Array()
	equal(t, len(producers), 0)
}
