package nsq

import (
	"fmt"
	"github.com/bitly/nsq/util"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestNsqdToLookupd(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	topicName := "cluster_test" + strconv.Itoa(int(time.Now().Unix()))

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("ERROR: failed to get hostname - %s", err.Error())
	}

	_, err = util.ApiRequest(fmt.Sprintf("http://127.0.0.1:4151/create_topic?topic=%s", topicName))
	if err != nil {
		t.Fatalf(err.Error())
	}

	_, err = util.ApiRequest(fmt.Sprintf("http://127.0.0.1:4151/create_channel?topic=%s&channel=ch", topicName))
	if err != nil {
		t.Fatalf(err.Error())
	}

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	data, err := util.ApiRequest("http://127.0.0.1:4161/debug")
	if err != nil {
		t.Fatalf(err.Error())
	}

	topicData := data.Get("topic:" + topicName + ":")
	producers, _ := topicData.Array()
	assert.Equal(t, len(producers), 1)

	producer := topicData.GetIndex(0)
	assert.Equal(t, producer.Get("hostname").MustString(), hostname)
	assert.Equal(t, producer.Get("broadcast_address").MustString(), hostname)
	assert.Equal(t, producer.Get("tcp_port").MustInt(), 4150)
	assert.Equal(t, producer.Get("tombstoned").MustBool(), false)

	channelData := data.Get("channel:" + topicName + ":ch")
	producers, _ = channelData.Array()
	assert.Equal(t, len(producers), 1)

	producer = topicData.GetIndex(0)
	assert.Equal(t, producer.Get("hostname").MustString(), hostname)
	assert.Equal(t, producer.Get("broadcast_address").MustString(), hostname)
	assert.Equal(t, producer.Get("tcp_port").MustInt(), 4150)
	assert.Equal(t, producer.Get("tombstoned").MustBool(), false)

	data, err = util.ApiRequest("http://127.0.0.1:4161/lookup?topic=" + topicName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 1)

	producer = data.Get("producers").GetIndex(0)
	assert.Equal(t, producer.Get("hostname").MustString(), hostname)
	assert.Equal(t, producer.Get("broadcast_address").MustString(), hostname)
	assert.Equal(t, producer.Get("tcp_port").MustInt(), 4150)

	channels, _ := data.Get("channels").Array()
	assert.Equal(t, len(channels), 1)

	channel := channels[0].(string)
	assert.Equal(t, channel, "ch")

	data, err = util.ApiRequest("http://127.0.0.1:4151/delete_topic?topic=" + topicName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	data, err = util.ApiRequest("http://127.0.0.1:4161/lookup?topic=" + topicName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 0)

	data, err = util.ApiRequest("http://127.0.0.1:4161/debug")
	if err != nil {
		t.Fatalf(err.Error())
	}

	producers, _ = data.Get("topic:" + topicName + ":").Array()
	assert.Equal(t, len(producers), 0)

	producers, _ = data.Get("channel:" + topicName + ":ch").Array()
	assert.Equal(t, len(producers), 0)
}
