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

	producers, _ := data.Get("topic:" + topicName + ":").Array()
	assert.Equal(t, len(producers), 1)

	producer := producers[0]
	producerData, _ := producer.(map[string]interface{})
	address := producerData["address"].(string) //TODO: remove for 1.0
	producerHostname := producerData["hostname"].(string)
	broadcastAddress := producerData["broadcast_address"].(string)
	port := int(producerData["tcp_port"].(float64))
	tombstoned := producerData["tombstoned"].(bool)
	assert.Equal(t, address, hostname)
	assert.Equal(t, producerHostname, hostname)
	assert.Equal(t, broadcastAddress, hostname)
	assert.Equal(t, port, 4150)
	assert.Equal(t, tombstoned, false)

	producers, _ = data.Get("channel:" + topicName + ":ch").Array()
	assert.Equal(t, len(producers), 1)

	producer = producers[0]
	producerData, _ = producer.(map[string]interface{})
	address = producerData["address"].(string) //TODO: remove for 1.0
	producerHostname = producerData["hostname"].(string)
	broadcastAddress = producerData["broadcast_address"].(string)

	port = int(producerData["tcp_port"].(float64))
	tombstoned = producerData["tombstoned"].(bool)
	assert.Equal(t, address, hostname)
	assert.Equal(t, producerHostname, hostname)
	assert.Equal(t, broadcastAddress, hostname)
	assert.Equal(t, port, 4150)
	assert.Equal(t, tombstoned, false)

	data, err = util.ApiRequest("http://127.0.0.1:4161/lookup?topic=" + topicName)
	if err != nil {
		t.Fatalf(err.Error())
	}

	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 1)

	producer = producers[0]
	producerData, _ = producer.(map[string]interface{})
	address = producerData["address"].(string) //TODO: remove for 1.0
	producerHostname = producerData["hostname"].(string)
	broadcastAddress = producerData["broadcast_address"].(string)
	port = int(producerData["tcp_port"].(float64))
	assert.Equal(t, address, hostname)
	assert.Equal(t, producerHostname, hostname)
	assert.Equal(t, broadcastAddress, hostname)
	assert.Equal(t, port, 4150)

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
