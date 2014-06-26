package nsqd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/bitly/nsq/util"
	"github.com/bmizerany/assert"
	"github.com/mreiferson/go-snappystream"
)

func TestStats(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	stats := nsqd.GetStats()
	assert.Equal(t, len(stats), 1)
	assert.Equal(t, len(stats[0].Channels), 1)
	assert.Equal(t, len(stats[0].Channels[0].Clients), 1)
	log.Printf("stats: %+v", stats)
}

func TestClientAttributes(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	userAgent := "Test User Agent"

	options := NewNSQDOptions()
	options.Verbose = true
	options.SnappyEnabled = true
	tcpAddr, httpAddr, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identify(t, conn, map[string]interface{}{
		"snappy":     true,
		"user_agent": userAgent,
	}, frameTypeResponse)
	resp := struct {
		Snappy    bool   `json:"snappy"`
		UserAgent string `json:"user_agent"`
	}{}
	err = json.Unmarshal(data, &resp)
	assert.Equal(t, err, nil)
	assert.Equal(t, resp.Snappy, true)

	r := snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	w := snappystream.NewWriter(conn)
	readValidate(t, r, frameTypeResponse, "OK")

	topicName := "test_client_attributes" + strconv.Itoa(int(time.Now().Unix()))
	sub(t, readWriter{r, w}, topicName, "ch")

	testUrl := fmt.Sprintf("http://127.0.0.1:%d/stats?format=json", httpAddr.Port)

	statsData, err := util.APIRequestNegotiateV1("GET", testUrl, nil)
	if err != nil {
		t.Fatalf(err.Error())
	}
	client := statsData.Get("topics").GetIndex(0).Get("channels").GetIndex(0).Get("clients").GetIndex(0)
	assert.Equal(t, client.Get("user_agent").MustString(), userAgent)
	assert.Equal(t, client.Get("snappy").MustBool(), true)
}
