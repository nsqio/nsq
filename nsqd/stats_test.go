package main

import (
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"github.com/bmizerany/assert"
	"github.com/mreiferson/go-snappystream"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQDOptions()
	tcpAddr, _, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	stats := nsqd.getStats()
	assert.Equal(t, len(stats), 1)
	assert.Equal(t, len(stats[0].Channels), 1)
	assert.Equal(t, len(stats[0].Channels[0].Clients), 1)
	log.Printf("stats: %+v", stats)
}

func TestClientAttributes(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	userAgent := "Test User Agent"

	*verbose = true
	options := NewNSQDOptions()
	options.SnappyEnabled = true
	tcpAddr, httpAddr, nsqd := mustStartNSQD(options)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	assert.Equal(t, err, nil)

	data := identifyFeatureNegotiation(t, conn, map[string]interface{}{"snappy": true, "user_agent": userAgent})
	r := struct {
		Snappy    bool   `json:"snappy"`
		UserAgent string `json:"user_agent"`
	}{}
	err = json.Unmarshal(data, &r)
	assert.Equal(t, err, nil)
	assert.Equal(t, r.Snappy, true)

	compressConn := snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)

	w := snappystream.NewWriter(conn)

	rw := readWriter{compressConn, w}

	topicName := "test_client_attributes" + strconv.Itoa(int(time.Now().Unix()))
	sub(t, rw, topicName, "ch")

	err = nsq.Ready(1).Write(rw)
	assert.Equal(t, err, nil)

	testUrl := fmt.Sprintf("http://127.0.0.1:%d/stats?format=json", httpAddr.Port)

	statsData, err := util.ApiRequest(testUrl)
	if err != nil {
		t.Fatalf(err.Error())
	}
	client := statsData.Get("topics").GetIndex(0).Get("channels").GetIndex(0).Get("clients").GetIndex(0)
	assert.Equal(t, client.Get("user_agent").MustString(), userAgent)
	assert.Equal(t, client.Get("snappy").MustBool(), true)
}
