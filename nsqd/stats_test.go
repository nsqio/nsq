package nsqd

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/test"
)

func TestStats(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	err := topic.PutMessage(msg)
	test.Nil(t, err)

	accompanyTopicName := "accompany_test_stats" + strconv.Itoa(int(time.Now().Unix()))
	accompanyTopic := nsqd.GetTopic(accompanyTopicName)
	msg = NewMessage(accompanyTopic.GenerateID(), []byte("accompany test body"))
	err = accompanyTopic.PutMessage(msg)
	test.Nil(t, err)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer func() { _ = conn.Close() }()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	stats := nsqd.GetStats(topicName, "ch", true).Topics
	t.Logf("stats: %+v", stats)

	test.Equal(t, 1, len(stats))
	test.Equal(t, 1, len(stats[0].Channels))
	test.Equal(t, 1, len(stats[0].Channels[0].Clients))
	test.Equal(t, 1, stats[0].Channels[0].ClientCount)

	stats = nsqd.GetStats(topicName, "ch", false).Topics
	t.Logf("stats: %+v", stats)

	test.Equal(t, 1, len(stats))
	test.Equal(t, 1, len(stats[0].Channels))
	test.Equal(t, 0, len(stats[0].Channels[0].Clients))
	test.Equal(t, 1, stats[0].Channels[0].ClientCount)

	stats = nsqd.GetStats(topicName, "none_exist_channel", false).Topics
	t.Logf("stats: %+v", stats)

	test.Equal(t, 0, len(stats))

	stats = nsqd.GetStats("none_exist_topic", "none_exist_channel", false).Topics
	t.Logf("stats: %+v", stats)

	test.Equal(t, 0, len(stats))
}

func TestStatsMultiTopicPubCounts(t *testing.T) {
	// clientV2.Stats must return an entry for every topic a client published
	// to. Before the fix a spurious break in the loop caused only the first
	// entry to be returned.
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

	client := newClientV2(0, conn, nsqd)
	client.PublishedMessage("topic-a", 3)
	client.PublishedMessage("topic-b", 7)
	client.PublishedMessage("topic-a", 1) // cumulative: topic-a = 4

	// Empty topicName means "all topics".
	stats := client.Stats("").(ClientV2Stats)
	countByTopic := make(map[string]uint64, len(stats.PubCounts))
	for _, pc := range stats.PubCounts {
		countByTopic[pc.Topic] += pc.Count
	}

	test.Equal(t, uint64(4), countByTopic["topic-a"])
	test.Equal(t, uint64(7), countByTopic["topic-b"])
}

func TestClientAttributes(t *testing.T) {
	userAgent := "Test User Agent"

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.SnappyEnabled = true
	tcpAddr, httpAddr, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer func() { _ = conn.Close() }()

	data := identify(t, conn, map[string]interface{}{
		"snappy":     true,
		"user_agent": userAgent,
	}, frameTypeResponse)
	resp := struct {
		Snappy    bool   `json:"snappy"`
		UserAgent string `json:"user_agent"`
	}{}
	err = json.Unmarshal(data, &resp)
	test.Nil(t, err)
	test.Equal(t, true, resp.Snappy)

	r := snappy.NewReader(conn)
	w := snappy.NewBufferedWriter(conn)
	readValidate(t, r, frameTypeResponse, "OK")

	topicName := "test_client_attributes" + strconv.Itoa(int(time.Now().Unix()))
	sub(t, readWriter{r, w}, topicName, "ch")

	var d struct {
		Topics []struct {
			Channels []struct {
				Clients []struct {
					UserAgent string `json:"user_agent"`
					Snappy    bool   `json:"snappy"`
				} `json:"clients"`
			} `json:"channels"`
		} `json:"topics"`
	}

	endpoint := fmt.Sprintf("http://%s/stats?format=json", httpAddr)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).GETV1(endpoint, &d)
	test.Nil(t, err)

	test.Equal(t, userAgent, d.Topics[0].Channels[0].Clients[0].UserAgent)
	test.Equal(t, true, d.Topics[0].Channels[0].Clients[0].Snappy)
}

func TestStatsChannelLocking(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer func() { _ = os.RemoveAll(opts.DataPath) }()
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		for i := 0; i < 25; i++ {
			msg := NewMessage(topic.GenerateID(), []byte("test"))
			err := topic.PutMessage(msg)
			test.Nil(t, err)
			err = channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
			test.Nil(t, err)
		}
		wg.Done()
	}()

	go func() {
		for i := 0; i < 25; i++ {
			nsqd.GetStats("", "", true)
		}
		wg.Done()
	}()

	wg.Wait()

	stats := nsqd.GetStats(topicName, "channel", false).Topics
	t.Logf("stats: %+v", stats)

	test.Equal(t, 1, len(stats))
	test.Equal(t, 1, len(stats[0].Channels))
	test.Equal(t, 25, stats[0].Channels[0].InFlightCount)
}
