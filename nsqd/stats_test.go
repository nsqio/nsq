package nsqd

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"sort"

	"github.com/golang/snappy"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/test"
)

func TestStats(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), []byte("test body"))
	topic.PutMessage(msg)

	accompanyTopicName := "accompany_test_stats" + strconv.Itoa(int(time.Now().Unix()))
	accompanyTopic := nsqd.GetTopic(accompanyTopicName)
	msg = NewMessage(accompanyTopic.GenerateID(), []byte("accompany test body"))
	accompanyTopic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

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

func TestClientAttributes(t *testing.T) {
	userAgent := "Test User Agent"

	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.LogLevel = LOG_DEBUG
	opts.SnappyEnabled = true
	tcpAddr, httpAddr, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Nil(t, err)
	defer conn.Close()

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
	//lint:ignore SA1019 NewWriter is deprecated by NewBufferedWriter, but we don't want to buffer
	w := snappy.NewWriter(conn)
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
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel("channel")

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		for i := 0; i < 25; i++ {
			msg := NewMessage(topic.GenerateID(), []byte("test"))
			topic.PutMessage(msg)
			channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
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

// Test generated using Keploy
func TestTopicsByNameSorting(t *testing.T) {
	topic1 := &Topic{name: "topicA"}
	topic2 := &Topic{name: "topicB"}
	topic3 := &Topic{name: "topicC"}

	topics := TopicsByName{Topics: []*Topic{topic3, topic1, topic2}}
	sort.Sort(topics)

	if topics.Topics[0].name != "topicA" || topics.Topics[1].name != "topicB" || topics.Topics[2].name != "topicC" {
		t.Errorf("Expected topics to be sorted by name, got: %v", topics)
	}
}

// Test generated using Keploy
func TestChannelsByNameSorting(t *testing.T) {
	channel1 := &Channel{name: "channelA"}
	channel2 := &Channel{name: "channelB"}
	channel3 := &Channel{name: "channelC"}

	channels := ChannelsByName{Channels: []*Channel{channel3, channel1, channel2}}
	sort.Sort(channels)

	if channels.Channels[0].name != "channelA" || channels.Channels[1].name != "channelB" || channels.Channels[2].name != "channelC" {
		t.Errorf("Expected channels to be sorted by name, got: %v", channels)
	}
}

// Test generated using Keploy
func TestGetStatsNoTopics(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	_, _, nsqd := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqd.Exit()

	stats := nsqd.GetStats("", "", false)
	if len(stats.Topics) != 0 {
		t.Errorf("Expected no topics, got: %v", stats.Topics)
	}
}
