package nsqdserver

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/absolute8511/nsq/internal/test"
	nsqdNs "github.com/absolute8511/nsq/nsqd"
	"github.com/mreiferson/go-snappystream"
)

func TestStats(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	msg := nsqdNs.NewMessage(0, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	stats := nsqd.GetStats()
	t.Logf("stats: %+v", stats)

	test.Equal(t, len(stats), 1)
	test.Equal(t, len(stats[0].Channels), 1)
	test.Equal(t, len(stats[0].Channels[0].Clients), 1)
}

func TestClientAttributes(t *testing.T) {
	userAgent := "Test User Agent"

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.LogLevel = 3
	opts.SnappyEnabled = true
	tcpAddr, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	test.Equal(t, err, nil)
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
	test.Equal(t, err, nil)
	test.Equal(t, resp.Snappy, true)

	r := snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	w := snappystream.NewWriter(conn)
	readValidate(t, r, frameTypeResponse, "OK")

	topicName := "test_client_attributes" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	topic.GetChannel("ch")
	sub(t, readWriter{r, w}, topicName, "ch")

	testURL := fmt.Sprintf("http://127.0.0.1:%d/stats?format=json", httpAddr.Port)

	statsData, err := API(testURL)
	test.Equal(t, err, nil)

	client := statsData.Get("topics").GetIndex(0).Get("channels").GetIndex(0).Get("clients").GetIndex(0)
	test.Equal(t, client.Get("user_agent").MustString(), userAgent)
	test.Equal(t, client.Get("snappy").MustBool(), true)
}
