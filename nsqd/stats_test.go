package nsqd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bitly/nsq/internal/http_api"
	"github.com/mreiferson/go-snappystream"
)

func TestStats(t *testing.T) {
	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
	defer conn.Close()

	identify(t, conn, nil, frameTypeResponse)
	sub(t, conn, topicName, "ch")

	stats := nsqd.GetStats()
	t.Logf("stats: %+v", stats)

	equal(t, len(stats), 1)
	equal(t, len(stats[0].Channels), 1)
	equal(t, len(stats[0].Channels[0].Clients), 1)
}

func TestClientAttributes(t *testing.T) {
	userAgent := "Test User Agent"

	opts := NewNSQDOptions()
	opts.Logger = newTestLogger(t)
	opts.Verbose = true
	opts.SnappyEnabled = true
	tcpAddr, httpAddr, nsqd := mustStartNSQD(opts)
	defer nsqd.Exit()

	conn, err := mustConnectNSQD(tcpAddr)
	equal(t, err, nil)
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
	equal(t, err, nil)
	equal(t, resp.Snappy, true)

	r := snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	w := snappystream.NewWriter(conn)
	readValidate(t, r, frameTypeResponse, "OK")

	topicName := "test_client_attributes" + strconv.Itoa(int(time.Now().Unix()))
	sub(t, readWriter{r, w}, topicName, "ch")

	testURL := fmt.Sprintf("http://127.0.0.1:%d/stats?format=json", httpAddr.Port)

	statsData, err := http_api.NegotiateV1("GET", testURL, nil)
	equal(t, err, nil)

	client := statsData.Get("topics").GetIndex(0).Get("channels").GetIndex(0).Get("clients").GetIndex(0)
	equal(t, client.Get("user_agent").MustString(), userAgent)
	equal(t, client.Get("snappy").MustBool(), true)
}
