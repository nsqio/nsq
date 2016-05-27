package nsqdserver

import (
	"fmt"
	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/clusterinfo"
	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	nsqdNs "github.com/absolute8511/nsq/nsqd"
	"github.com/absolute8511/nsq/nsqlookupd"
	"github.com/bitly/go-simplejson"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"testing"
	"time"
)

func newTestLogger(tbl test.TbLog) levellogger.Logger {
	return &test.TestLogger{tbl, 0}
}

func mustStartNSQLookupd(opts *nsqlookupd.Options) (*net.TCPAddr, *net.TCPAddr, *nsqlookupd.NSQLookupd) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	lookupd := nsqlookupd.New(opts)
	lookupd.Main()
	return lookupd.RealTCPAddr(), lookupd.RealHTTPAddr(), lookupd
}

func mustStartNSQD(opts *nsqdNs.Options) (*net.TCPAddr, *net.TCPAddr, *nsqdNs.NSQD, *NsqdServer) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
		if err != nil {
			panic(err)
		}
		opts.DataPath = tmpDir
	}
	nsqd := nsqdNs.New(opts)
	nsqdServer := NewNsqdServer(nsqd, opts)
	nsqdServer.Main()
	return nsqdServer.ctx.realTCPAddr(), nsqdServer.ctx.realHTTPAddr(), nsqdServer.ctx.nsqd, nsqdServer
}

func mustConnectNSQD(tcpAddr *net.TCPAddr) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		return nil, err
	}
	conn.Write(nsq.MagicV2)
	return conn, nil
}

func API(endpoint string) (data *simplejson.Json, err error) {
	d := make(map[string]interface{})
	err = http_api.NewClient(nil).NegotiateV1(endpoint, &d)
	data = simplejson.New()
	data.SetPath(nil, d)
	return
}

func TestChannelEmptyConsumer(t *testing.T) {
	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	conn, _ := mustConnectNSQD(tcpAddr)
	defer conn.Close()

	topicName := "test_channel_empty" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopicIgnPart(topicName)
	channel := topic.GetChannel("channel")
	client := nsqdNs.NewClientV2(0, conn, opts, nil)
	client.SetReadyCount(25)
	channel.AddClient(client.ID, client)

	for i := 0; i < 25; i++ {
		msg := nsqdNs.NewMessage(nsqdNs.MessageID(i), []byte("test"))
		channel.StartInFlightTimeout(msg, 0, opts.MsgTimeout)
		client.SendingMessage()
	}

	for _, cl := range channel.GetClients() {
		stats := cl.Stats()
		test.Equal(t, stats.InFlightCount, int64(25))
	}

	channel.Empty()

	for _, cl := range channel.GetClients() {
		stats := cl.Stats()
		test.Equal(t, stats.InFlightCount, int64(0))
	}
}

func TestReconfigure(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = newTestLogger(t)
	_, _, lookupd1 := mustStartNSQLookupd(lopts)
	defer lookupd1.Exit()
	_, _, lookupd2 := mustStartNSQLookupd(lopts)
	defer lookupd2.Exit()
	_, _, lookupd3 := mustStartNSQLookupd(lopts)
	defer lookupd3.Exit()

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	_, _, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	time.Sleep(50 * time.Millisecond)

	newOpts := *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd1.RealTCPAddr().String()}
	nsqd.SwapOpts(&newOpts)
	nsqd.TriggerOptsNotification()
	test.Equal(t, len(nsqd.GetOpts().NSQLookupdTCPAddresses), 1)

	time.Sleep(50 * time.Millisecond)

	numLookupPeers := len(nsqdServer.lookupPeers.Load().([]*clusterinfo.LookupPeer))
	test.Equal(t, numLookupPeers, 1)

	newOpts = *opts
	newOpts.NSQLookupdTCPAddresses = []string{lookupd2.RealTCPAddr().String(), lookupd3.RealTCPAddr().String()}
	nsqd.SwapOpts(&newOpts)
	nsqd.TriggerOptsNotification()
	test.Equal(t, len(nsqd.GetOpts().NSQLookupdTCPAddresses), 2)

	time.Sleep(50 * time.Millisecond)

	var lookupPeers []string
	for _, lp := range nsqdServer.lookupPeers.Load().([]*clusterinfo.LookupPeer) {
		lookupPeers = append(lookupPeers, lp.String())
	}
	test.Equal(t, len(lookupPeers), 2)
	test.Equal(t, lookupPeers, newOpts.NSQLookupdTCPAddresses)
}

func TestCluster(t *testing.T) {
	lopts := nsqlookupd.NewOptions()
	lopts.Logger = newTestLogger(t)
	lopts.BroadcastAddress = "127.0.0.1"
	_, _, lookupd := mustStartNSQLookupd(lopts)

	opts := nsqdNs.NewOptions()
	opts.Logger = newTestLogger(t)
	opts.NSQLookupdTCPAddresses = []string{lookupd.RealTCPAddr().String()}
	opts.BroadcastAddress = "127.0.0.1"
	tcpAddr, httpAddr, nsqd, nsqdServer := mustStartNSQD(opts)
	defer os.RemoveAll(opts.DataPath)
	defer nsqdServer.Exit()

	topicName := "cluster_test" + strconv.Itoa(int(time.Now().Unix()))
	partitionStr := "0"

	hostname, err := os.Hostname()
	test.Equal(t, err, nil)

	nsqd.GetTopicIgnPart(topicName)

	url := fmt.Sprintf("http://%s/channel/create?topic=%s&channel=ch", httpAddr, topicName)
	_, err = http_api.NewClient(nil).POSTV1(url)
	test.Equal(t, err, nil)

	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	endpoint := fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr())
	data, err := API(endpoint)
	test.Equal(t, err, nil)

	t.Logf("debug data: %v", data)
	topicData := data.Get("topic:" + topicName)
	producers, _ := topicData.Array()
	test.Equal(t, len(producers), 1)

	producer := topicData.GetIndex(0)
	test.Equal(t, producer.Get("hostname").MustString(), hostname)
	test.Equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	test.Equal(t, producer.Get("tcp_port").MustInt(), tcpAddr.Port)
	test.Equal(t, producer.Get("tombstoned").MustBool(), false)

	channelData := data.Get("channel:" + topicName + ":" + partitionStr)
	producers, _ = channelData.Array()
	test.Equal(t, len(producers), 1)

	producer = topicData.GetIndex(0)
	test.Equal(t, producer.Get("hostname").MustString(), hostname)
	test.Equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	test.Equal(t, producer.Get("tcp_port").MustInt(), tcpAddr.Port)
	test.Equal(t, producer.Get("tombstoned").MustBool(), false)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName)
	data, err = API(endpoint)

	producers, _ = data.Get("producers").Array()
	test.Equal(t, len(producers), 1)

	producer = data.Get("producers").GetIndex(0)
	test.Equal(t, producer.Get("hostname").MustString(), hostname)
	test.Equal(t, producer.Get("broadcast_address").MustString(), "127.0.0.1")
	test.Equal(t, producer.Get("tcp_port").MustInt(), tcpAddr.Port)

	channels, _ := data.Get("channels").Array()
	test.Equal(t, len(channels), 1)

	channel := channels[0].(string)
	test.Equal(t, channel, "ch")

	nsqd.DeleteExistingTopic(topicName, 0)
	// allow some time for nsqd to push info to nsqlookupd
	time.Sleep(350 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", lookupd.RealHTTPAddr(), topicName)
	data, err = API(endpoint)

	test.Equal(t, err, nil)

	producers, _ = data.Get("producers").Array()
	test.Equal(t, len(producers), 0)

	endpoint = fmt.Sprintf("http://%s/debug", lookupd.RealHTTPAddr())
	data, err = API(endpoint)

	test.Equal(t, err, nil)

	producers, _ = data.Get("topic:" + topicName).Array()
	test.Equal(t, len(producers), 0)

	producers, _ = data.Get("channel:" + topicName + ":" + partitionStr).Array()
	test.Equal(t, len(producers), 0)
}
