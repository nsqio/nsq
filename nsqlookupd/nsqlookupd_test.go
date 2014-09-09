package nsqlookupd

import (
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/internal/http_api"
	lookuputil "github.com/bitly/nsq/internal/lookupd"
)

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) logger {
	return &testLogger{tbl}
}

func mustStartLookupd(opts *nsqlookupdOptions) (*net.TCPAddr, *net.TCPAddr, *NSQLookupd) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	nsqlookupd := NewNSQLookupd(opts)
	nsqlookupd.Main()
	return nsqlookupd.tcpListener.Addr().(*net.TCPAddr),
		nsqlookupd.httpListener.Addr().(*net.TCPAddr),
		nsqlookupd
}

func mustConnectLookupd(t *testing.T, tcpAddr *net.TCPAddr) net.Conn {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		t.Fatal("failed to connect to lookupd")
	}
	conn.Write(nsq.MagicV1)
	return conn
}

func identify(t *testing.T, conn net.Conn, address string, tcpPort int, httpPort int, version string) {
	ci := make(map[string]interface{})
	ci["tcp_port"] = tcpPort
	ci["http_port"] = httpPort
	ci["broadcast_address"] = address
	ci["hostname"] = address
	ci["version"] = version
	cmd, _ := nsq.Identify(ci)
	_, err := cmd.WriteTo(conn)
	equal(t, err, nil)
	_, err = nsq.ReadResponse(conn)
	equal(t, err, nil)
}

func TestBasicLookupd(t *testing.T) {
	opts := NewNSQLookupdOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topics := nsqlookupd.DB.FindRegistrations("topic", "*", "*")
	equal(t, len(topics), 0)

	topicName := "connectmsg"

	conn := mustConnectLookupd(t, tcpAddr)

	tcpPort := 5000
	httpPort := 5555
	identify(t, conn, "ip.address", tcpPort, httpPort, "fake-version")

	nsq.Register(topicName, "channel1").WriteTo(conn)
	v, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	data, err := http_api.NegotiateV1("GET", endpoint, nil)
	t.Logf("got %v", data)
	returnedProducers, err := data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 1)

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	equal(t, len(topics), 1)

	producers := nsqlookupd.DB.FindProducers("topic", topicName, "")
	equal(t, len(producers), 1)
	producer := producers[0]

	equal(t, producer.peerInfo.BroadcastAddress, "ip.address")
	equal(t, producer.peerInfo.Hostname, "ip.address")
	equal(t, producer.peerInfo.TCPPort, tcpPort)
	equal(t, producer.peerInfo.HTTPPort, httpPort)

	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	data, err = http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	returnedTopics, err := data.Get("topics").Array()
	t.Logf("got returnedTopics %v", returnedTopics)
	equal(t, err, nil)
	equal(t, len(returnedTopics), 1)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	returnedChannels, err := data.Get("channels").Array()
	equal(t, err, nil)
	equal(t, len(returnedChannels), 1)

	returnedProducers, err = data.Get("producers").Array()
	t.Logf("got returnedProducers %v", returnedProducers)
	equal(t, err, nil)
	equal(t, len(returnedProducers), 1)
	for i := range returnedProducers {
		producer := data.Get("producers").GetIndex(i)
		t.Logf("producer %v", producer)

		port, err := producer.Get("tcp_port").Int()
		equal(t, err, nil)
		equal(t, port, tcpPort)

		port, err = producer.Get("http_port").Int()
		equal(t, err, nil)
		equal(t, port, httpPort)

		broadcastaddress, err := producer.Get("broadcast_address").String()
		equal(t, err, nil)
		equal(t, broadcastaddress, "ip.address")

		ver, err := producer.Get("version").String()
		equal(t, err, nil)
		equal(t, ver, "fake-version")
	}

	conn.Close()
	time.Sleep(10 * time.Millisecond)

	// now there should be no producers, but still topic/channel entries
	data, err = http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	returnedChannels, err = data.Get("channels").Array()
	equal(t, err, nil)
	equal(t, len(returnedChannels), 1)
	returnedProducers, err = data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 0)
}

func TestChannelUnregister(t *testing.T) {
	opts := NewNSQLookupdOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topics := nsqlookupd.DB.FindRegistrations("topic", "*", "*")
	equal(t, len(topics), 0)

	topicName := "channel_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	tcpPort := 5000
	httpPort := 5555
	identify(t, conn, "ip.address", tcpPort, httpPort, "fake-version")

	nsq.Register(topicName, "ch1").WriteTo(conn)
	v, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	equal(t, len(topics), 1)

	channels := nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	equal(t, len(channels), 1)

	nsq.UnRegister(topicName, "ch1").WriteTo(conn)
	v, err = nsq.ReadResponse(conn)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	equal(t, len(topics), 1)

	// we should still have mention of the topic even though there is no producer
	// (ie. we haven't *deleted* the channel, just unregistered as a producer)
	channels = nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	equal(t, len(channels), 1)

	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	returnedProducers, err := data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 1)
}

func TestTombstoneRecover(t *testing.T) {
	opts := NewNSQLookupdOptions()
	opts.Logger = newTestLogger(t)
	opts.TombstoneLifetime = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicName := "tombstone_recover"
	topicName2 := topicName + "2"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	nsq.Register(topicName2, "channel2").WriteTo(conn)
	_, err = nsq.ReadResponse(conn)
	equal(t, err, nil)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s",
		httpAddr, topicName, "ip.address:5555")
	_, err = http_api.NegotiateV1("POST", endpoint, nil)
	equal(t, err, nil)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	equal(t, len(producers), 0)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName2)
	data, err = http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 1)

	time.Sleep(75 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 1)
}

func TestTombstoneUnregister(t *testing.T) {
	opts := NewNSQLookupdOptions()
	opts.Logger = newTestLogger(t)
	opts.TombstoneLifetime = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicName := "tombstone_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s",
		httpAddr, topicName, "ip.address:5555")
	_, err = http_api.NegotiateV1("POST", endpoint, nil)
	equal(t, err, nil)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	equal(t, len(producers), 0)

	nsq.UnRegister(topicName, "").WriteTo(conn)
	_, err = nsq.ReadResponse(conn)
	equal(t, err, nil)

	time.Sleep(55 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = http_api.NegotiateV1("GET", endpoint, nil)
	equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 0)
}

func TestInactiveNodes(t *testing.T) {
	opts := NewNSQLookupdOptions()
	opts.Logger = newTestLogger(t)
	opts.InactiveProducerTimeout = 200 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	producers, _ := lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 1)
	equal(t, len(producers[0].Topics), 1)
	equal(t, producers[0].Topics[0].Topic, topicName)
	equal(t, producers[0].Topics[0].Tombstoned, false)

	time.Sleep(250 * time.Millisecond)

	producers, _ = lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 0)
}

func TestTombstonedNodes(t *testing.T) {
	opts := NewNSQLookupdOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	producers, _ := lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 1)
	equal(t, len(producers[0].Topics), 1)
	equal(t, producers[0].Topics[0].Topic, topicName)
	equal(t, producers[0].Topics[0].Tombstoned, false)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s",
		httpAddr, topicName, "ip.address:5555")
	_, err = http_api.NegotiateV1("POST", endpoint, nil)
	equal(t, err, nil)

	producers, _ = lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 1)
	equal(t, len(producers[0].Topics), 1)
	equal(t, producers[0].Topics[0].Topic, topicName)
	equal(t, producers[0].Topics[0].Tombstoned, true)
}
