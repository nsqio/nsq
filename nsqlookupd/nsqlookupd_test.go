package nsqlookupd

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/test"
)

const (
	ConnectTimeout = 2 * time.Second
	RequestTimeout = 5 * time.Second
	TCPPort        = 5000
	HTTPPort       = 5555
	HostAddr       = "ip.address"
	NSQDVersion    = "fake-version"
)

type ProducersDoc struct {
	Producers []interface{} `json:"producers"`
}

type TopicsDoc struct {
	Topics []interface{} `json:"topics"`
}

type LookupDoc struct {
	Channels  []interface{} `json:"channels"`
	Producers []*PeerInfo   `json:"producers"`
}

func mustStartLookupd(opts *Options) (*net.TCPAddr, *net.TCPAddr, *NSQLookupd) {
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	nsqlookupd := New(opts)
	nsqlookupd.Main()
	return nsqlookupd.RealTCPAddr(), nsqlookupd.RealHTTPAddr(), nsqlookupd
}

func mustConnectLookupd(t *testing.T, tcpAddr *net.TCPAddr) net.Conn {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		t.Fatal("failed to connect to lookupd")
	}
	conn.Write(nsq.MagicV1)
	return conn
}

func identify(t *testing.T, conn net.Conn) {
	ci := make(map[string]interface{})
	ci["tcp_port"] = TCPPort
	ci["http_port"] = HTTPPort
	ci["broadcast_address"] = HostAddr
	ci["hostname"] = HostAddr
	ci["version"] = NSQDVersion
	cmd, _ := nsq.Identify(ci)
	_, err := cmd.WriteTo(conn)
	test.Nil(t, err)
	_, err = nsq.ReadResponse(conn)
	test.Nil(t, err)
}

func TestNoLogger(t *testing.T) {
	opts := NewOptions()
	opts.Logger = nil
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	nsqlookupd := New(opts)

	nsqlookupd.logf("should never be logged")
}

func TestBasicLookupd(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topics := nsqlookupd.DB.FindRegistrations("topic", "*", "*")
	test.Equal(t, 0, len(topics))

	topicName := "connectmsg"

	conn := mustConnectLookupd(t, tcpAddr)

	identify(t, conn)

	nsq.Register(topicName, "channel1").WriteTo(conn)
	v, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	test.Equal(t, []byte("OK"), v)

	pr := ProducersDoc{}
	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &pr)
	test.Nil(t, err)

	t.Logf("got %v", pr)
	test.Equal(t, 1, len(pr.Producers))

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	test.Equal(t, 1, len(topics))

	producers := nsqlookupd.DB.FindProducers("topic", topicName, "")
	test.Equal(t, 1, len(producers))
	producer := producers[0]

	test.Equal(t, HostAddr, producer.peerInfo.BroadcastAddress)
	test.Equal(t, HostAddr, producer.peerInfo.Hostname)
	test.Equal(t, TCPPort, producer.peerInfo.TCPPort)
	test.Equal(t, HTTPPort, producer.peerInfo.HTTPPort)

	tr := TopicsDoc{}
	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &tr)
	test.Nil(t, err)

	t.Logf("got %v", tr)
	test.Equal(t, 1, len(tr.Topics))

	lr := LookupDoc{}
	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &lr)
	test.Nil(t, err)

	t.Logf("got %v", lr)
	test.Equal(t, 1, len(lr.Channels))
	test.Equal(t, 1, len(lr.Producers))
	for _, p := range lr.Producers {
		test.Equal(t, TCPPort, p.TCPPort)
		test.Equal(t, HTTPPort, p.HTTPPort)
		test.Equal(t, HostAddr, p.BroadcastAddress)
		test.Equal(t, NSQDVersion, p.Version)
	}

	conn.Close()
	time.Sleep(10 * time.Millisecond)

	// now there should be no producers, but still topic/channel entries
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &lr)
	test.Nil(t, err)

	test.Equal(t, 1, len(lr.Channels))
	test.Equal(t, 0, len(lr.Producers))
}

func TestChannelUnregister(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topics := nsqlookupd.DB.FindRegistrations("topic", "*", "*")
	test.Equal(t, 0, len(topics))

	topicName := "channel_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn)

	nsq.Register(topicName, "ch1").WriteTo(conn)
	v, err := nsq.ReadResponse(conn)
	test.Nil(t, err)
	test.Equal(t, []byte("OK"), v)

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	test.Equal(t, 1, len(topics))

	channels := nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	test.Equal(t, 1, len(channels))

	nsq.UnRegister(topicName, "ch1").WriteTo(conn)
	v, err = nsq.ReadResponse(conn)
	test.Nil(t, err)
	test.Equal(t, []byte("OK"), v)

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	test.Equal(t, 1, len(topics))

	// we should still have mention of the topic even though there is no producer
	// (ie. we haven't *deleted* the channel, just unregistered as a producer)
	channels = nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	test.Equal(t, 1, len(channels))

	pr := ProducersDoc{}
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &pr)
	test.Nil(t, err)
	t.Logf("got %v", pr)
	test.Equal(t, 1, len(pr.Producers))
}

func TestTombstoneRecover(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.TombstoneLifetime = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicName := "tombstone_recover"
	topicName2 := topicName + "2"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn)

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	test.Nil(t, err)

	nsq.Register(topicName2, "channel2").WriteTo(conn)
	_, err = nsq.ReadResponse(conn)
	test.Nil(t, err)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s:%d",
		httpAddr, topicName, HostAddr, HTTPPort)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(endpoint)
	test.Nil(t, err)

	pr := ProducersDoc{}

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &pr)
	test.Nil(t, err)
	test.Equal(t, 0, len(pr.Producers))

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName2)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &pr)
	test.Nil(t, err)
	test.Equal(t, 1, len(pr.Producers))

	time.Sleep(75 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &pr)
	test.Nil(t, err)
	test.Equal(t, 1, len(pr.Producers))
}

func TestTombstoneUnregister(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.TombstoneLifetime = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicName := "tombstone_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn)

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	test.Nil(t, err)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s:%d",
		httpAddr, topicName, HostAddr, HTTPPort)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(endpoint)
	test.Nil(t, err)

	pr := ProducersDoc{}

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &pr)
	test.Nil(t, err)
	test.Equal(t, 0, len(pr.Producers))

	nsq.UnRegister(topicName, "").WriteTo(conn)
	_, err = nsq.ReadResponse(conn)
	test.Nil(t, err)

	time.Sleep(55 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).NegotiateV1(endpoint, &pr)
	test.Nil(t, err)
	test.Equal(t, 0, len(pr.Producers))
}

func TestInactiveNodes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	opts.InactiveProducerTimeout = 200 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn)

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	test.Nil(t, err)

	ci := clusterinfo.New(nil, http_api.NewClient(nil, ConnectTimeout, RequestTimeout))

	producers, _ := ci.GetLookupdProducers(lookupdHTTPAddrs)
	test.Equal(t, 1, len(producers))
	test.Equal(t, 1, len(producers[0].Topics))
	test.Equal(t, topicName, producers[0].Topics[0].Topic)
	test.Equal(t, false, producers[0].Topics[0].Tombstoned)

	time.Sleep(250 * time.Millisecond)

	producers, _ = ci.GetLookupdProducers(lookupdHTTPAddrs)
	test.Equal(t, 0, len(producers))
}

func TestTombstonedNodes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = test.NewTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn)

	nsq.Register(topicName, "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	test.Nil(t, err)

	ci := clusterinfo.New(nil, http_api.NewClient(nil, ConnectTimeout, RequestTimeout))

	producers, _ := ci.GetLookupdProducers(lookupdHTTPAddrs)
	test.Equal(t, 1, len(producers))
	test.Equal(t, 1, len(producers[0].Topics))
	test.Equal(t, topicName, producers[0].Topics[0].Topic)
	test.Equal(t, false, producers[0].Topics[0].Tombstoned)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s:%d",
		httpAddr, topicName, HostAddr, HTTPPort)
	err = http_api.NewClient(nil, ConnectTimeout, RequestTimeout).POSTV1(endpoint)
	test.Nil(t, err)

	producers, _ = ci.GetLookupdProducers(lookupdHTTPAddrs)
	test.Equal(t, 1, len(producers))
	test.Equal(t, 1, len(producers[0].Topics))
	test.Equal(t, topicName, producers[0].Topics[0].Topic)
	test.Equal(t, true, producers[0].Topics[0].Tombstoned)
}
