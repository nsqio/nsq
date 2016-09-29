package nsqlookupd

import (
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/clusterinfo"
	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/bitly/go-simplejson"
)

func equal(t *testing.T, act, exp interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func nequal(t *testing.T, act, exp interface{}) {
	if reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		t.Logf("\033[31m%s:%d:\n\n\tnexp: %#v\n\n\tgot:  %#v\033[39m\n\n",
			filepath.Base(file), line, exp, act)
		t.FailNow()
	}
}

func OldRegister(topic string, channel string) *nsq.Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &nsq.Command{[]byte("REGISTER"), params, nil}
}

func OldUnRegister(topic string, channel string) *nsq.Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &nsq.Command{[]byte("UNREGISTER"), params, nil}
}

type tbLog interface {
	Log(...interface{})
}

type testLogger struct {
	tbLog
	level int32
}

func (tl *testLogger) Level() int32 {
	return tl.level
}

func (tl *testLogger) SetLevel(l int32) {
	tl.level = l
}

func (tl *testLogger) Logf(f string, args ...interface{}) {
	tl.Log(fmt.Sprintf(f, args...))
}

func (tl *testLogger) LogDebugf(f string, args ...interface{}) {
	tl.Log(fmt.Sprintf(f, args...))
}

func (tl *testLogger) LogErrorf(f string, args ...interface{}) {
	tl.Log(fmt.Sprintf(f, args...))
}

func (tl *testLogger) LogWarningf(f string, args ...interface{}) {
	tl.Log(fmt.Sprintf(f, args...))
}

func (tl *testLogger) Output(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func (tl *testLogger) OutputErr(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func (tl *testLogger) OutputWarning(maxdepth int, s string) error {
	tl.Log(s)
	return nil
}

func newTestLogger(tbl tbLog) levellogger.Logger {
	return &testLogger{tbl, 1}
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

func identify(t *testing.T, conn net.Conn, address string, tcpPort int, httpPort int, version string) {
	ci := make(map[string]interface{})
	ci["tcp_port"] = tcpPort
	ci["http_port"] = httpPort
	ci["broadcast_address"] = address
	ci["hostname"] = address
	ci["version"] = version
	ci["id"] = address
	cmd, _ := nsq.Identify(ci)
	_, err := cmd.WriteTo(conn)
	equal(t, err, nil)
	_, err = nsq.ReadResponse(conn)
	equal(t, err, nil)
}

func API(endpoint string) (data *simplejson.Json, err error) {
	d := make(map[string]interface{})
	_, err = http_api.NewClient(nil).GETV1(endpoint, &d)
	data = simplejson.New()
	data.SetPath(nil, d)
	return
}

func APIwithRetCode(endpoint string) (data *simplejson.Json, code int, err error) {
	d := make(map[string]interface{})
	code, err = http_api.NewClient(nil).GETV1(endpoint, &d)
	data = simplejson.New()
	data.SetPath(nil, d)
	return
}

func TestBasicLookupd(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicName := "connectmsg"

	conn := mustConnectLookupd(t, tcpAddr)

	tcpPort := 5000
	httpPort := 5555
	identify(t, conn, "ip.address", tcpPort, httpPort, "fake-version-HA")

	nsq.Register(topicName, "0", "channel1").WriteTo(conn)

	v, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	data, err := API(endpoint)

	t.Logf("got %v", data)
	returnedProducers, err := data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 1)

	topics := nsqlookupd.DB.FindTopicProducers(topicName, "*")
	equal(t, len(topics), 1)
	producer := topics[0].ProducerNode

	equal(t, producer.peerInfo.BroadcastAddress, "ip.address")
	equal(t, producer.peerInfo.Hostname, "ip.address")
	equal(t, producer.peerInfo.TCPPort, tcpPort)
	equal(t, producer.peerInfo.HTTPPort, httpPort)

	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	data, err = API(endpoint)

	equal(t, err, nil)
	returnedTopics, err := data.Get("topics").Array()
	t.Logf("got returnedTopics %v", returnedTopics)
	equal(t, err, nil)
	equal(t, len(returnedTopics), 1)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = API(endpoint)
	equal(t, err, nil)
	tmp, _ := data.MarshalJSON()
	t.Logf("got lookup data :%v", string(tmp))
	returnedChannels, err := data.Get("channels").Array()
	equal(t, err, nil)
	t.Logf("got returnedChannels %v", returnedChannels)
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
		equal(t, ver, "fake-version-HA")
	}

	conn.Close()
	time.Sleep(10 * time.Millisecond)

	// now there should be no producers
	data, err = API(endpoint)
	returnedChannels, err = data.Get("channels").Array()
	equal(t, err, nil)
	equal(t, len(returnedChannels), 0)
	returnedProducers, err = data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 0)
}

func TestChannelUnregister(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topics := nsqlookupd.DB.FindTopics()
	equal(t, len(topics), 0)

	topicName := "channel_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	tcpPort := 5000
	httpPort := 5555
	identify(t, conn, "ip.address", tcpPort, httpPort, "fake-version-HA")

	nsq.Register(topicName, "0", "ch1").WriteTo(conn)
	v, err := nsq.ReadResponse(conn)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	topicRegs := nsqlookupd.DB.FindTopicProducers(topicName, "*")
	equal(t, len(topicRegs), 1)

	channels := nsqlookupd.DB.FindChannelRegs(topicName, "*")
	equal(t, len(channels), 1)

	nsq.UnRegister(topicName, "0", "ch1").WriteTo(conn)
	v, err = nsq.ReadResponse(conn)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	topicRegs = nsqlookupd.DB.FindTopicProducers(topicName, "*")
	equal(t, len(topicRegs), 1)

	// we should still have mention of the topic even though there is no producer
	// (ie. we haven't *deleted* the channel, just unregistered as a producer)
	channels = nsqlookupd.DB.FindChannelRegs(topicName, "*")
	equal(t, len(channels), 0)

	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := API(endpoint)
	equal(t, err, nil)
	returnedProducers, err := data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 1)
}

func TestTombstoneRecover(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicName := "tombstone_recover"
	topicName2 := topicName + "2"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version-HA")

	nsq.Register(topicName, "0", "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	nsq.Register(topicName2, "0", "channel2").WriteTo(conn)
	_, err = nsq.ReadResponse(conn)
	equal(t, err, nil)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s",
		httpAddr, topicName, "ip.address:5555")
	_, err = http_api.NewClient(nil).POSTV1(endpoint)
	equal(t, err, nil)

	// available by default access mode
	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := API(endpoint)
	equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	equal(t, len(producers), 1)

	// unavailable by write access mode
	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s&access=w", httpAddr, topicName)
	data, err = API(endpoint)
	equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 0)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName2)
	data, err = API(endpoint)
	equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 1)

	time.Sleep(75 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = API(endpoint)
	equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 1)
}

func TestTombstoneUnregister(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicName := "tombstone_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version-HA")

	nsq.Register(topicName, "0", "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s",
		httpAddr, topicName, "ip.address:5555")
	_, err = http_api.NewClient(nil).POSTV1(endpoint)
	equal(t, err, nil)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s&access=w", httpAddr, topicName)
	data, err := API(endpoint)
	equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	equal(t, len(producers), 0)

	nsq.UnRegister(topicName, "0", "").WriteTo(conn)
	_, err = nsq.ReadResponse(conn)
	equal(t, err, nil)

	time.Sleep(55 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s&access=w", httpAddr, topicName)
	data, err = API(endpoint)
	equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	equal(t, len(producers), 0)
}

func TestInactiveNodes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	opts.InactiveProducerTimeout = 200 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version-HA")

	nsq.Register(topicName, "0", "channel1").WriteTo(conn)
	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	ci := clusterinfo.New(nil, http_api.NewClient(nil))

	producers, _ := ci.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 1)
	equal(t, len(producers[0].Topics), 1)
	equal(t, producers[0].Topics[0].Topic, topicName)
	equal(t, producers[0].Topics[0].Tombstoned, false)

	time.Sleep(250 * time.Millisecond)

	producers, _ = ci.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 0)
}

func TestTombstonedNodes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version-HA")

	nsq.Register(topicName, "0", "channel1").WriteTo(conn)

	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	ci := clusterinfo.New(nil, http_api.NewClient(nil))

	producers, _ := ci.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 1)
	equal(t, len(producers[0].Topics), 1)
	equal(t, producers[0].Topics[0].Topic, topicName)
	equal(t, producers[0].Topics[0].Tombstoned, false)

	endpoint := fmt.Sprintf("http://%s/topic/tombstone?topic=%s&node=%s",
		httpAddr, topicName, "ip.address:5555")
	_, err = http_api.NewClient(nil).POSTV1(endpoint)
	equal(t, err, nil)

	producers, _ = ci.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 1)
	equal(t, len(producers[0].Topics), 1)
	equal(t, producers[0].Topics[0].Topic, topicName)
	equal(t, producers[0].Topics[0].Tombstoned, true)
}

func TestTopicPartitions(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicList := nsqlookupd.DB.FindTopics()
	equal(t, len(topicList), 0)

	topicName := "topic_partitions"

	conn_p1 := mustConnectLookupd(t, tcpAddr)
	conn_p2 := mustConnectLookupd(t, tcpAddr)
	conn_p1_ip := "ip.address.p1"
	conn_p2_ip := "ip.address.p2"
	fakeVer := "fake-version"

	tcpPort := 5000
	httpPort := 5555
	identify(t, conn_p1, conn_p1_ip, tcpPort, httpPort, fakeVer)
	identify(t, conn_p2, conn_p2_ip, tcpPort, httpPort, fakeVer)

	nsq.Register(topicName, "0", "channel1").WriteTo(conn_p1)
	nsq.Register(topicName, "1", "channel1").WriteTo(conn_p2)

	v, err := nsq.ReadResponse(conn_p1)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))
	v, err = nsq.ReadResponse(conn_p2)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	data, err := API(endpoint)

	t.Logf("got %v", data)
	returnedProducers, err := data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 2)
	for i, _ := range returnedProducers {
		retP, err := data.Get("producers").GetIndex(i).Get("partitions").Map()
		equal(t, err, nil)
		equal(t, len(retP), 1)
		t.Logf("node partitions %v", retP)
		plist, err := data.Get("producers").GetIndex(i).Get("partitions").Get(topicName).Array()
		equal(t, err, nil)
		equal(t, len(plist), 1)
	}

	topics := nsqlookupd.DB.FindTopicProducers(topicName, "*")
	equal(t, len(topics), 2)
	topic_p1 := nsqlookupd.DB.FindTopicProducers(topicName, "0")
	equal(t, len(topic_p1), 1)
	topic_p2 := nsqlookupd.DB.FindTopicProducers(topicName, "1")
	equal(t, len(topic_p2), 1)

	producer := topics[0].ProducerNode

	if producer.peerInfo.BroadcastAddress == conn_p1_ip {
		equal(t, producer.peerInfo.Hostname, conn_p1_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)
		producer = topics[1].ProducerNode

		equal(t, producer.peerInfo.BroadcastAddress, conn_p2_ip)
		equal(t, producer.peerInfo.Hostname, conn_p2_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)
	} else {
		equal(t, producer.peerInfo.BroadcastAddress, conn_p2_ip)
		equal(t, producer.peerInfo.Hostname, conn_p2_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)
		producer = topics[1].ProducerNode

		equal(t, producer.peerInfo.BroadcastAddress, conn_p1_ip)
		equal(t, producer.peerInfo.Hostname, conn_p1_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)

	}
	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	data, err = API(endpoint)

	equal(t, err, nil)
	returnedTopics, err := data.Get("topics").Array()
	t.Logf("got returnedTopics %v", returnedTopics)
	equal(t, err, nil)
	equal(t, len(returnedTopics), 1)

	// test get first topic partition
	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s&partition=0", httpAddr, topicName)
	data, err = API(endpoint)

	equal(t, err, nil)
	returnedChannels, err := data.Get("channels").Array()
	equal(t, err, nil)
	equal(t, len(returnedChannels), 1)

	retPartitions, err := data.Get("partitions").Map()
	equal(t, err, nil)
	t.Logf("got returnedPartitions %v", retPartitions)
	equal(t, len(retPartitions), 1)
	partData := simplejson.New()
	partData.SetPath(nil, retPartitions["0"])
	broadcastaddress_p1, err := partData.Get("broadcast_address").String()
	equal(t, err, nil)
	equal(t, broadcastaddress_p1, conn_p1_ip)

	// test get topic all partitions
	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = API(endpoint)
	equal(t, err, nil)

	retPartitions, err = data.Get("partitions").Map()
	equal(t, err, nil)
	t.Logf("got returnedPartitions %v", retPartitions)
	equal(t, len(retPartitions), 2)
	partData.SetPath(nil, retPartitions["1"])
	broadcastaddress_p2, err := partData.Get("broadcast_address").String()
	equal(t, err, nil)
	equal(t, broadcastaddress_p2, conn_p2_ip)

	returnedProducers, err = data.Get("producers").Array()
	t.Logf("got returnedProducers %v", returnedProducers)
	equal(t, err, nil)
	equal(t, len(returnedProducers), 2)
	iplist := make(map[string]struct{})
	iplist[conn_p1_ip] = struct{}{}
	iplist[conn_p2_ip] = struct{}{}

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
		_, ok := iplist[broadcastaddress]
		equal(t, ok, true)
		delete(iplist, broadcastaddress)

		ver, err := producer.Get("version").String()
		equal(t, err, nil)
		equal(t, ver, fakeVer)
	}

	conn_p1.Close()
	conn_p2.Close()
}

func TestTopicChannelRegUnReg(t *testing.T) {
}

func TestTombstonedOldNodes(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	defer conn.Close()

	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	OldRegister(topicName, "channel1").WriteTo(conn)

	_, err := nsq.ReadResponse(conn)
	equal(t, err, nil)

	ci := clusterinfo.New(nil, http_api.NewClient(nil))

	producers, _ := ci.GetLookupdProducers(lookupdHTTPAddrs)
	equal(t, len(producers), 1)
	equal(t, len(producers[0].Topics), 1)
	equal(t, producers[0].Topics[0].Topic, topicName)
	equal(t, producers[0].Topics[0].Tombstoned, true)
}

func TestOldNsqdTopicRegUnReg(t *testing.T) {
	opts := NewOptions()
	opts.Logger = newTestLogger(t)
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(opts)
	defer nsqlookupd.Exit()

	topicList := nsqlookupd.DB.FindTopics()
	equal(t, len(topicList), 0)

	topicName := "topic_partitions"

	conn_p1 := mustConnectLookupd(t, tcpAddr)
	conn_p2 := mustConnectLookupd(t, tcpAddr)
	conn_p1_ip := "ip.address.p1"
	conn_p2_ip := "ip.address.p2"
	fakeVer := "fake-version"

	tcpPort := 5000
	httpPort := 5555
	identify(t, conn_p1, conn_p1_ip, tcpPort, httpPort, fakeVer)
	identify(t, conn_p2, conn_p2_ip, tcpPort, httpPort, fakeVer)

	OldRegister(topicName, "channel1").WriteTo(conn_p1)
	OldRegister(topicName, "channel1").WriteTo(conn_p2)

	v, err := nsq.ReadResponse(conn_p1)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))
	v, err = nsq.ReadResponse(conn_p2)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	data, err := API(endpoint)

	t.Logf("got %v", data)
	returnedProducers, err := data.Get("producers").Array()
	equal(t, err, nil)
	equal(t, len(returnedProducers), 2)
	for i, _ := range returnedProducers {
		retP, err := data.Get("producers").GetIndex(i).Get("partitions").Map()
		equal(t, err, nil)
		equal(t, len(retP), 1)
		t.Logf("node partitions %v", retP)
		plist, err := data.Get("producers").GetIndex(i).Get("partitions").Get(topicName).Array()
		equal(t, err, nil)
		equal(t, len(plist), 1)
	}

	topics := nsqlookupd.DB.FindTopicProducers(topicName, "*")
	equal(t, len(topics), 2)
	topic_p1 := nsqlookupd.DB.FindTopicProducers(topicName, "0")
	equal(t, len(topic_p1), 0)
	topic_p2 := nsqlookupd.DB.FindTopicProducers(topicName, "1")
	equal(t, len(topic_p2), 0)
	topic_p2 = nsqlookupd.DB.FindTopicProducers(topicName, strconv.Itoa(OLD_VERSION_PID))
	equal(t, true, len(topic_p2) > 0)

	producer := topics[0].ProducerNode

	if producer.peerInfo.BroadcastAddress == conn_p1_ip {
		equal(t, producer.peerInfo.Hostname, conn_p1_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)
		producer = topics[1].ProducerNode

		equal(t, producer.peerInfo.BroadcastAddress, conn_p2_ip)
		equal(t, producer.peerInfo.Hostname, conn_p2_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)
	} else {
		equal(t, producer.peerInfo.BroadcastAddress, conn_p2_ip)
		equal(t, producer.peerInfo.Hostname, conn_p2_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)
		producer = topics[1].ProducerNode

		equal(t, producer.peerInfo.BroadcastAddress, conn_p1_ip)
		equal(t, producer.peerInfo.Hostname, conn_p1_ip)
		equal(t, producer.peerInfo.TCPPort, tcpPort)
		equal(t, producer.peerInfo.HTTPPort, httpPort)
	}
	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	data, err = API(endpoint)

	equal(t, err, nil)
	returnedTopics, err := data.Get("topics").Array()
	t.Logf("got returnedTopics %v", returnedTopics)
	equal(t, err, nil)
	equal(t, len(returnedTopics), 1)

	// test get topic all partitions
	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = API(endpoint)
	equal(t, err, nil)

	retPartitions, err := data.Get("partitions").Map()
	equal(t, err, nil)
	t.Logf("got returnedPartitions %v", retPartitions)
	equal(t, true, len(retPartitions) > 0)

	returnedProducers, err = data.Get("producers").Array()
	t.Logf("got returnedProducers %v", returnedProducers)
	equal(t, err, nil)
	equal(t, len(returnedProducers), 2)
	iplist := make(map[string]struct{})
	iplist[conn_p1_ip] = struct{}{}
	iplist[conn_p2_ip] = struct{}{}

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
		_, ok := iplist[broadcastaddress]
		equal(t, ok, true)
		delete(iplist, broadcastaddress)

		ver, err := producer.Get("version").String()
		equal(t, err, nil)
		equal(t, ver, fakeVer)
	}

	OldUnRegister(topicName, "channel1").WriteTo(conn_p1)
	OldUnRegister(topicName, "channel1").WriteTo(conn_p2)
	v, err = nsq.ReadResponse(conn_p1)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))
	v, err = nsq.ReadResponse(conn_p2)
	equal(t, err, nil)
	equal(t, v, []byte("OK"))

	conn_p1.Close()
	conn_p2.Close()
}

// TODO: test performance for db
func BenchmarkTopicProducerLookup(t *testing.B) {
}

func BenchmarkTopicChannelRegUnReg(b *testing.B) {
}
