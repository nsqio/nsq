package nsqlookupd

import (
	"fmt"

	"github.com/bitly/nsq/util"
	lookuputil "github.com/bitly/nsq/util/lookupd"
	"github.com/bmizerany/assert"
	"github.com/bitly/go-nsq"

	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

func mustStartLookupd(options *nsqlookupdOptions) (*net.TCPAddr, *net.TCPAddr, *NSQLookupd) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	httpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")

	nsqlookupd := NewNSQLookupd(options)
	nsqlookupd.tcpAddr = tcpAddr
	nsqlookupd.httpAddr = httpAddr
	nsqlookupd.Main()

	return nsqlookupd.tcpListener.Addr().(*net.TCPAddr), nsqlookupd.httpListener.Addr().(*net.TCPAddr), nsqlookupd
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
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
	_, err = nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
}

func TestBasicLookupd(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(NewNSQLookupdOptions())
	defer nsqlookupd.Exit()

	topics := nsqlookupd.DB.FindRegistrations("topic", "*", "*")
	assert.Equal(t, len(topics), 0)

	topicName := "connectmsg"

	conn := mustConnectLookupd(t, tcpAddr)
	tcpPort := 5000
	httpPort := 5555
	identify(t, conn, "ip.address", tcpPort, httpPort, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)
	v, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("OK"))

	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	data, err := util.ApiRequest(endpoint)
	log.Printf("got %v", data)
	returnedProducers, err := data.Get("producers").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 1)

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	assert.Equal(t, len(topics), 1)

	producers := nsqlookupd.DB.FindProducers("topic", topicName, "")
	assert.Equal(t, len(producers), 1)
	producer := producers[0]

	assert.Equal(t, producer.peerInfo.BroadcastAddress, "ip.address")
	assert.Equal(t, producer.peerInfo.Hostname, "ip.address")
	assert.Equal(t, producer.peerInfo.TcpPort, tcpPort)
	assert.Equal(t, producer.peerInfo.HttpPort, httpPort)

	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	data, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedTopics, err := data.Get("topics").Array()
	log.Printf("got returnedTopics %v", returnedTopics)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedTopics), 1)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedChannels, err := data.Get("channels").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedChannels), 1)

	returnedProducers, err = data.Get("producers").Array()
	log.Printf("got returnedProducers %v", returnedProducers)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 1)
	for i := range returnedProducers {
		producer := data.Get("producers").GetIndex(i)
		log.Printf("producer %v", producer)

		port, err := producer.Get("tcp_port").Int()
		assert.Equal(t, err, nil)
		assert.Equal(t, port, tcpPort)

		port, err = producer.Get("http_port").Int()
		assert.Equal(t, err, nil)
		assert.Equal(t, port, httpPort)

		broadcastaddress, err := producer.Get("broadcast_address").String()
		assert.Equal(t, err, nil)
		assert.Equal(t, broadcastaddress, "ip.address")

		ver, err := producer.Get("version").String()
		assert.Equal(t, err, nil)
		assert.Equal(t, ver, "fake-version")
	}

	conn.Close()
	time.Sleep(10 * time.Millisecond)

	// now there should be no producers, but still topic/channel entries
	data, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedChannels, err = data.Get("channels").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedChannels), 1)
	returnedProducers, err = data.Get("producers").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 0)
}

func TestChannelUnregister(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(NewNSQLookupdOptions())
	defer nsqlookupd.Exit()

	topics := nsqlookupd.DB.FindRegistrations("topic", "*", "*")
	assert.Equal(t, len(topics), 0)

	topicName := "channel_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	tcpPort := 5000
	httpPort := 5555
	identify(t, conn, "ip.address", tcpPort, httpPort, "fake-version")

	nsq.Register(topicName, "ch1").Write(conn)
	v, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("OK"))

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	assert.Equal(t, len(topics), 1)

	channels := nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	assert.Equal(t, len(channels), 1)

	nsq.UnRegister(topicName, "ch1").Write(conn)
	v, err = nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("OK"))

	topics = nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	assert.Equal(t, len(topics), 1)

	// we should still have mention of the topic even though there is no producer
	// (ie. we haven't *deleted* the channel, just unregistered as a producer)
	channels = nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	assert.Equal(t, len(channels), 1)

	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedProducers, err := data.Get("producers").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 1)
}

func TestTombstoneRecover(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQLookupdOptions()
	options.TombstoneLifetime = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(options)
	defer nsqlookupd.Exit()

	topicName := "tombstone_recover"
	topicName2 := topicName + "2"

	conn := mustConnectLookupd(t, tcpAddr)
	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)
	_, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)

	nsq.Register(topicName2, "channel2").Write(conn)
	_, err = nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)

	endpoint := fmt.Sprintf("http://%s/tombstone_topic_producer?topic=%s&node=%s", httpAddr, topicName, "ip.address:5555")
	_, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	assert.Equal(t, len(producers), 0)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName2)
	data, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 1)

	time.Sleep(55 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 1)
}

func TestTombstoneUnregister(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQLookupdOptions()
	options.TombstoneLifetime = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(options)
	defer nsqlookupd.Exit()

	topicName := "tombstone_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)
	_, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)

	endpoint := fmt.Sprintf("http://%s/tombstone_topic_producer?topic=%s&node=%s", httpAddr, topicName, "ip.address:5555")
	_, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	assert.Equal(t, len(producers), 0)

	nsq.UnRegister(topicName, "").Write(conn)
	_, err = nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)

	time.Sleep(55 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 0)
}

func TestInactiveNodes(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQLookupdOptions()
	options.InactiveProducerTimeout = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(options)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)
	_, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)

	producers, _ := lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	assert.Equal(t, len(producers), 1)
	assert.Equal(t, len(producers[0].Topics), 1)
	assert.Equal(t, producers[0].Topics[0].Topic, topicName)
	assert.Equal(t, producers[0].Topics[0].Tombstoned, false)

	time.Sleep(55 * time.Millisecond)

	producers, _ = lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	assert.Equal(t, len(producers), 0)
}

func TestTombstonedNodes(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNSQLookupdOptions()
	options.InactiveProducerTimeout = 50 * time.Millisecond
	tcpAddr, httpAddr, nsqlookupd := mustStartLookupd(options)
	defer nsqlookupd.Exit()

	lookupdHTTPAddrs := []string{fmt.Sprintf("%s", httpAddr)}

	topicName := "inactive_nodes"

	conn := mustConnectLookupd(t, tcpAddr)
	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)
	_, err := nsq.ReadResponse(conn)
	assert.Equal(t, err, nil)

	producers, _ := lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	assert.Equal(t, len(producers), 1)
	assert.Equal(t, len(producers[0].Topics), 1)
	assert.Equal(t, producers[0].Topics[0].Topic, topicName)
	assert.Equal(t, producers[0].Topics[0].Tombstoned, false)

	endpoint := fmt.Sprintf("http://%s/tombstone_topic_producer?topic=%s&node=%s", httpAddr, topicName, "ip.address:5555")
	_, err = util.ApiRequest(endpoint)
	assert.Equal(t, err, nil)

	producers, _ = lookuputil.GetLookupdProducers(lookupdHTTPAddrs)
	assert.Equal(t, len(producers), 1)
	assert.Equal(t, len(producers[0].Topics), 1)
	assert.Equal(t, producers[0].Topics[0].Topic, topicName)
	assert.Equal(t, producers[0].Topics[0].Tombstoned, true)
}
