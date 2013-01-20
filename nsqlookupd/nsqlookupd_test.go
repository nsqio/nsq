package main

import (
	"../nsq"
	"fmt"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"net"
	"os"
	"testing"
	"time"
)

func mustStartLookupd() (*net.TCPAddr, *net.TCPAddr) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	httpAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0")

	lookupd = NewNSQLookupd()
	lookupd.tcpAddr = tcpAddr
	lookupd.httpAddr = httpAddr
	lookupd.Main()

	return lookupd.tcpListener.Addr().(*net.TCPAddr), lookupd.httpListener.Addr().(*net.TCPAddr)
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
	ci["address"] = address
	ci["version"] = version
	cmd, _ := nsq.Identify(ci)
	err := cmd.Write(conn)
	assert.Equal(t, err, nil)
}

func TestBasicLookupd(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, httpAddr := mustStartLookupd()
	defer lookupd.Exit()

	topics := lookupd.DB.FindRegistrations("topic", "*", "*")
	assert.Equal(t, len(topics), 0)

	topicName := "connectmsg"

	conn := mustConnectLookupd(t, tcpAddr)
	tcpPort := 5000
	httpPort := 5555
	identify(t, conn, "ip.address", tcpPort, httpPort, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)

	time.Sleep(10 * time.Millisecond)

	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	data, err := nsq.ApiRequest(endpoint)
	log.Printf("got %v", data)
	returnedProducers, err := data.Get("producers").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 1)

	topics = lookupd.DB.FindRegistrations("topic", topicName, "")
	assert.Equal(t, len(topics), 1)

	producers := lookupd.DB.FindProducers("topic", topicName, "")
	assert.Equal(t, len(producers), 1)
	producer := producers[0]

	assert.Equal(t, producer.Address, "ip.address")
	assert.Equal(t, producer.TcpPort, tcpPort)
	assert.Equal(t, producer.HttpPort, httpPort)

	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	data, err = nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedTopics, err := data.Get("topics").Array()
	log.Printf("got returnedTopics %v", returnedTopics)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedTopics), 1)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedChannels, err := data.Get("channels").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedChannels), 1)

	returnedProducers, err = data.Get("producers").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 1)
	for i, _ := range returnedProducers {
		producer := data.Get("producers").GetIndex(i)
		log.Printf("producer %v", producer)
		assert.Equal(t, err, nil)
		port, err := producer.Get("tcp_port").Int()
		assert.Equal(t, err, nil)
		assert.Equal(t, port, tcpPort)
		port, err = producer.Get("http_port").Int()
		assert.Equal(t, err, nil)
		assert.Equal(t, port, httpPort)
		address, err := producer.Get("address").String()
		assert.Equal(t, err, nil)
		assert.Equal(t, address, "ip.address")
		ver, err := producer.Get("version").String()
		assert.Equal(t, err, nil)
		assert.Equal(t, ver, "fake-version")
	}

	conn.Close()
	time.Sleep(10 * time.Millisecond)

	// now there should be no producers, but still topic/channel entries
	data, err = nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedChannels, err = data.Get("channels").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedChannels), 1)
	returnedProducers, err = data.Get("producers").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 0)
}

func TestTombstoneRecover(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, httpAddr := mustStartLookupd()
	defer lookupd.Exit()
	lookupd.tombstoneLifetime = 50 * time.Millisecond

	topicName := "tombstone_recover"

	conn := mustConnectLookupd(t, tcpAddr)
	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)

	endpoint := fmt.Sprintf("http://%s/tombstone_topic_producer?topic=%s&node=%s", httpAddr, topicName, "ip.address:5555")
	_, err := nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	assert.Equal(t, len(producers), 0)

	time.Sleep(55 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 1)
}

func TestTombstoneUnregister(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, httpAddr := mustStartLookupd()
	defer lookupd.Exit()
	lookupd.tombstoneLifetime = 50 * time.Millisecond

	topicName := "tombstone_unregister"

	conn := mustConnectLookupd(t, tcpAddr)
	identify(t, conn, "ip.address", 5000, 5555, "fake-version")

	nsq.Register(topicName, "channel1").Write(conn)

	endpoint := fmt.Sprintf("http://%s/tombstone_topic_producer?topic=%s&node=%s", httpAddr, topicName, "ip.address:5555")
	_, err := nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err := nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ := data.Get("producers").Array()
	assert.Equal(t, len(producers), 0)

	nsq.UnRegister(topicName, "").Write(conn)

	time.Sleep(55 * time.Millisecond)

	endpoint = fmt.Sprintf("http://%s/lookup?topic=%s", httpAddr, topicName)
	data, err = nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	producers, _ = data.Get("producers").Array()
	assert.Equal(t, len(producers), 0)
}
