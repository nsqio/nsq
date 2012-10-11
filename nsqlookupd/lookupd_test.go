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

	tcpAddr = lookupd.tcpListener.Addr().(*net.TCPAddr)
	httpAddr = lookupd.httpListener.Addr().(*net.TCPAddr)
	return tcpAddr, httpAddr
}

func mustConnectLookupd(t *testing.T, tcpAddr *net.TCPAddr) net.Conn {
	conn, err := net.DialTimeout("tcp", tcpAddr.String(), time.Second)
	if err != nil {
		t.Fatal("failed to connect to lookupdd")
	}
	conn.Write(nsq.MagicV1)
	return conn
}

func TestBasicLookupd(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	tcpAddr, httpAddr := mustStartLookupd()
	defer lookupd.Exit()

	topics := lookupd.DB.FindRegistrations("topic", "*", "*")
	assert.Equal(t, len(topics), 0)

	conn := mustConnectLookupd(t, tcpAddr)
	topicName := "legacyannounce"
	port := 1000
	ips := []string{"ip.first", "second.hostname"}
	err := nsq.SendCommand(conn, nsq.Announce(topicName, ".", port, ips))
	assert.Equal(t, err, nil)

	time.Sleep(10 * time.Millisecond)

	topics = lookupd.DB.FindRegistrations("topic", topicName, "")
	assert.Equal(t, len(topics), 1)

	producers := lookupd.DB.FindProducers("topic", topicName, "")
	assert.Equal(t, len(producers), 1)
	producer := producers[0]

	assert.Equal(t, producer.Address, "second.hostname")
	assert.Equal(t, producer.TcpPort, 1000)
	// old announce message was +=1 for http port
	assert.Equal(t, producer.HttpPort, 1001)
	conn.Close()

	// send a connect message
	conn = mustConnectLookupd(t, tcpAddr)
	topicName = "connectmsg"
	tcpPort := 5000
	httpPort := 5555
	cmd := nsq.Identify("fake-version", tcpPort, httpPort, "ip.address")
	log.Printf("cmd is %s", string(cmd.Body))
	err = nsq.SendCommand(conn, cmd)
	assert.Equal(t, err, nil)
	err = nsq.SendCommand(conn, nsq.Register(topicName, "channel1"))

	time.Sleep(10 * time.Millisecond)

	endpoint := fmt.Sprintf("http://%s/nodes", httpAddr)
	data, err := nsq.ApiRequest(endpoint)
	log.Printf("got %v", data)
	returnedProducers, err := data.Get("producers").Array()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedProducers), 1)

	topics = lookupd.DB.FindRegistrations("topic", topicName, "")
	assert.Equal(t, len(topics), 1)

	producers = lookupd.DB.FindProducers("topic", topicName, "")
	assert.Equal(t, len(producers), 1)
	producer = producers[0]

	assert.Equal(t, producer.Address, "ip.address")
	assert.Equal(t, producer.TcpPort, tcpPort)
	assert.Equal(t, producer.HttpPort, httpPort)

	endpoint = fmt.Sprintf("http://%s/topics", httpAddr)
	data, err = nsq.ApiRequest(endpoint)
	assert.Equal(t, err, nil)
	returnedTopics, err := data.Get("topics").Array()
	log.Printf("got returnedTopics %v", returnedTopics)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(returnedTopics), 2)

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
