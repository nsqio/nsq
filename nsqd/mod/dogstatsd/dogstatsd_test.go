package dogstatsd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
)

func makeNSQD(t *testing.T, opts *nsqd.Options) *nsqd.NSQD {
	opts.Logger = test.NewTestLogger(t)
	opts.TCPAddress = "127.0.0.1:0"
	opts.HTTPAddress = "127.0.0.1:0"
	opts.HTTPSAddress = "127.0.0.1:0"
	if opts.DataPath == "" {
		tmpDir, err := ioutil.TempDir("", "nsq-test-")
		test.Nil(t, err)
		opts.DataPath = tmpDir
	}
	nsqd := nsqd.New(opts)
	nsqd.Main()
	return nsqd
}


func TestFlagsParsedSuccess(t *testing.T) {
	nsqdOpts := nsqd.NewOptions()
	nsqd := makeNSQD(t, nsqdOpts)
	defer nsqd.Exit()

	opts := []string{"-address=127.0.0.1:8125"}
	addon, err := Init(opts)
	test.Nil(t, err)
	test.Equal(t, addon.(*NSQDDogStatsd).opts.DogStatsdAddress, "127.0.0.1:8125")
}

func TestLoopSendsCorrectMessages(t *testing.T) {
	nsqdOpts := nsqd.NewOptions()
	n := makeNSQD(t, nsqdOpts)
	defer n.Exit()
	topic := n.GetTopic("test")
	topic.GetChannel("test_channel")

	// setup the UDP Server
	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 0,
	})
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Start the DD loop to send status updates to the test server addr
	go func(addr net.Addr) {
		exitChan := make(chan int)
		dd := &NSQDDogStatsd{
			opts: &NSQDDogStatsdOptions{
				DogStatsdAddress:  addr.String(),
				DogStatsdInterval: time.Second,
			},
			nsqd:       n,
			singleLoop: true,
		}

		dd.Loop(exitChan)

	}(conn.LocalAddr())

	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	// read from server conn and assert all data was sent from datadog
	// as expected
	// Makes X Specific reads
	cases := []struct {
		Name string
		A    string
	}{
		{Name: "message_count", A: "message_count:0|c|#topic_name:test"},
		{Name: "topic_depth", A: "topic.depth:0|g|#topic_name:test"},
		{Name: "backend_depth", A: "topic.backend_depth:0|g|#topic_name:test"},
		{Name: "channel.message_count", A: "channel.message_count:0|c|#topic_name:test,channel_name:test_channel"},
		{Name: "channel.depth", A: "channel.depth:0|g|#topic_name:test,channel_name:test_channel"},
		{Name: "channel.backend_depth", A: "channel.backend_depth:0|g|#topic_name:test,channel_name:test_channel"},
		{Name: "channel.in_flight_count", A: "channel.in_flight_count:0|g|#topic_name:test,channel_name:test_channel"},
		{Name: "channel.deferred_count", A: "channel.deferred_count:0|g|#topic_name:test,channel_name:test_channel"},
		{Name: "channel.requeue_count", A: "channel.requeue_count:0|c|#topic_name:test,channel_name:test_channel"},
		{Name: "channel.timeout_count", A: "channel.timeout_count:0|c|#topic_name:test,channel_name:test_channel"},
		{Name: "channel.clients", A: "channel.clients:0|g|#topic_name:test,channel_name:test_channel"},
	}
	for _, tc := range cases {
		buffer := make([]byte, 128)

		_, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(buffer))
		test.Equal(t, string(bytes.Trim(buffer, "\x00")), tc.A)
	}
}
