package contrib

import (
	"bytes"
	"fmt"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
	"net"
	"testing"
	"time"
)

type StubNSQD struct{}

func (n *StubNSQD) Logf(level lg.LogLevel, f string, args ...interface{}) {
	fmt.Printf(f, args)
}
func (n *StubNSQD) GetStats() []nsqd.TopicStats {
	return []nsqd.TopicStats{
		{
			TopicName:            "test",
			E2eProcessingLatency: &quantile.Result{},
			Channels: []nsqd.ChannelStats{
				{
					ChannelName:          "test_channel",
					E2eProcessingLatency: &quantile.Result{},
				},
			},
		},
	}
}
func (n *StubNSQD) AddModuleGoroutine(addonFn func(exitChan chan int)) {}

func TestEnabledTrueWhenAddressPresent(t *testing.T) {
	dd := &NSQDDogStatsd{
		opts: &NSQDDogStatsdOptions{
			DogStatsdAddress: "test.com.org",
		},
		nsqd: &StubNSQD{},
	}
	test.Equal(t, dd.Enabled(), true)

}

func TestEnabledFalseWhenAddressAbsent(t *testing.T) {
	dd := &NSQDDogStatsd{
		opts: &NSQDDogStatsdOptions{},
		nsqd: &StubNSQD{},
	}
	test.Equal(t, dd.Enabled(), false)
}

func TestFlagsParsedSuccess(t *testing.T) {
	opts := []string{"-dogstatsd-address", "127.0.0.1:8125"}
	addon := NewNSQDDogStatsd(opts, &StubNSQD{})
	test.Equal(t, addon.(*NSQDDogStatsd).opts.DogStatsdAddress, "127.0.0.1:8125")
}

// Tests that no opts are parsed when the - prefix is missing from the module
// opts.  The - is required because the optional module opts list is passed directly
// back to flags.Parse()
func TestFlagsMissingDashPrefix(t *testing.T) {
	opts := []string{"dogstatsd-address", "127.0.0.1:8125"}
	addon := NewNSQDDogStatsd(opts, &StubNSQD{})
	test.Equal(t, addon.(*NSQDDogStatsd).opts.DogStatsdAddress, "")
}

func TestLoopSendsCorrectMessages(t *testing.T) {
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
			nsqd:       &StubNSQD{},
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
		t.Run(tc.Name, func(t *testing.T) {
			buffer := make([]byte, 128)

			_, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(buffer))
			test.Equal(t, string(bytes.Trim(buffer, "\x00")), tc.A)

		})
	}
}
