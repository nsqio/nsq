package contrib

import (
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/test"
	"github.com/nsqio/nsq/nsqd"
	"testing"
)

type StubNSQD struct{}

func (n *StubNSQD) Logf(level lg.LogLevel, f string, args ...interface{}) {}
func (n *StubNSQD) GetStats() []nsqd.TopicStats {
	return []nsqd.TopicStats{}
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
