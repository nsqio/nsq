package	contrib

import (
	"testing"
	_"github.com/nsqio/nsq/internal/test"
	_"github.com/nsqio/nsq/nsqd"
)

func TestEnabledTrueWhenAddressPresent(t *testing.T) {
	t.Fail()

	//n := nsqd.New(&nsqd.Options{
	//	LogLevel: "debug",
	//	MaxDeflateLevel: 1,
	//})
	//
	//dd := &NSQDDogStatsd{
	//	opts: &NSQDDogStatsdOptions{
	//		DogStatsdAddress: "test.com.org",
	//	},
	//	nsqd: n,
	//}
	//test.Equal(t, dd.Enabled(), true)
	//
}

func TestEnabledFalseWhenAddressAbsent(t *testing.T) {
	t.Fail()

	//n := nsqd.New(&nsqd.Options{
	//	LogLevel: "debug",
	//	MaxDeflateLevel: 1,
	//})
	//
	//dd := &NSQDDogStatsd{
	//	opts: &NSQDDogStatsdOptions{},
	//	nsqd: n,
	//}
	//test.Equal(t, dd.Enabled(), false)
	//
}

func TestFlagsParsedSuccess(t *testing.T) {
	t.Fail()
}

// Tests that no opts are parsed when the - prefix is missing from the module
// opts.  The - is required because the optional module opts list is passed directly
// back to flags.Parse()
func TestFlagsMissingDashPrefix(t *testing.T) {
	t.Fail()
}
