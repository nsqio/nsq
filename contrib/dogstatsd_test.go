package	contrib

import (
	"testing"
	"github.com/nsqio/nsq/internal/test"
	"log"
	"github.com/nsqio/nsq/internal/lg"
	"os"
)

func logf(level lg.LogLevel, f string, args ...interface{}) {
	logger := log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)
	lg.Logf(logger, lg.DEBUG, level, f, args...)
}

func TestEnabledTrueWhenAddressPresent(t *testing.T) {

	dd := &NSQDDogStatsd{
		opts: &NSQDDogStatsdOptions{
			DogStatsdAddress: "test.com.org",
		},
		logf: logf,
	}
	test.Equal(t, dd.Enabled(), true)
}

func TestEnabledFalseWhenAddressAbsent(t *testing.T) {

	dd := &NSQDDogStatsd{
		opts: &NSQDDogStatsdOptions{},
		logf: logf,
	}
	test.Equal(t, dd.Enabled(), false)
}
