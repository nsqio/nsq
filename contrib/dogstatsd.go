package contrib

import (
	"github.com/nsqio/nsq/nsqd"
	"time"
)


type NSQDDogStatsdOptions struct {
	DogStatsdAddress  string        `flag:"dogstatsd-address"`
	DogStatsdPrefix   string        `flag:"dogstatsd-prefix"`
	DogStatsdInterval time.Duration `flag:"dogstatsd-interval"`
}


type NSQDDogStatsd struct {
	nsqd *nsqd.NSQD
	contribOpts *NSQDContribOptions
}


func (dd *NSQDDogStatsd) Active() bool {
	dd.nsqd.Logf(nsqd.LOG_ERROR, "%s", dd.contribOpts)
	if dd.contribOpts.DogStatsdAddress != "" {
		return true
	} else {
		return false
	}
}

func (dd *NSQDDogStatsd) Start() {
	logger.Println("Starting nsqd datadog")

	dd.nsqd.RegisterAddon(dd.Loop)
}

func (dd *NSQDDogStatsd) Loop() {
	ticker := time.NewTicker(dd.contribOpts.DogStatsdInterval)
	dd.nsqd.Logf(nsqd.LOG_DEBUG, "Loop started")
	exitChan := *dd.nsqd.ExitChan()

	for {
		select {
		case <- exitChan:
			goto exit
		case <- ticker.C:
			dd.nsqd.Logf(nsqd.LOG_DEBUG, "LOOPING")
		}
	}

exit:
	ticker.Stop()
}



