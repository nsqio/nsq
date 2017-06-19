package contrib

import (
	"github.com/nsqio/nsq/nsqd"
	"time"
)


type NSQDDogStatsd struct {
	opts *nsqd.Options
	nsqd *nsqd.NSQD
}


func (dd *NSQDDogStatsd) Active() bool {
	if dd.opts.DogStatsdAddress != "" {
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
	ticker := time.NewTicker(dd.opts.DogStatsdInterval)
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



