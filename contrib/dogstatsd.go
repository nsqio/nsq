package contrib

import "github.com/nsqio/nsq/nsqd"


type NSQDDogStatsd struct {}


func (dd *NSQDDogStatsd) Active(opts *nsqd.Options) bool {
	if opts.DogStatsdAddress != "" {
		return true
	} else {
		return false
	}
}

func (dd *NSQDDogStatsd) Start(n *nsqd.NSQD) {
	logger.Println("Starting nsqd datadog")
}
