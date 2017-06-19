package contrib

import (
	"github.com/nsqio/nsq/nsqd"
	"log"
	"os"
)


var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)


type INSQDAddon interface {
	Active(opts *nsqd.Options) bool
	Start(*nsqd.NSQD)
}


type NSQDAddons struct {
	addons []INSQDAddon
}


// Starts all addons that are active
func (as *NSQDAddons) Start(opts *nsqd.Options, n *nsqd.NSQD) {
	logger.Println("Starting All addons")

	for _, addon := range as.addons {
		if addon.Active(opts) {
			addon.Start(n)
		}
	}
}

func NewNSQDAddons() *NSQDAddons {
	return &NSQDAddons{
		addons: []INSQDAddon{
			&NSQDDogStatsd{},
		},
	}
}

