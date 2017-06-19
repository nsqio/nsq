package contrib

import (
	"github.com/nsqio/nsq/nsqd"
	"log"
	"os"
)


var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)


type INSQDAddon interface {
	Active() bool
	Start()
}


type NSQDAddons struct {
	addons []INSQDAddon
}


// Starts all addons that are active
func (as *NSQDAddons) Start() {
	logger.Println("Starting All addons")

	for _, addon := range as.addons {
		if addon.Active() {
			addon.Start()
		}
	}
}


func NewNSQDAddons(opts *nsqd.Options, nsqd *nsqd.NSQD) *NSQDAddons {
	return &NSQDAddons{
		addons: []INSQDAddon{
			&NSQDDogStatsd{
				opts: opts,
				nsqd: nsqd,
			},
		},
	}
}

