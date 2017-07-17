package contrib

import (
	"github.com/nsqio/nsq/nsqd"
	"log"
	"os"
)

var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)

type INSQDAddon interface {
	Enabled() bool
	Start()
}

type NSQDAddons struct {
	addons []INSQDAddon
}

// Starts all addons that are active
func (as *NSQDAddons) Start() {
	logger.Println("Starting All Enabled Addons: %+v", as.addons)

	for _, addon := range as.addons {
		addon.Start()
	}
}

// Initializes addons that have options set
func NewEnabledNSQDAddons(contribOpts []string, nsqd *nsqd.NSQD) *NSQDAddons {
	var activeAddons []INSQDAddon

	logger.Println(contribOpts)

	// ask each addon if it should be initialize
	dogStats := NewNSQDDogStatsd(contribOpts, nsqd)
	if dogStats.Enabled() {
		activeAddons = append(activeAddons, dogStats)
	}

	return &NSQDAddons{
		addons: activeAddons,
	}
}
