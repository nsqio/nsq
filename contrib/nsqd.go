package contrib

import (
	"github.com/nsqio/nsq/nsqd"
)


type INSQDAddon interface {
	Enabled() bool
	Start()
}

type NSQDAddons struct {
	addons []INSQDAddon
}

// Starts all addons that are active
func (as *NSQDAddons) Start() {

	for _, addon := range as.addons {
		addon.Start()
	}
}

// Initializes addons that have options set
func NewEnabledNSQDAddons(contribOpts []string, n *nsqd.NSQD) *NSQDAddons {
	var activeAddons []INSQDAddon

	n.Logf(nsqd.LOG_INFO, "Addons Initializing")

	// ask each addon if it should be initialize
	dogStats := NewNSQDDogStatsd(contribOpts, n)
	if dogStats.Enabled() {
		activeAddons = append(activeAddons, dogStats)
	}

	return &NSQDAddons{
		addons: activeAddons,
	}
}
