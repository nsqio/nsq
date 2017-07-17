package contrib

import (
	"github.com/nsqio/nsq/nsqd"
	"github.com/nsqio/nsq/internal/lg"
)


type INSQDAddon interface {
	Enabled() bool
	Start()
}

type NSQDAddons struct {
	addons []INSQDAddon
	logf lg.AppLogFunc
}

// Starts all addons that are active
func (as *NSQDAddons) Start() {

	for _, addon := range as.addons {
		addon.Start()
	}
}

// Initializes addons that have options set
func NewEnabledNSQDAddons(contribOpts []string, n *nsqd.NSQD, logf lg.AppLogFunc) *NSQDAddons {
	var activeAddons []INSQDAddon

	logf(nsqd.LOG_INFO, "Addons Initializing")

	// ask each addon if it should be initialize
	dogStats := NewNSQDDogStatsd(contribOpts, n, logf)
	if dogStats.Enabled() {
		activeAddons = append(activeAddons, dogStats)
	}

	return &NSQDAddons{
		addons: activeAddons,
		logf: logf,
	}
}
