package contrib

import (
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/nsqd"
)

type INSQD interface {
	Logf(level lg.LogLevel, f string, args ...interface{})
	GetStats() []nsqd.TopicStats
	AddModuleGoroutine(addonFn func(exitChan chan int))
}

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
func NewEnabledNSQDAddons(contribOpts []string, n INSQD) *NSQDAddons {
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
