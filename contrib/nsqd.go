package contrib

import (
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/nsqd"
	"strings"
)

type INSQD interface {
	Logf(level lg.LogLevel, f string, args ...interface{})
	GetStats() []nsqd.TopicStats
	AddModuleGoroutine(addonFn func(exitChan chan int))
}

type initializer func([]string, INSQD) INSQDAddon

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
	var hasOpt bool

	initializers := map[string]initializer{
		"dogstatsd": NewNSQDDogStatsd,
	}

	n.Logf(nsqd.LOG_INFO, "Addons Initializing")

	for k, initializer := range initializers {
		// check if any of the options contains this addon's expected argument
		hasOpt = false

		for _, opt := range contribOpts {
			if strings.Contains(opt, k) {
				hasOpt = true
				break
			}
		}

		if hasOpt {
			addon := initializer(contribOpts, n)
			if addon.Enabled() {
				activeAddons = append(activeAddons, addon)
			}
		}
	}

	return &NSQDAddons{
		addons: activeAddons,
	}
}
