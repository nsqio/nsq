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

func optHasPrefix(opt string, prefix string) bool {
	return strings.Index(opt, prefix) == 0
}

// Initializes addons that have options set
func NewEnabledNSQDAddons(contribOpts []string, n INSQD) *NSQDAddons {
	var activeAddons []INSQDAddon

	initializers := map[string]initializer{
		"-dogstatsd-": NewNSQDDogStatsd,
	}

	n.Logf(nsqd.LOG_INFO, "Addons Initializing")

	for prefix, initializer := range initializers {
		validOpts := []string{}
		// check if any of the options contains this addon's expected argument
		// keeps track of all options starting with the correct prefix
		// and initializes with the valid options
		for _, opt := range contribOpts {
			if optHasPrefix(opt, prefix) {
				validOpts = append(validOpts, opt)
			}
		}

		addon := initializer(contribOpts, n)
		if addon.Enabled() {
			activeAddons = append(activeAddons, addon)
		}
	}

	return &NSQDAddons{
		addons: activeAddons,
	}
}
