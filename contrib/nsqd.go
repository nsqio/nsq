package contrib

import (
	"github.com/nsqio/nsq/nsqd"
	"log"
	"os"
	"flag"
	"time"
)


var logger = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)


type INSQDAddon interface {
	Active() bool
	Start()
}

type NSQDContribOptions struct {
	*NSQDDogStatsdOptions
}

// Instantiates all contrib default options
func NewContribOptions() *NSQDContribOptions {
	return &NSQDContribOptions{
		&NSQDDogStatsdOptions{
			DogStatsdPrefix:   "nsq.%s",
			DogStatsdInterval: 10 * time.Second,
		},
	}
}

func AddNSQDContribFlags(opts *NSQDContribOptions, flagSet *flag.FlagSet) {
	flagSet.String("dogstatsd-address", opts.DogStatsdAddress, "UDP <addr>:<port> of a statsd daemon for pushing stats")
	flagSet.Duration("dogstatsd-interval", opts.DogStatsdInterval, "duration between pushing to dogstatsd")
	// flagSet.Bool("statsd-mem-stats", opts.StatsdMemStats, "toggle sending memory and GC stats to statsd")
	flagSet.String("dogstatsd-prefix", opts.DogStatsdPrefix, "prefix used for keys sent to statsd (%s for host replacement)")
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


func NewNSQDAddons(contribOpts *NSQDContribOptions, nsqd *nsqd.NSQD) *NSQDAddons {
	return &NSQDAddons{
		addons: []INSQDAddon{
			&NSQDDogStatsd{
				contribOpts: contribOpts,
				nsqd: nsqd,
			},
		},
	}
}

