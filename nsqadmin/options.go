package nsqadmin

import (
	"log"
	"os"
	"time"
)

type nsqadminOptions struct {
	HTTPAddress string `flag:"http-address"`

	GraphiteURL   string `flag:"graphite-url"`
	ProxyGraphite bool   `flag:"proxy-graphite"`

	UseStatsdPrefixes bool          `flag:"use-statsd-prefixes"`
	StatsdPrefix      string        `flag:"statsd-prefix"`
	StatsdInterval    time.Duration `flag:"statsd-interval"`

	NSQLookupdHTTPAddresses []string `flag:"lookupd-http-address" cfg:"nsqlookupd_http_addresses"`
	NSQDHTTPAddresses       []string `flag:"nsqd-http-address" cfg:"nsqd_http_addresses"`

	NotificationHTTPEndpoint string `flag:"notification-http-endpoint"`

	Logger logger
}

func NewNSQAdminOptions() *nsqadminOptions {
	return &nsqadminOptions{
		HTTPAddress:       "0.0.0.0:4171",
		UseStatsdPrefixes: true,
		StatsdPrefix:      "nsq.%s",
		StatsdInterval:    60 * time.Second,
		Logger:            log.New(os.Stderr, "[nsqadmin] ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}
