package nsqlookupd

import (
	"github.com/absolute8511/nsq/internal/levellogger"
	"log"
	"os"
	"time"
)

type Options struct {
	Verbose bool `flag:"verbose"`

	TCPAddress       string `flag:"tcp-address"`
	HTTPAddress      string `flag:"http-address"`
	RPCPort          string `flag:"rpc-port"`
	BroadcastAddress string `flag:"broadcast-address"`

	ClusterID                  string   `flag:"cluster-id"`
	ClusterLeadershipAddresses []string `flag:"cluster-leadership-addresses" cfg:"cluster_leadership_addresses"`

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`

	LogLevel int32 `flag:"log-level" cfg:"log_level"`
	Logger   levellogger.Logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,

		ClusterLeadershipAddresses: make([]string, 0),
		ClusterID:                  "nsq-clusterid-test-only",

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,

		LogLevel: 1,
		Logger:   &levellogger.GLogger{},
	}
}
