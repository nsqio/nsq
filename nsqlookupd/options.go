package nsqlookupd

import (
	"github.com/absolute8511/nsq/internal/levellogger"
	"log"
	"os"
	"time"
)

type Options struct {
	Verbose bool `flag:"verbose"`

	TCPAddress         string `flag:"tcp-address"`
	HTTPAddress        string `flag:"http-address"`
	RPCPort            string `flag:"rpc-port"`
	BroadcastAddress   string `flag:"broadcast-address"`
	BroadcastInterface string `flag:"broadcast-interface"`

	ReverseProxyPort string `flag:"reverse-proxy-port"`

	ClusterID                  string `flag:"cluster-id"`
	ClusterLeadershipAddresses string `flag:"cluster-leadership-addresses" cfg:"cluster_leadership_addresses"`

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	NsqdPingTimeout         time.Duration `flag:"nsqd-ping-timeout"`

	LogLevel int32  `flag:"log-level" cfg:"log_level"`
	LogDir   string `flag:"log-dir" cfg:"log_dir"`
	Logger   levellogger.Logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		TCPAddress:         "0.0.0.0:4160",
		HTTPAddress:        "0.0.0.0:4161",
		BroadcastAddress:   hostname,
		BroadcastInterface: "eth0",

		ClusterLeadershipAddresses: "",
		ClusterID:                  "nsq-clusterid-test-only",

		InactiveProducerTimeout: 60 * time.Second,
		NsqdPingTimeout:         15 * time.Second,

		LogLevel: 1,
		LogDir:   "",
		Logger:   &levellogger.GLogger{},
	}
}
