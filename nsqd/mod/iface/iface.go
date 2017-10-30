package iface

import "github.com/nsqio/nsq/nsqd"
// it would be great to have an interface for NSQD instead of this import ...
// but TopicStats, ChannelStats, and ClientStats are annoying to duplicate

type NSQDModule interface {
	Start(n *nsqd.NSQD) error
	// Check() string
	// Stop()
}

type ModInitFunc func([]string) (NSQDModule, error)
