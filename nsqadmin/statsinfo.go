package main

import (
	"time"
)

type TopicHostStats struct {
	HostAddress  string
	Depth        int64
	MemoryDepth  int64
	BackendDepth int64
	MessageCount int64
	ChannelCount int
}

type ChannelStats struct {
	HostAddress   string
	ChannelName   string
	Depth         int64
	MemoryDepth   int64
	BackendDepth  int64
	InFlightCount int64
	DeferredCount int64
	RequeueCount  int64
	TimeoutCount  int64
	MessageCount  int64
	ClientCount   int
	Selected      bool
	Topic         string
	HostStats     []*ChannelStats
	Clients       []ClientInfo
}

type ClientInfo struct {
	HostAddress       string
	ClientVersion     string
	ClientIdentifier  string
	ConnectedDuration time.Duration
	InFlightCount     int
	ReadyCount        int
	FinishCount       int64
	RequeueCount      int64
	MessageCount      int64
}

func (c *ChannelStats) AddHostStats(a *ChannelStats) {
	c.Depth += a.Depth
	c.MemoryDepth += a.MemoryDepth
	c.BackendDepth += a.BackendDepth
	c.InFlightCount += a.InFlightCount
	c.DeferredCount += a.DeferredCount
	c.RequeueCount += a.RequeueCount
	c.TimeoutCount += a.TimeoutCount
	c.MessageCount += a.MessageCount
	c.ClientCount += a.ClientCount
	c.HostStats = append(c.HostStats, a)
}
