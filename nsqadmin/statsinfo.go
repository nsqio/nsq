package main

import (
	"time"
)

type HostStats struct {
	HostAddress  string
	Depth        int64
	MemoryDepth  int64
	BackendDepth int64
	MessageCount int64
	ChannelCount int
}

type ChannelStats struct {
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
