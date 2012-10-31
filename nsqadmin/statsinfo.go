package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Topic struct {
	TopicName string
}
type Topics []*Topic

func TopicsForStrings(s []string) Topics {
	t := make(Topics, 0)
	for _, ss := range s {
		tt := &Topic{ss}
		t = append(t, tt)
	}
	return t
}

type Version struct {
	src        string
	components []string
}

type Producer struct {
	Address    string   `json:"address"`
	TcpPort    int      `json:"tcp_port"`
	HttpPort   int      `json:"http_port"`
	Version    string   `json:"version"`
	VersionObj *Version `json:-`
	Topics     []string `json:"topics"`
	OutOfDate  bool
}

type TopicHostStats struct {
	HostAddress  string
	Depth        int64
	MemoryDepth  int64
	BackendDepth int64
	MessageCount int64
	ChannelCount int
	Topic        string
	Aggregate    bool
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
	Clients       []*ClientInfo
	Paused        bool
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

type ChannelStatsList []*ChannelStats
type ChannelStatsByHost struct {
	ChannelStatsList
}
type ClientInfos []*ClientInfo
type ClientsByHost struct {
	ClientInfos
}
type TopicHostStatsList []*TopicHostStats
type TopicHostStatsByHost struct {
	TopicHostStatsList
}
type ProducerList []*Producer
type ProducersByHost struct {
	ProducerList
}

func (c ChannelStatsList) Len() int        { return len(c) }
func (c ChannelStatsList) Swap(i, j int)   { c[i], c[j] = c[j], c[i] }
func (c ClientInfos) Len() int             { return len(c) }
func (c ClientInfos) Swap(i, j int)        { c[i], c[j] = c[j], c[i] }
func (t TopicHostStatsList) Len() int      { return len(t) }
func (t TopicHostStatsList) Swap(i, j int) { t[i], t[j] = t[j], t[i] }
func (t ProducerList) Len() int            { return len(t) }
func (t ProducerList) Swap(i, j int)       { t[i], t[j] = t[j], t[i] }

func (c ChannelStatsByHost) Less(i, j int) bool {
	return c.ChannelStatsList[i].HostAddress < c.ChannelStatsList[j].HostAddress
}
func (c ClientsByHost) Less(i, j int) bool {
	if c.ClientInfos[i].ClientIdentifier == c.ClientInfos[j].ClientIdentifier {
		return c.ClientInfos[i].HostAddress < c.ClientInfos[j].HostAddress
	}
	return c.ClientInfos[i].ClientIdentifier < c.ClientInfos[j].ClientIdentifier
}
func (c TopicHostStatsByHost) Less(i, j int) bool {
	return c.TopicHostStatsList[i].HostAddress < c.TopicHostStatsList[j].HostAddress
}
func (c ProducersByHost) Less(i, j int) bool {
	return c.ProducerList[i].Address < c.ProducerList[j].Address
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
	if a.Paused {
		c.Paused = a.Paused
	}
	c.HostStats = append(c.HostStats, a)
	sort.Sort(ChannelStatsByHost{c.HostStats})
}

func (t *TopicHostStats) AddHostStats(a *TopicHostStats) {
	t.Aggregate = true
	t.Topic = a.Topic
	t.Depth += a.Depth
	t.MemoryDepth += a.MemoryDepth
	t.BackendDepth += a.BackendDepth
	t.MessageCount += a.MessageCount
	if a.ChannelCount > t.ChannelCount {
		t.ChannelCount = a.ChannelCount
	}
}

func (p *Producer) HTTPAddress() string {
	return fmt.Sprintf("%s:%d", p.Address, p.HttpPort)
}

func NewVersion(v string) *Version {
	version := &Version{
		src:        v,
		components: strings.Split(v, "."),
	}
	return version
}

func (v *Version) Less(vv *Version) bool {
	for i, x := range v.components {
		if i >= len(vv.components) || i >= 3 {
			break
		}
		y := vv.components[i]
		xx, _ := strconv.Atoi(x)
		yy, _ := strconv.Atoi(y)
		if xx > yy {
			return true
		}
	}
	return false
}
