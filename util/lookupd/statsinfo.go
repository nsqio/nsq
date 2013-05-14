package lookupd

import (
	"fmt"
	"github.com/bitly/nsq/util/semver"
	"sort"
	"strings"
	"time"
)

type Producer struct {
	RemoteAddresses  []string        `json:"remote_addresses"`
	Address          string          `json:"address"` //TODO: remove for 1.0
	Hostname         string          `json:"hostname"`
	BroadcastAddress string          `json:"broadcast_address"`
	TcpPort          int             `json:"tcp_port"`
	HttpPort         int             `json:"http_port"`
	Version          string          `json:"version"`
	VersionObj       *semver.Version `json:-`
	Topics           []string        `json:"topics"`
	OutOfDate        bool
}

func (p *Producer) HTTPAddress() string {
	return fmt.Sprintf("%s:%d", p.BroadcastAddress, p.HttpPort)
}

func (p *Producer) TCPAddress() string {
	return fmt.Sprintf("%s:%d", p.BroadcastAddress, p.TcpPort)
}

// IsInconsistent checks for cases where an unexpected number of nsqd connections are
// reporting the same information to nsqlookupd (ie: multiple instances are using the
// same broadcast address), or cases where some nsqd are not reporting to all nsqlookupd.
func (p *Producer) IsInconsistent(numLookupd int) bool {
	return len(p.RemoteAddresses) != numLookupd
}

type TopicStats struct {
	HostAddress  string
	Depth        int64
	MemoryDepth  int64
	BackendDepth int64
	MessageCount int64
	ChannelCount int
	Topic        string
	Aggregate    bool
}

func (t *TopicStats) AddHostStats(a *TopicStats) {
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

func (t *TopicStats) Target(key string) (string, string) {
	h := graphiteHostKey(t.HostAddress)
	if t.Aggregate {
		h = "*"
	}
	color := "blue"
	if key == "depth" || key == "deferred_count" {
		color = "red"
	}
	target := fmt.Sprintf("nsq.%s.topic.%s.%s", h, t.Topic, key)
	return target, color
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

func (c *ChannelStats) Target(key string) (string, string) {
	h := "*"
	if len(c.HostStats) == 0 {
		h = graphiteHostKey(c.HostAddress)
	}
	color := "blue"
	if key == "depth" || key == "deferred_count" {
		color = "red"
	}
	target := fmt.Sprintf("nsq.%s.topic.%s.channel.%s.%s", h, c.Topic, c.ChannelName, key)
	return target, color
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
type TopicStatsList []*TopicStats
type TopicStatsByHost struct {
	TopicStatsList
}
type ProducerList []*Producer
type ProducersByHost struct {
	ProducerList
}

func (c ChannelStatsList) Len() int      { return len(c) }
func (c ChannelStatsList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }
func (c ClientInfos) Len() int           { return len(c) }
func (c ClientInfos) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (t TopicStatsList) Len() int        { return len(t) }
func (t TopicStatsList) Swap(i, j int)   { t[i], t[j] = t[j], t[i] }
func (t ProducerList) Len() int          { return len(t) }
func (t ProducerList) Swap(i, j int)     { t[i], t[j] = t[j], t[i] }

func (c ChannelStatsByHost) Less(i, j int) bool {
	return c.ChannelStatsList[i].HostAddress < c.ChannelStatsList[j].HostAddress
}
func (c ClientsByHost) Less(i, j int) bool {
	if c.ClientInfos[i].ClientIdentifier == c.ClientInfos[j].ClientIdentifier {
		return c.ClientInfos[i].HostAddress < c.ClientInfos[j].HostAddress
	}
	return c.ClientInfos[i].ClientIdentifier < c.ClientInfos[j].ClientIdentifier
}
func (c TopicStatsByHost) Less(i, j int) bool {
	return c.TopicStatsList[i].HostAddress < c.TopicStatsList[j].HostAddress
}
func (c ProducersByHost) Less(i, j int) bool {
	return c.ProducerList[i].BroadcastAddress < c.ProducerList[j].BroadcastAddress
}

func graphiteHostKey(h string) string {
	s := strings.Replace(h, ".", "_", -1)
	return strings.Replace(s, ":", "_", -1)
}
