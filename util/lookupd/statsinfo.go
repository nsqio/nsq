package lookupd

import (
	"fmt"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/semver"
	"sort"
	"time"
)

type ProducerTopic struct {
	Topic      string `json:"topic"`
	Tombstoned bool   `json:"tombstoned"`
}

type ProducerTopics []ProducerTopic

func (pt ProducerTopics) Len() int           { return len(pt) }
func (pt ProducerTopics) Swap(i, j int)      { pt[i], pt[j] = pt[j], pt[i] }
func (pt ProducerTopics) Less(i, j int) bool { return pt[i].Topic < pt[j].Topic }

type Producer struct {
	RemoteAddresses  []string        `json:"remote_addresses"`
	Address          string          `json:"address"` //TODO: remove for 1.0
	Hostname         string          `json:"hostname"`
	BroadcastAddress string          `json:"broadcast_address"`
	TcpPort          int             `json:"tcp_port"`
	HttpPort         int             `json:"http_port"`
	Version          string          `json:"version"`
	VersionObj       *semver.Version `json:-`
	Topics           ProducerTopics  `json:"topics"`
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
	TopicName    string
	Depth        int64
	MemoryDepth  int64
	BackendDepth int64
	MessageCount int64
	ChannelCount int
	Aggregate    bool
	Channels     []*ChannelStats

	E2eProcessingLatency *util.E2eProcessingLatencyAggregate
	numAggregates        int
}

func (t *TopicStats) Add(a *TopicStats) {
	t.Aggregate = true
	t.TopicName = a.TopicName
	t.Depth += a.Depth
	t.MemoryDepth += a.MemoryDepth
	t.BackendDepth += a.BackendDepth
	t.MessageCount += a.MessageCount
	if a.ChannelCount > t.ChannelCount {
		t.ChannelCount = a.ChannelCount
	}
	t.numAggregates += 1
	t.E2eProcessingLatency = t.E2eProcessingLatency.Add(a.E2eProcessingLatency, t.numAggregates)
}

func (t *TopicStats) Target(key string) ([]string, string) {
	color := "blue"
	if key == "depth" || key == "deferred_count" {
		color = "red"
	}
	target := fmt.Sprintf("sumSeries(%%stopic.%s.%s)", t.TopicName, key)
	return []string{target}, color
}

func (t *TopicStats) Host() string {
	h := t.HostAddress
	if t.Aggregate {
		h = "*"
	}
	return h
}

type ChannelStats struct {
	HostAddress   string
	TopicName     string
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
	HostStats     []*ChannelStats
	Clients       []*ClientInfo
	Paused        bool

	E2eProcessingLatency *util.E2eProcessingLatencyAggregate
}

func (c *ChannelStats) Add(a *ChannelStats) {
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
	c.E2eProcessingLatency = c.E2eProcessingLatency.Add(a.E2eProcessingLatency, len(c.HostStats))
	sort.Sort(ChannelStatsByHost{c.HostStats})
}

func (c *ChannelStats) Target(key string) ([]string, string) {
	color := "blue"
	if key == "depth" || key == "deferred_count" {
		color = "red"
	}
	target := fmt.Sprintf("sumSeries(%%stopic.%s.channel.%s.%s)", c.TopicName, c.ChannelName, key)
	return []string{target}, color
}

func (c *ChannelStats) Host() string {
	h := "*"
	if len(c.HostStats) == 0 {
		h = c.HostAddress
	}
	return h
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

type ClientInfoList []*ClientInfo
type ClientsByHost struct {
	ClientInfoList
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
func (c ClientInfoList) Len() int        { return len(c) }
func (c ClientInfoList) Swap(i, j int)   { c[i], c[j] = c[j], c[i] }
func (t TopicStatsList) Len() int        { return len(t) }
func (t TopicStatsList) Swap(i, j int)   { t[i], t[j] = t[j], t[i] }
func (t ProducerList) Len() int          { return len(t) }
func (t ProducerList) Swap(i, j int)     { t[i], t[j] = t[j], t[i] }

func (c ChannelStatsByHost) Less(i, j int) bool {
	return c.ChannelStatsList[i].HostAddress < c.ChannelStatsList[j].HostAddress
}
func (c ClientsByHost) Less(i, j int) bool {
	if c.ClientInfoList[i].ClientIdentifier == c.ClientInfoList[j].ClientIdentifier {
		return c.ClientInfoList[i].HostAddress < c.ClientInfoList[j].HostAddress
	}
	return c.ClientInfoList[i].ClientIdentifier < c.ClientInfoList[j].ClientIdentifier
}
func (c TopicStatsByHost) Less(i, j int) bool {
	return c.TopicStatsList[i].HostAddress < c.TopicStatsList[j].HostAddress
}
func (c ProducersByHost) Less(i, j int) bool {
	return c.ProducerList[i].BroadcastAddress < c.ProducerList[j].BroadcastAddress
}
