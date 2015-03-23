package lookupd

import (
	"fmt"
	"sort"
	"time"

	"github.com/bitly/nsq/internal/quantile"
	"github.com/blang/semver"
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
	RemoteAddresses  []string       `json:"remote_addresses"`
	Hostname         string         `json:"hostname"`
	BroadcastAddress string         `json:"broadcast_address"`
	TCPPort          int            `json:"tcp_port"`
	HTTPPort         int            `json:"http_port"`
	Version          string         `json:"version"`
	VersionObj       semver.Version `json:"-"`
	Topics           ProducerTopics `json:"topics"`
	OutOfDate        bool
}

func (p *Producer) HTTPAddress() string {
	return fmt.Sprintf("%s:%d", p.BroadcastAddress, p.HTTPPort)
}

func (p *Producer) TCPAddress() string {
	return fmt.Sprintf("%s:%d", p.BroadcastAddress, p.TCPPort)
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
	Paused       bool

	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate
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
	if a.Paused {
		t.Paused = a.Paused
	}
	if t.E2eProcessingLatency == nil {
		t.E2eProcessingLatency = &quantile.E2eProcessingLatencyAggregate{}
	}
	t.E2eProcessingLatency.Add(a.E2eProcessingLatency)
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
	Clients       []*ClientStats
	Paused        bool

	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate
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
	if c.E2eProcessingLatency == nil {
		c.E2eProcessingLatency = &quantile.E2eProcessingLatencyAggregate{}
	}
	c.E2eProcessingLatency.Add(a.E2eProcessingLatency)
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

type ClientStats struct {
	HostAddress       string
	RemoteAddress     string
	Version           string
	ClientID          string
	Hostname          string
	UserAgent         string
	ConnectedDuration time.Duration
	InFlightCount     int
	ReadyCount        int
	FinishCount       int64
	RequeueCount      int64
	MessageCount      int64
	SampleRate        int32
	Deflate           bool
	Snappy            bool
	Authed            bool
	AuthIdentity      string
	AuthIdentityURL   string

	TLS                           bool
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

func (c *ClientStats) HasUserAgent() bool {
	return c.UserAgent != ""
}

func (c *ClientStats) HasSampleRate() bool {
	return c.SampleRate > 0
}

type ChannelStatsList []*ChannelStats
type ChannelStatsByHost struct {
	ChannelStatsList
}

type ClientStatsList []*ClientStats
type ClientsByHost struct {
	ClientStatsList
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
func (c ClientStatsList) Len() int       { return len(c) }
func (c ClientStatsList) Swap(i, j int)  { c[i], c[j] = c[j], c[i] }
func (t TopicStatsList) Len() int        { return len(t) }
func (t TopicStatsList) Swap(i, j int)   { t[i], t[j] = t[j], t[i] }
func (t ProducerList) Len() int          { return len(t) }
func (t ProducerList) Swap(i, j int)     { t[i], t[j] = t[j], t[i] }

func (c ChannelStatsByHost) Less(i, j int) bool {
	return c.ChannelStatsList[i].HostAddress < c.ChannelStatsList[j].HostAddress
}
func (c ClientsByHost) Less(i, j int) bool {
	if c.ClientStatsList[i].ClientID == c.ClientStatsList[j].ClientID {
		return c.ClientStatsList[i].HostAddress < c.ClientStatsList[j].HostAddress
	}
	return c.ClientStatsList[i].ClientID < c.ClientStatsList[j].ClientID
}
func (c TopicStatsByHost) Less(i, j int) bool {
	return c.TopicStatsList[i].HostAddress < c.TopicStatsList[j].HostAddress
}
func (c ProducersByHost) Less(i, j int) bool {
	return c.ProducerList[i].BroadcastAddress < c.ProducerList[j].BroadcastAddress
}
