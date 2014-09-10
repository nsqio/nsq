package clusterinfo

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
	OutOfDate        bool           `json:"out_of_date"`
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
	Node         string          `json:"node"`
	TopicName    string          `json:"name"`
	Depth        int64           `json:"depth"`
	MemoryDepth  int64           `json:"memory_depth"`
	BackendDepth int64           `json:"backend_depth"`
	MessageCount int64           `json:"msg_count"`
	NodeStats    []*TopicStats   `json:"nodes"`
	Channels     []*ChannelStats `json:"channels"`
	Paused       bool            `json:"paused"`

	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate `json:"e2e_processing_latency"`
}

func (t *TopicStats) Add(a *TopicStats) {
	t.Node = "*"
	t.Depth += a.Depth
	t.MemoryDepth += a.MemoryDepth
	t.BackendDepth += a.BackendDepth
	t.MessageCount += a.MessageCount
	if a.Paused {
		t.Paused = a.Paused
	}
	found := false
	for _, aChannelStats := range a.Channels {
		for _, channelStats := range t.Channels {
			if aChannelStats.ChannelName == channelStats.ChannelName {
				found = true
				channelStats.Add(aChannelStats)
			}
		}
		if !found {
			t.Channels = append(t.Channels, aChannelStats)
		}
	}
	t.NodeStats = append(t.NodeStats, a)
	sort.Sort(TopicStatsByHost{t.NodeStats})
	if t.E2eProcessingLatency == nil {
		t.E2eProcessingLatency = &quantile.E2eProcessingLatencyAggregate{
			Addr:  t.Node,
			Topic: t.TopicName,
		}
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
	if len(t.NodeStats) > 0 {
		return "*"
	}
	return t.Node
}

type ChannelStats struct {
	Node          string          `json:"node"`
	TopicName     string          `json:"topic_name"`
	ChannelName   string          `json:"name"`
	Depth         int64           `json:"depth"`
	MemoryDepth   int64           `json:"memory_depth"`
	BackendDepth  int64           `json:"backend_depth"`
	InFlightCount int64           `json:"in_flight_count"`
	DeferredCount int64           `json:"defer_count"`
	RequeueCount  int64           `json:"requeue_count"`
	TimeoutCount  int64           `json:"timeout_count"`
	MessageCount  int64           `json:"msg_count"`
	ClientCount   int             `json:"-"`
	Selected      bool            `json:"-"`
	NodeStats     []*ChannelStats `json:"nodes"`
	Clients       []*ClientStats  `json:"clients"`
	Paused        bool            `json:"paused"`

	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate `json:"e2e_processing_latency"`
}

func (c *ChannelStats) Add(a *ChannelStats) {
	c.Node = "*"
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
	c.NodeStats = append(c.NodeStats, a)
	sort.Sort(ChannelStatsByHost{c.NodeStats})
	if c.E2eProcessingLatency == nil {
		c.E2eProcessingLatency = &quantile.E2eProcessingLatencyAggregate{
			Addr:    c.Node,
			Topic:   c.TopicName,
			Channel: c.ChannelName,
		}
	}
	c.E2eProcessingLatency.Add(a.E2eProcessingLatency)
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
	if len(c.NodeStats) > 0 {
		return "*"
	}
	return c.Node
}

type ClientStats struct {
	Node              string        `json:"node"`
	RemoteAddress     string        `json:"remote_address"`
	Version           string        `json:"version"`
	ClientID          string        `json:"client_id"`
	Hostname          string        `json:"hostname"`
	UserAgent         string        `json:"user_agent"`
	ConnectedDuration time.Duration `json:"connected"`
	InFlightCount     int           `json:"in_flight_count"`
	ReadyCount        int           `json:"ready_count"`
	FinishCount       int64         `json:"finish_count"`
	RequeueCount      int64         `json:"requeue_count"`
	MessageCount      int64         `json:"message_count"`
	SampleRate        int32         `json:"sample_rate"`
	Deflate           bool          `json:"deflate"`
	Snappy            bool          `json:"snappy"`
	Authed            bool          `json:"authed"`
	AuthIdentity      string        `json:"auth_identity"`
	AuthIdentityURL   string        `json:"auth_identity_url"`

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
	return c.ChannelStatsList[i].Node < c.ChannelStatsList[j].Node
}
func (c ClientsByHost) Less(i, j int) bool {
	if c.ClientStatsList[i].ClientID == c.ClientStatsList[j].ClientID {
		return c.ClientStatsList[i].Node < c.ClientStatsList[j].Node
	}
	return c.ClientStatsList[i].ClientID < c.ClientStatsList[j].ClientID
}
func (c TopicStatsByHost) Less(i, j int) bool {
	return c.TopicStatsList[i].Node < c.TopicStatsList[j].Node
}
func (c ProducersByHost) Less(i, j int) bool {
	return c.ProducerList[i].BroadcastAddress < c.ProducerList[j].BroadcastAddress
}