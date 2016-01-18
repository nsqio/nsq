package clusterinfo

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/nsqio/nsq/internal/quantile"
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
	RemoteAddress    string         `json:"remote_address"`
	Hostname         string         `json:"hostname"`
	BroadcastAddress string         `json:"broadcast_address"`
	TCPPort          int            `json:"tcp_port"`
	HTTPPort         int            `json:"http_port"`
	Version          string         `json:"version"`
	VersionObj       semver.Version `json:"-"`
	Topics           ProducerTopics `json:"topics"`
	OutOfDate        bool           `json:"out_of_date"`
}

// UnmarshalJSON implements json.Unmarshaler and postprocesses of ProducerTopics and VersionObj
func (p *Producer) UnmarshalJSON(b []byte) error {
	var r struct {
		RemoteAddress    string   `json:"remote_address"`
		Hostname         string   `json:"hostname"`
		BroadcastAddress string   `json:"broadcast_address"`
		TCPPort          int      `json:"tcp_port"`
		HTTPPort         int      `json:"http_port"`
		Version          string   `json:"version"`
		Topics           []string `json:"topics"`
		Tombstoned       []bool   `json:"tombstones"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return err
	}
	*p = Producer{
		RemoteAddress:    r.RemoteAddress,
		Hostname:         r.Hostname,
		BroadcastAddress: r.BroadcastAddress,
		TCPPort:          r.TCPPort,
		HTTPPort:         r.HTTPPort,
		Version:          r.Version,
	}
	for i, t := range r.Topics {
		p.Topics = append(p.Topics, ProducerTopic{Topic: t, Tombstoned: r.Tombstoned[i]})
	}
	version, err := semver.Parse(p.Version)
	if err != nil {
		version, _ = semver.Parse("0.0.0")
	}
	p.VersionObj = version
	return nil
}

func (p *Producer) Address() string {
	if p.RemoteAddress == "" {
		return "N/A"
	}
	return p.RemoteAddress
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
	Hostname     string          `json:"hostname"`
	TopicName    string          `json:"topic_name"`
	Depth        int64           `json:"depth"`
	MemoryDepth  int64           `json:"memory_depth"`
	BackendDepth int64           `json:"backend_depth"`
	MessageCount int64           `json:"message_count"`
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

type ChannelStats struct {
	Node          string          `json:"node"`
	Hostname      string          `json:"hostname"`
	TopicName     string          `json:"topic_name"`
	ChannelName   string          `json:"channel_name"`
	Depth         int64           `json:"depth"`
	MemoryDepth   int64           `json:"memory_depth"`
	BackendDepth  int64           `json:"backend_depth"`
	InFlightCount int64           `json:"in_flight_count"`
	DeferredCount int64           `json:"deferred_count"`
	RequeueCount  int64           `json:"requeue_count"`
	TimeoutCount  int64           `json:"timeout_count"`
	MessageCount  int64           `json:"message_count"`
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
	c.Clients = append(c.Clients, a.Clients...)
	sort.Sort(ClientsByHost{c.Clients})
}

type ClientStats struct {
	Node              string        `json:"node"`
	RemoteAddress     string        `json:"remote_address"`
	Name              string        `json:"name"` // TODO: deprecated, remove in 1.0
	Version           string        `json:"version"`
	ClientID          string        `json:"client_id"`
	Hostname          string        `json:"hostname"`
	UserAgent         string        `json:"user_agent"`
	ConnectTs         int64         `json:"connect_ts"`
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

	TLS                           bool   `json:"tls"`
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

// UnmarshalJSON implements json.Unmarshaler and postprocesses ConnectedDuration
func (s *ClientStats) UnmarshalJSON(b []byte) error {
	type locaClientStats ClientStats // re-typed to prevent recursion from json.Unmarshal
	var ss locaClientStats
	if err := json.Unmarshal(b, &ss); err != nil {
		return err
	}
	*s = ClientStats(ss)
	s.ConnectedDuration = time.Now().Truncate(time.Second).Sub(time.Unix(s.ConnectTs, 0))

	if s.ClientID == "" {
		// TODO: deprecated, remove in 1.0
		remoteAddressParts := strings.Split(s.RemoteAddress, ":")
		port := remoteAddressParts[len(remoteAddressParts)-1]
		if len(remoteAddressParts) < 2 {
			port = "NA"
		}
		s.ClientID = fmt.Sprintf("%s:%s", s.Name, port)
	}
	return nil
}

func (c *ClientStats) HasUserAgent() bool {
	return c.UserAgent != ""
}

func (c *ClientStats) HasSampleRate() bool {
	return c.SampleRate > 0
}

type ChannelStatsList []*ChannelStats

func (c ChannelStatsList) Len() int      { return len(c) }
func (c ChannelStatsList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ChannelStatsByHost struct {
	ChannelStatsList
}

func (c ChannelStatsByHost) Less(i, j int) bool {
	return c.ChannelStatsList[i].Hostname < c.ChannelStatsList[j].Hostname
}

type ClientStatsList []*ClientStats

func (c ClientStatsList) Len() int      { return len(c) }
func (c ClientStatsList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ClientsByHost struct {
	ClientStatsList
}

func (c ClientsByHost) Less(i, j int) bool {
	return c.ClientStatsList[i].Hostname < c.ClientStatsList[j].Hostname
}

type TopicStatsList []*TopicStats

func (t TopicStatsList) Len() int      { return len(t) }
func (t TopicStatsList) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicStatsByHost struct {
	TopicStatsList
}

func (c TopicStatsByHost) Less(i, j int) bool {
	return c.TopicStatsList[i].Hostname < c.TopicStatsList[j].Hostname
}

type Producers []*Producer

func (t Producers) Len() int      { return len(t) }
func (t Producers) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

func (t Producers) HTTPAddrs() []string {
	var addrs []string
	for _, p := range t {
		addrs = append(addrs, p.HTTPAddress())
	}
	return addrs
}

func (t Producers) Search(needle string) *Producer {
	for _, producer := range t {
		if needle == producer.HTTPAddress() {
			return producer
		}
	}
	return nil
}

type ProducersByHost struct {
	Producers
}

func (c ProducersByHost) Less(i, j int) bool {
	return c.Producers[i].Hostname < c.Producers[j].Hostname
}
