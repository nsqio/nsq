package clusterinfo

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/absolute8511/gorpc"
	"github.com/absolute8511/nsq/internal/quantile"
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

type ClientPubStats struct {
	RemoteAddress string `json:"remote_address"`
	UserAgent     string `json:"user_agent"`
	Protocol      string `json:"protocol"`
	PubCount      int64  `json:"pub_count"`
	ErrCount      int64  `json:"err_count"`
	LastPubTs     int64  `json:"last_pub_ts"`
}

type NodeStat struct {
	HostName         string  `json:"hostname"`
	BroadcastAddress string  `json:"broadcast_address"`
	TCPPort          int     `json:"tcp_port"`
	HTTPPort         int     `json:"http_port"`
	LeaderLoadFactor float64 `json:"leader_load_factor"`
	NodeLoadFactor   float64 `json:"node_load_factor"`
}

type ClusterNodeInfo struct {
	Stable       bool        `json:"stable"`
	NodeStatList []*NodeStat `json:"node_stat_list"`
}

type TopicStats struct {
	Node           string        `json:"node"`
	Hostname       string        `json:"hostname"`
	TopicName      string        `json:"topic_name"`
	TopicPartition string        `json:"topic_partition"`
	StatsdName     string        `json:"statsd_name"`
	IsLeader       bool          `json:"is_leader"`
	IsMultiOrdered bool          `json:"is_multi_ordered"`
	IsExt          bool          `json:"is_ext"`
	SyncingNum     int           `json:"syncing_num"`
	ISRStats       []ISRStat     `json:"isr_stats"`
	CatchupStats   []CatchupStat `json:"catchup_stats"`
	Depth          int64         `json:"depth"`
	MemoryDepth    int64         `json:"memory_depth"`
	// the queue maybe auto cleaned, so the start means the queue oldest offset.
	BackendStart           int64            `json:"backend_start"`
	BackendDepth           int64            `json:"backend_depth"`
	MessageCount           int64            `json:"message_count"`
	NodeStats              []*TopicStats    `json:"nodes"`
	Channels               []*ChannelStats  `json:"channels"`
	TotalChannelDepth      int64            `json:"total_channel_depth"`
	Paused                 bool             `json:"paused"`
	HourlyPubSize          int64            `json:"hourly_pubsize"`
	PartitionHourlyPubSize []int64          `json:"partition_hourly_pubsize"`
	Clients                []ClientPubStats `json:"client_pub_stats"`
	MessageSizeStats       [16]int64        `json:"msg_size_stats"`
	MessageLatencyStats    [16]int64        `json:"msg_write_latency_stats"`

	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate `json:"e2e_processing_latency"`
}

type TopicMsgStatsInfo struct {
	// <100bytes, <1KB, 2KB, 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB, 512KB, 1MB, 2MB, 4MB
	MsgSizeStats [16]int64
	// <1024us, 2ms, 4ms, 8ms, 16ms, 32ms, 64ms, 128ms, 256ms, 512ms, 1024ms, 2048ms, 4s, 8s
	MsgWriteLatencyStats [16]int64
}

func (t *TopicStats) Add(a *TopicStats) {
	t.Node = "*"
	if t.IsMultiOrdered || a.IsMultiOrdered {
		// for multi ordered partitions, it may have several partitions on the single node,
		// so in order to get all the partitions, we should query "topic.*" to get all partitions
		t.StatsdName = t.TopicName + ".*"
	} else {
		t.StatsdName = t.TopicName
	}

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
	sort.Sort(TopicStatsByPartitionAndHost{t.NodeStats})
	if t.E2eProcessingLatency == nil {
		t.E2eProcessingLatency = &quantile.E2eProcessingLatencyAggregate{
			Addr:  t.Node,
			Topic: t.TopicName,
		}
	}
	t.E2eProcessingLatency.Add(a.E2eProcessingLatency)
}

type ChannelStats struct {
	Node               string          `json:"node"`
	Hostname           string          `json:"hostname"`
	TopicName          string          `json:"topic_name"`
	TopicPartition     string          `json:"topic_partition"`
	StatsdName         string          `json:"statsd_name"`
	ChannelName        string          `json:"channel_name"`
	Depth              int64           `json:"depth"`
	DepthSize          int64           `json:"depth_size"`
	DepthTimestamp     string          `json:"depth_ts"`
	MemoryDepth        int64           `json:"memory_depth"`
	BackendDepth       int64           `json:"backend_depth"`
	InFlightCount      int64           `json:"in_flight_count"`
	DeferredCount      int64           `json:"deferred_count"`
	RequeueCount       int64           `json:"requeue_count"`
	TimeoutCount       int64           `json:"timeout_count"`
	MessageCount       int64           `json:"message_count"`
	DelayedQueueCount  uint64          `json:"delayed_queue_count"`
	DelayedQueueRecent string          `json:"delayed_queue_recent"`
	ClientCount        int             `json:"-"`
	Selected           bool            `json:"-"`
	NodeStats          []*ChannelStats `json:"nodes"`
	Clients            []*ClientStats  `json:"clients"`
	Paused             bool            `json:"paused"`
	Skipped            bool            `json:"skipped"`
	IsMultiOrdered     bool            `json:"is_multi_ordered"`
	IsExt              bool            `json:"is_ext"`

	E2eProcessingLatency *quantile.E2eProcessingLatencyAggregate `json:"e2e_processing_latency"`
}

func (c *ChannelStats) Add(a *ChannelStats) {
	c.Node = "*"
	if c.IsMultiOrdered || a.IsMultiOrdered {
		c.StatsdName = c.TopicName + ".*"
	} else {
		c.StatsdName = c.TopicName
	}
	c.Depth += a.Depth
	c.DepthSize += a.DepthSize
	if c.DepthTimestamp == "" {
		c.DepthTimestamp = a.DepthTimestamp
	} else if a.DepthTimestamp < c.DepthTimestamp {
		c.DepthTimestamp = a.DepthTimestamp
	}
	c.MemoryDepth += a.MemoryDepth
	c.BackendDepth += a.BackendDepth
	c.InFlightCount += a.InFlightCount
	c.DeferredCount += a.DeferredCount
	c.RequeueCount += a.RequeueCount
	c.TimeoutCount += a.TimeoutCount
	c.MessageCount += a.MessageCount
	c.DelayedQueueCount += a.DelayedQueueCount
	if c.DelayedQueueRecent == "" {
		c.DelayedQueueRecent = a.DelayedQueueRecent
	} else if a.DelayedQueueRecent < c.DelayedQueueRecent {
		c.DelayedQueueRecent = a.DelayedQueueRecent
	}

	c.ClientCount += a.ClientCount
	if a.Paused {
		c.Paused = a.Paused
	}
	if a.Skipped {
		c.Skipped = a.Skipped
	}
	c.NodeStats = append(c.NodeStats, a)
	sort.Sort(ChannelStatsByPartAndHost{c.NodeStats})
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
	TimeoutCount      int64         `json:"timeout_count"`
	DeferredCount     int64         `json:"deferred_count"`
	SampleRate        int32         `json:"sample_rate"`
	Deflate           bool          `json:"deflate"`
	Snappy            bool          `json:"snappy"`
	Authed            bool          `json:"authed"`
	AuthIdentity      string        `json:"auth_identity"`
	AuthIdentityURL   string        `json:"auth_identity_url"`

	DesiredTag string `json:"desired_tag"`

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

type ChannelStatsByPartAndHost struct {
	ChannelStatsList
}

func (c ChannelStatsByPartAndHost) Less(i, j int) bool {
	if c.ChannelStatsList[i].TopicPartition == c.ChannelStatsList[j].TopicPartition {
		return c.ChannelStatsList[i].Hostname < c.ChannelStatsList[j].Hostname
	}
	l, _ := strconv.Atoi(c.ChannelStatsList[i].TopicPartition)
	r, _ := strconv.Atoi(c.ChannelStatsList[j].TopicPartition)

	return l < r
}

type ClientStatsList []*ClientStats

func (c ClientStatsList) Len() int      { return len(c) }
func (c ClientStatsList) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ClientsByHost struct {
	ClientStatsList
}

func (c ClientsByHost) Less(i, j int) bool {
	if c.ClientStatsList[i].Hostname == c.ClientStatsList[j].Hostname {
		return c.ClientStatsList[i].RemoteAddress < c.ClientStatsList[j].RemoteAddress
	}
	return c.ClientStatsList[i].Hostname < c.ClientStatsList[j].Hostname
}

type TopicStatsList []*TopicStats

func (t TopicStatsList) Len() int      { return len(t) }
func (t TopicStatsList) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicStatsByPartitionAndHost struct {
	TopicStatsList
}

func (c TopicStatsByPartitionAndHost) Less(i, j int) bool {
	if c.TopicStatsList[i].TopicPartition == c.TopicStatsList[j].TopicPartition {
		return c.TopicStatsList[i].Hostname < c.TopicStatsList[j].Hostname
	}
	l, _ := strconv.Atoi(c.TopicStatsList[i].TopicPartition)
	r, _ := strconv.Atoi(c.TopicStatsList[j].TopicPartition)
	return l < r
}

type TopicStatsByHourlyPubsize struct {
	TopicStatsList
}

func (c TopicStatsByHourlyPubsize) Less(i, j int) bool {
	if c.TopicStatsList[i].HourlyPubSize == c.TopicStatsList[j].HourlyPubSize {
		return c.TopicStatsList[i].Hostname < c.TopicStatsList[j].Hostname
	}
	l := c.TopicStatsList[i].HourlyPubSize
	r := c.TopicStatsList[j].HourlyPubSize
	return l > r
}

type TopicStatsByChannelDepth struct {
	TopicStatsList
}

func (c TopicStatsByChannelDepth) Less(i, j int) bool {
	if c.TopicStatsList[i].TotalChannelDepth == c.TopicStatsList[j].TotalChannelDepth {
		return c.TopicStatsList[i].Hostname < c.TopicStatsList[j].Hostname
	}
	l := c.TopicStatsList[i].TotalChannelDepth
	r := c.TopicStatsList[j].TotalChannelDepth
	return l > r
}

type TopicStatsByMessageCount struct {
	TopicStatsList
}

func (c TopicStatsByMessageCount) Less(i, j int) bool {
	if c.TopicStatsList[i].MessageCount == c.TopicStatsList[j].MessageCount {
		return c.TopicStatsList[i].Hostname < c.TopicStatsList[j].Hostname
	}
	l := c.TopicStatsList[i].MessageCount
	r := c.TopicStatsList[j].MessageCount
	return l > r
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

type ISRStat struct {
	HostName string `json:"hostname"`
	NodeID   string `json:"node_id"`
}

type CatchupStat struct {
	HostName string `json:"hostname"`
	NodeID   string `json:"node_id"`
	Progress int    `json:"progress"`
}

type TopicCoordStat struct {
	Node         string        `json:"node"`
	Name         string        `json:"name"`
	Partition    int           `json:"partition"`
	ISRStats     []ISRStat     `json:"isr_stats"`
	CatchupStats []CatchupStat `json:"catchup_stats"`
}

type CoordStats struct {
	RpcStats        *gorpc.ConnStats `json:"rpc_stats"`
	TopicCoordStats []TopicCoordStat `json:"topic_coord_stats"`
}

type MessageHistoryStat []int64

type NsqLookupdNodeInfo struct {
	ID       string
	NodeIP   string
	TcpPort  string
	HttpPort string
	RpcPort  string
}

type LookupdNodes struct {
	LeaderNode NsqLookupdNodeInfo   `json:"lookupdleader"`
	AllNodes   []NsqLookupdNodeInfo `json:"lookupdnodes"`
}

type NodeHourlyPubsize struct {
	TopicName      string `json:"topic_name"`
	TopicPartition string `json:"topic_partition"`
	HourlyPubSize  int64  `json:"hourly_pub_size"`
}
