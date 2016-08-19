package nsqd

import (
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/quantile"
)

type TopicStats struct {
	TopicName      string           `json:"topic_name"`
	TopicFullName  string           `json:"topic_full_name"`
	TopicPartition string           `json:"topic_partition"`
	Channels       []ChannelStats   `json:"channels"`
	Depth          int64            `json:"depth"`
	BackendDepth   int64            `json:"backend_depth"`
	BackendStart   int64            `json:"backend_start"`
	MessageCount   uint64           `json:"message_count"`
	IsLeader       bool             `json:"is_leader"`
	HourlyPubSize  int64            `json:"hourly_pubsize"`
	Clients        []ClientPubStats `json:"client_pub_stats"`

	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

func NewTopicStats(t *Topic, channels []ChannelStats) TopicStats {
	return TopicStats{
		TopicName:      t.GetTopicName(),
		TopicFullName:  t.GetFullName(),
		TopicPartition: strconv.Itoa(t.GetTopicPart()),
		Channels:       channels,
		Depth:          t.TotalDataSize(),
		BackendDepth:   t.TotalDataSize(),
		BackendStart:   t.GetQueueReadStart(),
		MessageCount:   t.TotalMessageCnt(),
		IsLeader:       !t.IsWriteDisabled(),
		Clients:        t.GetPubStats(),

		E2eProcessingLatency: t.AggregateChannelE2eProcessingLatency().Result(),
	}
}

type ChannelStats struct {
	ChannelName string `json:"channel_name"`
	// message size need to consume
	Depth          int64  `json:"depth"`
	DepthSize      int64  `json:"depth_size"`
	DepthTimestamp string `json:"depth_ts"`
	BackendDepth   int64  `json:"backend_depth"`
	// total size sub past hour on this channel
	HourlySubSize int64         `json:"hourly_subsize"`
	InFlightCount int           `json:"in_flight_count"`
	DeferredCount int           `json:"deferred_count"`
	MessageCount  uint64        `json:"message_count"`
	RequeueCount  uint64        `json:"requeue_count"`
	TimeoutCount  uint64        `json:"timeout_count"`
	Clients       []ClientStats `json:"clients"`
	Paused        bool          `json:"paused"`

	E2eProcessingLatency *quantile.Result `json:"e2e_processing_latency"`
}

func NewChannelStats(c *Channel, clients []ClientStats) ChannelStats {
	return ChannelStats{
		ChannelName:    c.name,
		Depth:          c.Depth(),
		DepthTimestamp: time.Unix(0, c.DepthTimestamp()).String(),
		// the message bytes need to be consumed
		DepthSize:     c.DepthSize(),
		BackendDepth:  c.backend.Depth(),
		InFlightCount: len(c.inFlightMessages),
		// this is total message count need consume.
		// may diff with topic total size since some is in buffer.
		// TODO: this should be the messages count that already be consumed.
		MessageCount:  uint64(c.backend.GetQueueReadEnd().TotalMsgCnt()),
		RequeueCount:  atomic.LoadUint64(&c.requeueCount),
		DeferredCount: int(atomic.LoadInt64(&c.deferredCount)),
		TimeoutCount:  atomic.LoadUint64(&c.timeoutCount),
		Clients:       clients,
		Paused:        c.IsPaused(),

		E2eProcessingLatency: c.e2eProcessingLatencyStream.Result(),
	}
}

type ClientPubStats struct {
	RemoteAddress string `json:"remote_address"`
	UserAgent     string `json:"user_agent"`
	Protocol      string `json:"protocol"`
	PubCount      int64  `json:"pub_count"`
	ErrCount      int64  `json:"err_count"`
	LastPubTs     int64  `json:"last_pub_ts"`
}

type ClientStats struct {
	// TODO: deprecated, remove in 1.0
	Name string `json:"name"`

	ClientID        string `json:"client_id"`
	Hostname        string `json:"hostname"`
	Version         string `json:"version"`
	RemoteAddress   string `json:"remote_address"`
	State           int32  `json:"state"`
	ReadyCount      int64  `json:"ready_count"`
	InFlightCount   int64  `json:"in_flight_count"`
	MessageCount    uint64 `json:"message_count"`
	FinishCount     uint64 `json:"finish_count"`
	RequeueCount    uint64 `json:"requeue_count"`
	TimeoutCount    int64  `json:"timeout_count"`
	DeferredCount   int64  `json:"deferred_count"`
	ConnectTime     int64  `json:"connect_ts"`
	SampleRate      int32  `json:"sample_rate"`
	Deflate         bool   `json:"deflate"`
	Snappy          bool   `json:"snappy"`
	UserAgent       string `json:"user_agent"`
	Authed          bool   `json:"authed,omitempty"`
	AuthIdentity    string `json:"auth_identity,omitempty"`
	AuthIdentityURL string `json:"auth_identity_url,omitempty"`

	TLS                           bool   `json:"tls"`
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

type Topics []*Topic

func (t Topics) Len() int      { return len(t) }
func (t Topics) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicsByName struct {
	Topics
}

func (t TopicsByName) Less(i, j int) bool {
	return t.Topics[i].GetFullName() <
		t.Topics[j].GetFullName()
}

type Channels []*Channel

func (c Channels) Len() int      { return len(c) }
func (c Channels) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ChannelsByName struct {
	Channels
}

func (c ChannelsByName) Less(i, j int) bool { return c.Channels[i].name < c.Channels[j].name }

func (n *NSQD) GetStats() []TopicStats {
	n.RLock()
	realTopics := make([]*Topic, 0, len(n.topicMap))
	for _, topicParts := range n.topicMap {
		for _, t := range topicParts {
			realTopics = append(realTopics, t)
		}
	}
	n.RUnlock()

	return n.getTopicStats(realTopics)
}

func (n *NSQD) getTopicStats(realTopics []*Topic) []TopicStats {
	sort.Sort(TopicsByName{realTopics})
	topics := make([]TopicStats, 0, len(realTopics))
	for _, t := range realTopics {
		t.channelLock.RLock()
		realChannels := make([]*Channel, 0, len(t.channelMap))
		for _, c := range t.channelMap {
			realChannels = append(realChannels, c)
		}
		t.channelLock.RUnlock()
		sort.Sort(ChannelsByName{realChannels})
		channels := make([]ChannelStats, 0, len(realChannels))
		for _, c := range realChannels {
			c.RLock()
			clients := make([]ClientStats, 0, len(c.clients))
			for _, client := range c.clients {
				clients = append(clients, client.Stats())
			}
			c.RUnlock()
			channels = append(channels, NewChannelStats(c, clients))
		}
		topics = append(topics, NewTopicStats(t, channels))
	}
	return topics
}

func (n *NSQD) GetTopicStats(topic string) []TopicStats {
	n.RLock()
	realTopics := make([]*Topic, 0, len(n.topicMap))
	for name, topicParts := range n.topicMap {
		if name != topic {
			continue
		}
		for _, t := range topicParts {
			realTopics = append(realTopics, t)
		}
	}
	n.RUnlock()
	return n.getTopicStats(realTopics)
}
