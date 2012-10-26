package main

import (
	"sort"
)

type TopicStats struct {
	TopicName    string         `json:"topic_name"`
	Channels     []ChannelStats `json:"channels"`
	Depth        int64          `json:"depth"`
	BackendDepth int64          `json:"backend_depth"`
	MessageCount uint64         `json:"message_count"`
}

func NewTopicStats(t *Topic, channels []ChannelStats) TopicStats {
	return TopicStats{
		TopicName:    t.name,
		Channels:     channels,
		Depth:        t.Depth(),
		BackendDepth: t.backend.Depth(),
		MessageCount: t.messageCount,
	}
}

type ChannelStats struct {
	ChannelName   string        `json:"channel_name"`
	Depth         int64         `json:"depth"`
	BackendDepth  int64         `json:"backend_depth"`
	InFlightCount int           `json:"in_flight_count"`
	DeferredCount int           `json:"deferred_count"`
	MessageCount  uint64        `json:"message_count"`
	RequeueCount  uint64        `json:"requeue_count"`
	TimeoutCount  uint64        `json:"timeout_count"`
	Clients       []ClientStats `json:"clients"`
	Paused        bool          `json:"paused"`
}

func NewChannelStats(c *Channel, clients []ClientStats) ChannelStats {
	return ChannelStats{
		ChannelName:   c.name,
		Depth:         c.Depth(),
		BackendDepth:  c.backend.Depth(),
		InFlightCount: len(c.inFlightMessages),
		DeferredCount: len(c.deferredMessages),
		MessageCount:  c.messageCount,
		RequeueCount:  c.requeueCount,
		TimeoutCount:  c.timeoutCount,
		Clients:       clients,
		Paused:        c.IsPaused(),
	}
}

type ClientStats struct {
	Version       string `json:"version"`
	RemoteAddress string `json:"remote_address"`
	Name          string `json:"name"`
	State         int32  `json:"state"`
	ReadyCount    int64  `json:"ready_count"`
	InFlightCount int64  `json:"in_flight_count"`
	MessageCount  uint64 `json:"message_count"`
	FinishCount   uint64 `json:"finish_count"`
	RequeueCount  uint64 `json:"requeue_count"`
	ConnectTime   int64  `json:"connect_ts"`
}

type Topics []*Topic

func (t Topics) Len() int      { return len(t) }
func (t Topics) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

type TopicsByName struct {
	Topics
}

func (t TopicsByName) Less(i, j int) bool { return t.Topics[i].name < t.Topics[j].name }

type Channels []*Channel

func (c Channels) Len() int      { return len(c) }
func (c Channels) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

type ChannelsByName struct {
	Channels
}

func (c ChannelsByName) Less(i, j int) bool { return c.Channels[i].name < c.Channels[j].name }

func (n *NSQd) getStats() []TopicStats {
	n.RLock()
	defer n.RUnlock()

	realTopics := make([]*Topic, len(nsqd.topicMap))
	topics := make([]TopicStats, len(nsqd.topicMap))
	topic_index := 0
	for _, t := range nsqd.topicMap {
		realTopics[topic_index] = t
		topic_index++
	}

	sort.Sort(TopicsByName{realTopics})
	for topic_index, t := range realTopics {
		t.RLock()

		realChannels := make([]*Channel, len(t.channelMap))
		channel_index := 0
		for _, c := range t.channelMap {
			realChannels[channel_index] = c
			channel_index++
		}
		sort.Sort(ChannelsByName{realChannels})

		channels := make([]ChannelStats, len(t.channelMap))
		for channel_index, c := range realChannels {
			c.RLock()
			clients := make([]ClientStats, len(c.clients))
			for client_index, client := range c.clients {
				clients[client_index] = client.Stats()
			}
			channels[channel_index] = NewChannelStats(c, clients)
			c.RUnlock()
		}

		topics[topic_index] = NewTopicStats(t, channels)

		t.RUnlock()
	}

	return topics
}
