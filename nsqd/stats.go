package main

import (
	"../util"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"time"
)

type ClientStats struct {
	version       string
	address       string
	name          string
	state         int32
	inFlightCount int64
	readyCount    int64
	messageCount  uint64
	finishCount   uint64
	requeueCount  uint64
	connectTime   time.Time
}

type Topics []*Topic
type TopicsByName struct {
	Topics
}
type Channels []*Channel
type ChannelsByName struct {
	Channels
}

func (t Topics) Len() int                   { return len(t) }
func (t Topics) Swap(i, j int)              { t[i], t[j] = t[j], t[i] }
func (c Channels) Len() int                 { return len(c) }
func (c Channels) Swap(i, j int)            { c[i], c[j] = c[j], c[i] }
func (t TopicsByName) Less(i, j int) bool   { return t.Topics[i].name < t.Topics[j].name }
func (c ChannelsByName) Less(i, j int) bool { return c.Channels[i].name < c.Channels[j].name }

// print out stats for each topic/channel
func statsHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		util.ApiResponse(w, 500, "INVALID_REQUEST", nil)
		return
	}

	formatString, _ := reqParams.Query("format")
	jsonFormat := formatString == "json"
	now := time.Now()

	nsqd.RLock()
	defer nsqd.RUnlock()

	if !jsonFormat {
		io.WriteString(w, fmt.Sprintf("nsqd v%s\n\n", util.BINARY_VERSION))
	}

	if len(nsqd.topicMap) == 0 {
		if jsonFormat {
			util.ApiResponse(w, 500, "NO_TOPICS", nil)
		} else {
			io.WriteString(w, "NO_TOPICS\n")
		}
		return
	}

	realTopics := make([]*Topic, len(nsqd.topicMap))
	topics := make([]interface{}, len(nsqd.topicMap))
	topic_index := 0
	for _, t := range nsqd.topicMap {
		realTopics[topic_index] = t
		topic_index++
	}

	sort.Sort(TopicsByName{realTopics})
	for topic_index, t := range realTopics {
		t.RLock()

		if !jsonFormat {
			io.WriteString(w, fmt.Sprintf("\n[%-15s] depth: %-5d be-depth: %-5d msgs: %-8d\n",
				t.name,
				t.Depth(),
				t.backend.Depth(),
				t.messageCount))
		}

		realChannels := make([]*Channel, len(t.channelMap))
		channel_index := 0
		for _, c := range t.channelMap {
			realChannels[channel_index] = c
			channel_index++
		}
		sort.Sort(ChannelsByName{realChannels})

		channels := make([]interface{}, len(t.channelMap))
		for channel_index, c := range realChannels {
			c.RLock()
			if jsonFormat {
				clients := make([]interface{}, len(c.clients))
				for client_index, client := range c.clients {
					clientStats := client.Stats()
					clients[client_index] = struct {
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
					}{
						clientStats.version,
						clientStats.address,
						clientStats.name,
						clientStats.state,
						clientStats.readyCount,
						clientStats.inFlightCount,
						clientStats.messageCount,
						clientStats.finishCount,
						clientStats.requeueCount,
						clientStats.connectTime.Unix(),
					}
				}
				channels[channel_index] = struct {
					ChannelName   string        `json:"channel_name"`
					Depth         int64         `json:"depth"`
					BackendDepth  int64         `json:"backend_depth"`
					InFlightCount int           `json:"in_flight_count"`
					DeferredCount int           `json:"deferred_count"`
					MessageCount  uint64        `json:"message_count"`
					RequeueCount  uint64        `json:"requeue_count"`
					TimeoutCount  uint64        `json:"timeout_count"`
					Clients       []interface{} `json:"clients"`
					Paused        bool          `json:"paused"`
				}{
					c.name,
					c.Depth(),
					c.backend.Depth(),
					len(c.inFlightMessages),
					len(c.deferredMessages),
					c.messageCount,
					c.requeueCount,
					c.timeoutCount,
					clients,
					c.IsPaused(),
				}
				channel_index++
			} else {
				var pausedPrefix string
				if c.IsPaused() {
					pausedPrefix = " *P "
				} else {
					pausedPrefix = "    "
				}
				io.WriteString(w,
					fmt.Sprintf("%s[%-25s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d re-q: %-5d timeout: %-5d msgs: %-8d\n",
						pausedPrefix,
						c.name,
						c.Depth(),
						c.backend.Depth(),
						len(c.inFlightMessages),
						len(c.deferredMessages),
						c.requeueCount,
						c.timeoutCount,
						c.messageCount))
				for _, client := range c.clients {
					clientStats := client.Stats()
					duration := now.Sub(clientStats.connectTime).Seconds()
					_, port, _ := net.SplitHostPort(clientStats.address)
					io.WriteString(w, fmt.Sprintf("        [%s %-21s] state: %d inflt: %-4d rdy: %-4d fin: %-8d re-q: %-8d msgs: %-8d connected: %s\n",
						clientStats.version,
						fmt.Sprintf("%s:%s", clientStats.name, port),
						clientStats.state,
						clientStats.inFlightCount,
						clientStats.readyCount,
						clientStats.finishCount,
						clientStats.requeueCount,
						clientStats.messageCount,
						time.Duration(int64(duration))*time.Second, // truncate to the second
					))
				}
			}
			c.RUnlock()
		}

		topics[topic_index] = struct {
			TopicName    string        `json:"topic_name"`
			Channels     []interface{} `json:"channels"`
			Depth        int64         `json:"depth"`
			BackendDepth int64         `json:"backend_depth"`
			MessageCount uint64        `json:"message_count"`
		}{
			TopicName:    t.name,
			Channels:     channels,
			Depth:        t.Depth(),
			BackendDepth: t.backend.Depth(),
			MessageCount: t.messageCount,
		}
		topic_index++

		t.RUnlock()
	}

	if jsonFormat {
		util.ApiResponse(w, 200, "OK", struct {
			Topics []interface{} `json:"topics"`
		}{topics})
	}

}
