package main

import (
	"../util"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type ClientStats struct {
	version       string
	name          string
	state         int
	inFlightCount int64
	readyCount    int64
	messageCount  uint64
	connectTime   time.Time
}

// print out stats for each topic/channel
func statsHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write(util.ApiResponse(500, "INVALID_REQUEST", nil))
		return
	}

	formatString, _ := reqParams.Query("format")
	jsonFormat := formatString == "json"
	now := time.Now()

	nsqd.RLock()
	defer nsqd.RUnlock()

	if len(nsqd.topicMap) == 0 {
		if jsonFormat {
			w.Write(util.ApiResponse(500, "NO_TOPICS", nil))
		} else {
			io.WriteString(w, "NO_TOPICS\n")
		}
		return
	}

	topics := make([]interface{}, len(nsqd.topicMap))
	i := 0
	for topicName, t := range nsqd.topicMap {
		t.RLock()

		if !jsonFormat {
			io.WriteString(w, fmt.Sprintf("\n[%s] depth: %-5d be-depth: %-5d msgs %-8d\n",
				topicName,
				int64(len(t.memoryMsgChan))+t.backend.Depth(),
				t.backend.Depth(),
				t.messageCount))
		}

		channels := make([]interface{}, len(t.channelMap))
		j := 0
		for channelName, c := range t.channelMap {
			c.RLock()
			if jsonFormat {
				clients := make([]interface{}, len(c.clients))
				for ci, client := range c.clients {
					clientStats := client.Stats()
					clients[ci] = struct {
						Version       string `json:"version"`
						Name          string `json:"name"`
						State         int    `json:"state"`
						ReadyCount    int64  `json:"ready_count"`
						InFlightCount int64  `json:"in_flight_count"`
						MessageCount  uint64 `json:"message_count"`
						ConnectTime   int64  `json:"connect_ts"`
					}{
						clientStats.version,
						clientStats.name,
						clientStats.state,
						clientStats.readyCount,
						clientStats.inFlightCount,
						clientStats.messageCount,
						clientStats.connectTime.Unix(),
					}
				}
				channels[j] = struct {
					ChannelName   string        `json:"channel_name"`
					Depth         int64         `json:"depth"`
					BackendDepth  int64         `json:"backend_depth"`
					InFlightCount int           `json:"in_flight_count"`
					DeferredCount int           `json:"deferred_count"`
					MessageCount  uint64        `json:"message_count"`
					RequeueCount  uint64        `json:"requeue_count"`
					TimeoutCount  uint64        `json:"timeout_count"`
					Clients       []interface{} `json:"clients"`
				}{
					channelName,
					int64(len(c.memoryMsgChan)) + c.backend.Depth(),
					c.backend.Depth(),
					len(c.inFlightMessages),
					len(c.deferredMessages),
					c.messageCount,
					c.requeueCount,
					c.timeoutCount,
					clients,
				}
				j++
			} else {
				io.WriteString(w,
					fmt.Sprintf("    [%s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d msgs: %-8d re-q: %-5d timeout: %-5d\n",
						channelName,
						int64(len(c.memoryMsgChan))+c.backend.Depth(),
						c.backend.Depth(),
						len(c.inFlightMessages),
						len(c.deferredMessages),
						c.messageCount,
						c.requeueCount,
						c.timeoutCount))
				for _, client := range c.clients {
					clientStats := client.Stats()
					duration := now.Sub(clientStats.connectTime).Seconds()
					io.WriteString(w, fmt.Sprintf("        [%s %s] state: %d inflt: %-4d rdy: %-4d msgs: %-8d connected: %s\n",
						clientStats.version,
						clientStats.name,
						clientStats.state,
						clientStats.inFlightCount,
						clientStats.readyCount,
						clientStats.messageCount,
						time.Duration(int64(duration))*time.Second, // truncate to the second
					))
				}
			}
			c.RUnlock()
		}

		topics[i] = struct {
			TopicName    string        `json:"topic_name"`
			Channels     []interface{} `json:"channels"`
			Depth        int64         `json:"depth"`
			BackendDepth int64         `json:"backend_depth"`
			MessageCount uint64        `json:"message_count"`
		}{
			TopicName:    topicName,
			Channels:     channels,
			Depth:        int64(len(t.memoryMsgChan)) + t.backend.Depth(),
			BackendDepth: t.backend.Depth(),
			MessageCount: t.messageCount,
		}
		i++

		t.RUnlock()
	}

	if jsonFormat {
		w.Write(util.ApiResponse(200, "OK", struct {
			Topics []interface{} `json:"topics"`
		}{topics}))
	}

}
