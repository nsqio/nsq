package main

import (
	"../util"
	"fmt"
	"io"
	"log"
	"net/http"
)

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
			io.WriteString(w, fmt.Sprintf("\n[%s] depth: %-5d be-depth: %-5d\n",
				topicName,
				int64(len(t.memoryMsgChan))+t.backend.Depth(),
				t.backend.Depth()))
		}

		channels := make([]interface{}, len(t.channelMap))
		j := 0
		for channelName, c := range t.channelMap {
			c.RLock()
			if jsonFormat {
				clients := make([]interface{}, len(c.clients))
				for ci, client := range c.clients {
					state, _ := client.GetState("state")
					clients[ci] = struct {
						Name  string `json:"name"`
						State int    `json:"state"`
					}{
						client.String(),
						state.(int),
					}
				}
				channels[j] = struct {
					ChannelName      string        `json:"channel_name"`
					Depth            int64         `json:"depth"`
					BackendDepth     int64         `json:"backend_depth"`
					InFlightMessages int           `json:"in_flight_messages"`
					GetCount         int64         `json:"get_count"`
					PutCount         int64         `json:"put_count"`
					RequeueCount     int64         `json:"requeue_count"`
					TimeoutCount     int64         `json:"timeout_count"`
					Clients          []interface{} `json:"clients"`
				}{
					channelName,
					int64(len(c.memoryMsgChan)) + c.backend.Depth(),
					c.backend.Depth(),
					len(c.inFlightMessages),
					c.getCount,
					c.putCount,
					c.requeueCount,
					c.timeoutCount,
					clients,
				}
				j += 1
			} else {
				io.WriteString(w,
					fmt.Sprintf("    [%s] depth: %-5d be-depth: %-5d inflt: %-4d get: %-8d put: %-8d re-q: %-5d timeout: %-5d\n",
						channelName,
						int64(len(c.memoryMsgChan))+c.backend.Depth(),
						c.backend.Depth(),
						len(c.inFlightMessages),
						c.getCount,
						c.putCount,
						c.requeueCount,
						c.timeoutCount))
				for _, client := range c.clients {
					state, _ := client.GetState("state")
					io.WriteString(w, fmt.Sprintf("        [%s] state: %d\n", client.String(), state.(int)))
				}
			}
			c.RUnlock()
		}

		topics[i] = struct {
			TopicName    string        `json:"topic_name"`
			Channels     []interface{} `json:"channels"`
			Depth        int64         `json:"depth"`
			BackendDepth int64         `json:"backend_depth"`
		}{
			TopicName:    topicName,
			Channels:     channels,
			Depth:        int64(len(t.memoryMsgChan)) + t.backend.Depth(),
			BackendDepth: t.backend.Depth(),
		}
		i += 1

		t.RUnlock()
	}

	if jsonFormat {
		w.Write(util.ApiResponse(200, "OK", struct {
			Topics []interface{} `json:"topics"`
		}{topics}))
	}

}
