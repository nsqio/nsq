package main

import (
	"../util"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
)

// print out stats for each topic/channel
// TODO: show number of connected clients per channel
func statsHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write(api_response(500, "INVALID_REQUEST", nil))
		return
	}

	formatString, _ := reqParams.Query("format")
	jsonFormat := formatString == "json"

	nsqd.RLock()
	defer nsqd.RUnlock()

	if len(nsqd.topicMap) == 0 {
		if jsonFormat {
			w.Write(api_response(500, "NO_TOPICS", nil))
			return
		} else {
			io.WriteString(w, "NO_TOPICS\n")
		}
	}

	output := make([]interface{}, len(nsqd.topicMap))
	i := 0
	for topicName, t := range nsqd.topicMap {
		if !jsonFormat {
			io.WriteString(w, fmt.Sprintf("\nTopic: %s\n", topicName))
		}

		t.RLock()
		channels := make([]interface{}, len(t.channelMap))
		j := 0
		for channelName, c := range t.channelMap {
			if jsonFormat {
				channels[j] = struct {
					ChannelName      string `json:"channel_name"`
					Depth            int64  `json:"depth"`
					InFlightMessages int    `json:"in_flight_messages"`
					GetCount         int64  `json:"get_count"`
					PutCount         int64  `json:"put_count"`
					RequeueCount     int64  `json:"requeue_count"`
					TimeoutCount     int64  `json:"timeout_count"`
				}{
					channelName,
					c.backend.Depth(),
					len(c.inFlightMessages),
					c.GetCount,
					c.PutCount,
					c.RequeueCount,
					c.TimeoutCount,
				}
				j += 1
			} else {

				io.WriteString(w,
					fmt.Sprintf("    [%s] depth: %-5d inflt: %-4d get: %-8d put: %-8d re-q: %-5d timeout: %-5d\n",
						channelName,
						c.backend.Depth(),
						len(c.inFlightMessages),
						c.GetCount,
						c.PutCount,
						c.RequeueCount,
						c.TimeoutCount))
			}
		}
		t.RUnlock()

		output[i] = struct {
			TopicName string        `json:"topic_name"`
			Channels  []interface{} `json:"channels"`
		}{
			TopicName: topicName,
			Channels:  channels,
		}
		i += 1

	}

	if jsonFormat {
		w.Write(api_response(200, "OK", output))
	}

}

func api_response(statusCode int, statusTxt string, data interface{}) []byte {
	response, err := json.Marshal(struct {
		StatusCode int         `json:"status_code"`
		StatusTxt  string      `json:"status_txt"`
		Data       interface{} `json:"data"`
	}{
		statusCode,
		statusTxt,
		data,
	})
	if err != nil {
		errorTxt := fmt.Sprintf(`{"status_code":500, "status_txt":"%s","data":null}`, err.Error())
		return []byte(errorTxt)
	}
	return response
}
