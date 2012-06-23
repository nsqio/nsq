package main

import (
	"fmt"
	"io"
	"net/http"
)

// print out stats for each topic/channel
// TODO: json output
// TODO: show number of connected clients per channel
func statsHandler(w http.ResponseWriter, req *http.Request) {
	nsqd.RLock()
	defer nsqd.RUnlock()

	if len(nsqd.topicMap) == 0 {
		io.WriteString(w, "NO_TOPICS\n")
	}

	for topicName, t := range nsqd.topicMap {
		io.WriteString(w, fmt.Sprintf("\nTopic: %s\n", topicName))

		for channelName, c := range t.channelMap {
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
}
