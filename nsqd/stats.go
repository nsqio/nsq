package main

import (
	"fmt"
	"io"
	"net/http"
)

func statsHandler(w http.ResponseWriter, req *http.Request) {
	// TODO: one cannot simply walk... over a map in a goroutine
	for topicName, _ := range nsqd.topicMap {
		io.WriteString(w, fmt.Sprintf("%s\n", topicName))
	}
}
