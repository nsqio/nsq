package main

import (
	"fmt"
	"io"
	"net/http"
)

func statsHandler(w http.ResponseWriter, req *http.Request) {
	for topicName, _ := range TopicMap {
		io.WriteString(w, fmt.Sprintf("%s\n", topicName))
	}
}
