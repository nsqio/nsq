package server

import (
	"net/http"
	"../message"
	"io"
	"fmt"
)

func statsHandler(w http.ResponseWriter, req *http.Request) {
	for topicName, _ := range message.TopicMap {
		io.WriteString(w, fmt.Sprintf("%s\n", topicName))
	}
}

