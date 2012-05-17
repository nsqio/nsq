package server

import (
	"../message"
	"fmt"
	"io"
	"net/http"
)

func statsHandler(w http.ResponseWriter, req *http.Request) {
	for topicName, _ := range message.TopicMap {
		io.WriteString(w, fmt.Sprintf("%s\n", topicName))
	}
}
