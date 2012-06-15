package main

import (
	"fmt"
	"io"
	"net/http"
)

func statsHandler(w http.ResponseWriter, req *http.Request) {
	for topicName, _ := range topicMap {
		io.WriteString(w, fmt.Sprintf("%s\n", topicName))
	}
}
