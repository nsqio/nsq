package main

import (
	"../nsq"
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type AdminAction struct {
	Action    string `json:"action"`
	Topic     string `json:"topic"`
	Channel   string `json:"channel"`
	Timestamp int64  `json:"timestamp"`
	User      string `json:"user"`
	RemoteIP  string `json:"remote_ip"`
	UserAgent string `json:"user_agent"`
}

func HandleAdminActions() {
	for action := range notifications {
		content, err := json.Marshal(action)
		if err != nil {
			log.Printf("Error serializing admin action! %s", err)
		}
		httpclient := &http.Client{Transport: nsq.NewDeadlineTransport(10 * time.Second)}
		log.Printf("Posting notification to %s", *notificationHTTPEndpoint)
		_, err = httpclient.Post(*notificationHTTPEndpoint, "application/json", bytes.NewBuffer(content))
		if err != nil {
			log.Printf("Error posting notification: %s", err)
		}
	}
}

func NotifyAdminAction(actionType string, topicName string, channelName string, req *http.Request) {
	if *notificationHTTPEndpoint == "" {
		return
	}
	var username string
	if req.URL.User != nil {
		username = req.URL.User.Username()
	}
	action := &AdminAction{
		actionType,
		topicName,
		channelName,
		time.Now().Unix(),
		username,
		req.RemoteAddr,
		req.UserAgent(),
	}
	// Perform all work in a new goroutine so this never blocks
	go func() { notifications <- action }()
}
