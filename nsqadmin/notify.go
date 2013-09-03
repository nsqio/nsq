package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"github.com/bitly/nsq/util"
	"log"
	"net/http"
	"strings"
	"time"
)

type AdminAction struct {
	Action    string `json:"action"`
	Topic     string `json:"topic"`
	Channel   string `json:"channel,omitempty"`
	Node      string `json:"node,omitempty"`
	Timestamp int64  `json:"timestamp"`
	User      string `json:"user,omitempty"`
	RemoteIP  string `json:"remote_ip"`
	UserAgent string `json:"user_agent"`
}

func HandleAdminActions() {
	for action := range notifications {
		content, err := json.Marshal(action)
		if err != nil {
			log.Printf("Error serializing admin action! %s", err)
		}
		httpclient := &http.Client{Transport: util.NewDeadlineTransport(10 * time.Second)}
		log.Printf("Posting notification to %s", *notificationHTTPEndpoint)
		_, err = httpclient.Post(*notificationHTTPEndpoint, "application/json", bytes.NewBuffer(content))
		if err != nil {
			log.Printf("Error posting notification: %s", err)
		}
	}
}

func basicAuthUser(req *http.Request) string {
	s := strings.SplitN(req.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 || s[0] != "Basic" {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(s[1])
	if err != nil {
		return ""
	}
	pair := strings.SplitN(string(b), ":", 2)
	if len(pair) != 2 {
		return ""
	}
	return pair[0]
}

func NotifyAdminAction(actionType string, topicName string, channelName string, node string, req *http.Request) {
	if *notificationHTTPEndpoint == "" {
		return
	}
	action := &AdminAction{
		actionType,
		topicName,
		channelName,
		node,
		time.Now().Unix(),
		basicAuthUser(req),
		req.RemoteAddr,
		req.UserAgent(),
	}
	// Perform all work in a new goroutine so this never blocks
	go func() { notifications <- action }()
}
