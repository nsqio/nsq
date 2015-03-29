package nsqadmin

import (
	"encoding/base64"
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

func (s *httpServer) notifyAdminAction(actionType string, topicName string,
	channelName string, node string, req *http.Request) {
	if s.ctx.nsqadmin.opts.NotificationHTTPEndpoint == "" {
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
	go func() { s.ctx.nsqadmin.notifications <- action }()
}
