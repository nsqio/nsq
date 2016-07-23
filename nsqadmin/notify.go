package nsqadmin

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"os"
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
	URL       string `json:"url"` // The URL of the HTTP request that triggered this action
	Via       string `json:"via"` // the Hostname of the nsqadmin performing this action
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

func (s *httpServer) notifyAdminAction(action, topic, channel, node string, req *http.Request) {
	if s.ctx.nsqadmin.getOpts().NotificationHTTPEndpoint == "" {
		return
	}
	via, _ := os.Hostname()

	u := url.URL{
		Scheme:   "http",
		Host:     req.Host,
		Path:     req.URL.Path,
		RawQuery: req.URL.RawQuery,
	}
	if req.TLS != nil || req.Header.Get("X-Scheme") == "https" {
		u.Scheme = "https"
	}

	a := &AdminAction{
		Action:    action,
		Topic:     topic,
		Channel:   channel,
		Node:      node,
		Timestamp: time.Now().Unix(),
		User:      basicAuthUser(req),
		RemoteIP:  req.RemoteAddr,
		UserAgent: req.UserAgent(),
		URL:       u.String(),
		Via:       via,
	}
	// Perform all work in a new goroutine so this never blocks
	go func() { s.ctx.nsqadmin.notifications <- a }()
}
