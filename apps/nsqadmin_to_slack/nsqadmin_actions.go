package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/nsqio/nsq/apps/nsqadmin_to_slack/internal/slack"
)

type AdminAction struct {
	Action    string `json:"action"`
	Topic     string `json:"topic"`
	Channel   string `json:"channel,omitempty"`
	Node      string `json:"node,omitempty"`
	Timestamp int64  `json:"timestamp"`
	User      string `json:"user,omitempty"`
	UserEmail string `json:"user_email,omitempty"` // from X-Forwarded-Email
	RemoteIP  string `json:"remote_ip"`
	UserAgent string `json:"user_agent"`
	URL       string `json:"url"` // The URL of the HTTP request that triggered this action
	Via       string `json:"via"` // the Hostname of the nsqadmin performing this action
}

// SlackMsg formats an appropriate message for posting to Slack
func (msg AdminAction) SlackMsg() string {
	var txt string
	switch msg.Action {
	case "create_topic":
		txt = "Created Topic"
	case "create_channel":
		txt = "Created Channel"
	case "delete_topic":
		txt = "Deleted Topic"
	case "delete_channel":
		txt = "Deleted Channel"
	case "empty_topic":
		txt = "Emptied Topic"
	case "empty_channel":
		txt = "Emptied Channel"
	case "pause_topic":
		txt = "Paused Topic"
	case "pause_channel":
		txt = "Paused Channel"
	case "unpause_topic":
		txt = "Unpaused Topic"
	case "unpause_channel":
		txt = "Unpaused Channel"
	case "tombstone_topic_producer":
		txt = "Tombstoned Topic"
	default:
		txt = msg.Action
	}

	name := msg.Topic
	if msg.Channel != "" {
		name = fmt.Sprintf("%s/%s", name, msg.Channel)
	}

	u, err := url.Parse(msg.URL)
	if err != nil {
		log.Printf("failed parsing %q %s. using %s instead", msg.URL, err, msg.Via)
		u = &url.URL{
			Scheme: "http",
			Host:   msg.Via,
		}
	}
	u.Path = ""
	u.RawQuery = ""

	switch msg.Action {
	case "toombstone_topic_producer":
		txt += fmt.Sprintf(" `%s` on %s", name, msg.Node)
	case "delete_topic", "delete_channel":
		txt += fmt.Sprintf(" `%s`", name)
	default:
		u.Path = fmt.Sprintf("/topics/%s", name)
		txt += fmt.Sprintf(" <%s|%s>", u, name)
	}
	u.Path = ""
	txt += fmt.Sprintf(" from <%s|%s>", u, u.Host)

	// warn if we are retrying this message after a long period of time
	if msg.Timestamp > 0 {
		t := time.Unix(msg.Timestamp, 0)
		duration := time.Since(t)
		if duration > time.Minute*2 {
			txt += fmt.Sprintf("\n\nWARNING: outdated notification from %s ago %s", duration, t.Format(time.RFC3339))
		}
	}
	return txt
}

// nsqadmin sends notifications for nsqadmin actions to #ops
func (h *Handler) nsqadmin(b []byte) error {
	var msg AdminAction
	err := json.Unmarshal(b, &msg)
	if err != nil {
		return err
	}

	username := msg.User
	// prefer an exact slack username match via email
	if msg.UserEmail != "" {
		if user, _ := h.Slack.UserByEmail(msg.UserEmail); user != nil {
			username = user.Name
		}
	}

	txt := msg.SlackMsg()
	log.Printf("sending %s %s %q", username, h.SlackChannel, txt)
	err = h.Slack.ChatPostMessage(&slack.Message{
		Channel: h.SlackChannel,
		Text:     txt,
		Username: username,
		IconURL:  "https://s3.amazonaws.com/static.bitly.com/graphics/eng/nsq_logo_small.png",
	})
	return err
}
