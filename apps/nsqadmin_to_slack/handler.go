package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/nsqio/nsq/apps/nsqadmin_to_slack/internal/slack"
	"github.com/nsqio/go-nsq"
)

type Handler struct {
	Slack *slack.Slack
	SlackChannel string
}

type Action struct {
	Action string `json:"action"`
}

func (h *Handler) HandleMessage(m *nsq.Message) error {
	var a Action
	err := json.Unmarshal(m.Body, &a)
	if err != nil {
		return err
	}

	type handler struct {
		Action  string
		Handler func(b []byte) error
	}

	handlers := []handler{
		{"tombstone_topic_producer", h.nsqadmin},
		{"create_topic", h.nsqadmin},
		{"create_channel", h.nsqadmin},
		{"delete_topic", h.nsqadmin},
		{"delete_channel", h.nsqadmin},
		{"pause_channel", h.nsqadmin},
		{"pause_topic", h.nsqadmin},
		{"unpause_channel", h.nsqadmin},
		{"unpause_topic", h.nsqadmin},
		{"empty_channel", h.nsqadmin},
		{"empty_topic", h.nsqadmin},
	}

	var errors bool
	for _, hh := range handlers {
		switch {
		case hh.Action == a.Action:
			fallthrough
		case hh.Action == "":
			err = hh.Handler(m.Body)
			if err != nil {
				log.Printf("%s", err)
				errors = true
			}
		}
	}
	if errors {
		return fmt.Errorf("failed handling")
	}
	return nil
}
