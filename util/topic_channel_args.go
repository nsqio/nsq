package util

import (
	"errors"
)

type Getter interface {
	Get(key string) (string, error)
}

func GetTopicChannelArgs(rp Getter) (string, string, error) {
	topicName, err := rp.Get("topic")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	if !IsValidTopicName(topicName) {
		return "", "", errors.New("INVALID_ARG_TOPIC")
	}

	channelName, err := rp.Get("channel")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !IsValidChannelName(channelName) {
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}
