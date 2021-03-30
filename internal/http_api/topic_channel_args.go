package http_api

import (
	"errors"

	"github.com/nsqio/nsq/internal/protocol"
)

type getter interface {
	Get(key string) (string, error)
}

// GetTopicArg returns the ?topic parameter
func GetTopicArg(rp getter) (string, error) {
	topicName, err := rp.Get("topic")
	if err != nil {
		return "", errors.New("MISSING_ARG_TOPIC")
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", errors.New("INVALID_ARG_TOPIC")
	}
	return topicName, nil
}

// GetChannelArg returns the ?channel parameter
func GetChannelArg(rp getter) (string, error) {
	channelName, err := rp.Get("channel")
	if err != nil {
		return "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !protocol.IsValidChannelName(channelName) {
		return "", errors.New("INVALID_ARG_CHANNEL")
	}
	return channelName, nil
}

func GetTopicChannelArgs(rp getter) (string, string, error) {
	topicName, err := GetTopicArg(rp)
	if err != nil {
		return "", "", err
	}
	channelName, err := GetChannelArg(rp)
	if err != nil {
		return "", "", err
	}
	return topicName, channelName, nil
}
