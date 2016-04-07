package http_api

import (
	"errors"

	"github.com/absolute8511/nsq/internal/protocol"
	"strconv"
)

type getter interface {
	Get(key string) string
}

func GetTopicArg(rp getter) (string, error) {
	topicName := rp.Get("topic")
	if topicName == "" {
		return "", errors.New("MISSING_ARG_TOPIC")
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", errors.New("INVALID_ARG_TOPIC")
	}

	return topicName, nil
}

func GetTopicChannelArgs(rp getter) (string, string, error) {
	topicName, err := GetTopicArg(rp)
	if err != nil {
		return "", "", err
	}

	channelName := rp.Get("channel")
	if channelName == "" {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !protocol.IsValidChannelName(channelName) {
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}

// partition can be missing, default as 0.
func GetTopicPartitionArgs(rp getter) (string, int, error) {
	topicName, err := GetTopicArg(rp)
	if err != nil {
		return "", 0, err
	}

	topicPartStr := rp.Get("partition")
	topicPart := 0
	if topicPartStr == "" {
		topicPart = 0
	} else {
		var err error
		topicPart, err = strconv.Atoi(topicPartStr)
		if err != nil {
			return "", 0, err
		}
	}
	return topicName, topicPart, nil
}

func GetTopicPartitionChannelArgs(rp getter) (string, int, string, error) {
	topicName, topicPart, err := GetTopicPartitionArgs(rp)
	if err != nil {
		return "", 0, "", err
	}
	channelName := rp.Get("channel")
	if channelName == "" {
		return "", 0, "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !protocol.IsValidChannelName(channelName) {
		return "", 0, "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, topicPart, channelName, nil
}
