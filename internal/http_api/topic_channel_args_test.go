package http_api

import (
    "errors"
    "testing"
)


// Test generated using Keploy
type MockGetter struct {
    values map[string]string
    errors map[string]error
}

func (m *MockGetter) Get(key string) (string, error) {
    if err, ok := m.errors[key]; ok {
        return "", err
    }
    if val, ok := m.values[key]; ok {
        return val, nil
    }
    return "", errors.New("key not found")
}

func TestGetTopicChannelArgs_MissingTopic(t *testing.T) {
    mockGetter := &MockGetter{
        errors: map[string]error{
            "topic": errors.New("missing"),
        },
    }

    topic, channel, err := GetTopicChannelArgs(mockGetter)
    if topic != "" || channel != "" || err == nil || err.Error() != "MISSING_ARG_TOPIC" {
        t.Errorf("Expected error 'MISSING_ARG_TOPIC', got topic: '%v', channel: '%v', err: %v", topic, channel, err)
    }
}

// Test generated using Keploy
func TestGetTopicChannelArgs_InvalidTopic(t *testing.T) {
    mockGetter := &MockGetter{
        values: map[string]string{
            "topic": "invalid_topic!", // invalid topic name
        },
    }

    topic, channel, err := GetTopicChannelArgs(mockGetter)
    if topic != "" || channel != "" || err == nil || err.Error() != "INVALID_ARG_TOPIC" {
        t.Errorf("Expected error 'INVALID_ARG_TOPIC', got topic: '%v', channel: '%v', err: %v", topic, channel, err)
    }
}


// Test generated using Keploy
func TestGetTopicChannelArgs_MissingChannel(t *testing.T) {
    mockGetter := &MockGetter{
        values: map[string]string{
            "topic": "valid_topic",
        },
        errors: map[string]error{
            "channel": errors.New("missing"),
        },
    }

    topic, channel, err := GetTopicChannelArgs(mockGetter)
    if topic != "" || channel != "" || err == nil || err.Error() != "MISSING_ARG_CHANNEL" {
        t.Errorf("Expected error 'MISSING_ARG_CHANNEL', got topic: '%v', channel: '%v', err: %v", topic, channel, err)
    }
}


// Test generated using Keploy
func TestGetTopicChannelArgs_InvalidChannel(t *testing.T) {
    mockGetter := &MockGetter{
        values: map[string]string{
            "topic":   "valid_topic",
            "channel": "invalid_channel#", // invalid channel name
        },
    }

    topic, channel, err := GetTopicChannelArgs(mockGetter)
    if topic != "" || channel != "" || err == nil || err.Error() != "INVALID_ARG_CHANNEL" {
        t.Errorf("Expected error 'INVALID_ARG_CHANNEL', got topic: '%v', channel: '%v', err: %v", topic, channel, err)
    }
}


// Test generated using Keploy
func TestGetTopicChannelArgs_ValidArgs(t *testing.T) {
    mockGetter := &MockGetter{
        values: map[string]string{
            "topic":   "valid_topic",
            "channel": "valid_channel",
        },
    }

    topic, channel, err := GetTopicChannelArgs(mockGetter)
    if topic != "valid_topic" || channel != "valid_channel" || err != nil {
        t.Errorf("Expected topic: 'valid_topic', channel: 'valid_channel', got topic: '%v', channel: '%v', err: %v", topic, channel, err)
    }
}

