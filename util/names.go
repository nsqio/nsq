package util

import (
	"regexp"
)

var validTopicNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+$`)
var validChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

// IsValidTopicName checks a topic name for correctness
func IsValidTopicName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicNameRegex.MatchString(name)
}

// IsValidChannelName checks a channel name for correctness
func IsValidChannelName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validChannelNameRegex.MatchString(name)
}
