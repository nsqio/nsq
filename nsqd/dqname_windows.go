// +build windows

package nsqd

// On Windows, file names cannot contain colons.
func getBackendReaderName(topicName string, part int, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>;<channel>
	backendName := GetTopicFullName(topicName, part) + ";" + channelName
	return backendName
}

func getBackendName(topicName string, part int) string {
	backendName := GetTopicFullName(topicName, part)
	return backendName
}
