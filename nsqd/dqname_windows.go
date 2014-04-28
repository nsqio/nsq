// +build windows

package nsqd

// On Windows, file names cannot contain colons.
func getBackendName(topicName, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>__<channel>
	backendName := topicName + "__" + channelName
	return backendName
}
