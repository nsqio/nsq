package statsd

import (
	"strings"
)

func HostKey(h string) string {
	return strings.Replace(strings.Replace(h, ".", "_", -1), ":", "_", -1)
}
