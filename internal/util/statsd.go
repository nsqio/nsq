package util

import (
	"strings"
)

func StatsdHostKey(h string) string {
	return strings.Replace(strings.Replace(h, ".", "_", -1), ":", "_", -1)
}
