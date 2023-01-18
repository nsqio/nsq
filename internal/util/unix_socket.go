package util

import (
	"net"
)

func TypeOfAddr(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return "tcp"
	}
	return "unix"
}
