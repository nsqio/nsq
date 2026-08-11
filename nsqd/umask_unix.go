//go:build !windows

package nsqd

import "syscall"

// checkUnixSocketSupport returns nil on platforms where --unix-socket-path
// is supported.
func checkUnixSocketSupport() error {
	return nil
}

// setUmask sets the process umask and returns the previous value.
func setUmask(mask int) int {
	return syscall.Umask(mask)
}
