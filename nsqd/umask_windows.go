//go:build windows

package nsqd

import "errors"

// checkUnixSocketSupport returns an error because --unix-socket-path relies on
// filesystem permissions (umask/chmod) for access control, which is not
// supported on windows.
func checkUnixSocketSupport() error {
	return errors.New("--unix-socket-path is not supported on windows")
}

// setUmask is a no-op on windows, which has no process umask.
func setUmask(mask int) int {
	return 0
}
