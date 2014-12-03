// +build windows

package nsqd

import (
	"syscall"
	"unsafe"
)

var (
	modkernel32     = syscall.NewLazyDLL("kernel32.dll")
	procMoveFileExW = modkernel32.NewProc("MoveFileExW")
)

const (
	MOVEFILE_REPLACE_EXISTING = 1
)

func moveFileEx(source_file, target_file *uint16, flags uint32) error {
	ret, _, err := procMoveFileExW.Call(uintptr(unsafe.Pointer(source_file)), uintptr(unsafe.Pointer(target_file)), uintptr(flags))
	if ret == 0 {
		if err != nil {
			return err
		}
		return syscall.EINVAL
	}
	return nil
}

func atomic_rename(source_file, target_file string) error {
	lpReplacedFileName, err := syscall.UTF16PtrFromString(target_file)
	if err != nil {
		return err
	}

	lpReplacementFileName, err := syscall.UTF16PtrFromString(source_file)
	if err != nil {
		return err
	}

	return moveFileEx(lpReplacementFileName, lpReplacedFileName, MOVEFILE_REPLACE_EXISTING)
}
