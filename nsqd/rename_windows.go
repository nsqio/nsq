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

func moveFileEx(sourceFile, targetFile *uint16, flags uint32) error {
	ret, _, err := procMoveFileExW.Call(uintptr(unsafe.Pointer(sourceFile)), uintptr(unsafe.Pointer(targetFile)), uintptr(flags))
	if ret == 0 {
		if err != nil {
			return err
		}
		return syscall.EINVAL
	}
	return nil
}

func atomicRename(sourceFile, targetFile string) error {
	lpReplacedFileName, err := syscall.UTF16PtrFromString(targetFile)
	if err != nil {
		return err
	}

	lpReplacementFileName, err := syscall.UTF16PtrFromString(sourceFile)
	if err != nil {
		return err
	}

	return moveFileEx(lpReplacementFileName, lpReplacedFileName, MOVEFILE_REPLACE_EXISTING)
}
