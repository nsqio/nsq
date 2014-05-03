// +build windows

package nsqd

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32 = syscall.NewLazyDLL("kernel32.dll")

	procReplaceFile  = modkernel32.NewProc("ReplaceFile")
	procGetLastError = modkernel32.NewProc("GetLastError")
)

const (
	ERROR_SUCCESS                      = 0
	ERROR_UNABLE_TO_REMOVE_REPLACED    = 0x497
	ERROR_UNABLE_TO_MOVE_REPLACEMENT   = 0x498
	ERROR_UNABLE_TO_MOVE_REPLACEMENT_2 = 0x499

	// Ignore errors merging ACLs and attributes (how does rename(2) handle ACLs?)
	REPLACEFILE_IGNORE_MERGE_ERRORS = 0x00000002
)

func atomic_rename(source_file, target_file string) error {
	lpReplacedFileName, err := syscall.UTF16PtrFromString(target_file)
	if err != nil {
		return err
	}

	lpReplacementFileName, err := syscall.UTF16PtrFromString(source_file)
	if err != nil {
		return err
	}

	ret, _, _ := procReplaceFile.Call(
		uintptr(unsafe.Pointer(lpReplacedFileName)),
		uintptr(unsafe.Pointer(lpReplacementFileName)),
		uintptr(0),
		REPLACEFILE_IGNORE_MERGE_ERRORS,
		uintptr(0),
		uintptr(0))

	// ReplaceFile returns non-zero on success.
	if ret > 0 {
		return nil
	}

	// GetLastError returns 0 when the replaced file does not exist and ReplaceFile returns
	// 0 (false); it should have been UNABLE_TO_REMOVE_REPLACED.  Maybe this is a bug in the
	// Win32 Go code?
	if u := GetLastError(); u != uint32(0) {
		panic(errors.New(fmt.Sprintf("GetLastError: %d (is GetLastError working now?)", u)))
	}

	// If the target_file isn't present, just do a move (rename(2)).
	if _, err := os.Stat(target_file); os.IsNotExist(err) {
		return os.Rename(source_file, target_file)
	} else if err != nil {
		return err
	}

	// If the source_file isn't present, well, that's unrecoverable.
	if _, err = os.Stat(source_file); os.IsNotExist(err) {
		return errors.New("source file not found")
	} else if err != nil {
		return err
	}

	// I don't understand the circumstances that would lead to this branch,
	// unless GetLastError() is well and truly broken.
	panic(errors.New("it didn't work; hope you have backups!"))
}

func GetLastError() uint32 {
	ret, _, _ := procGetLastError.Call()
	return uint32(ret)
}
