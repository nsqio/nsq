// +build !windows

package util

import (
	"os"
)

func AtomicRename(sourceFile, targetFile string) error {
	return os.Rename(sourceFile, targetFile)
}
