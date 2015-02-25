// +build !windows

package nsqd

import (
	"os"
)

func atomicRename(sourceFile, targetFile string) error {
	return os.Rename(sourceFile, targetFile)
}
