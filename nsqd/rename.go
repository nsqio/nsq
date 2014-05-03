// +build !windows

package nsqd

import (
	"os"
)

func atomic_rename(source_file, target_file string) error {
	return os.Rename(source_file, target_file)
}
