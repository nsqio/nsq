package util

import (
	"fmt"
	"runtime"
)

const BinaryVersion = "0.3.3-alpha"

func Version(app string) string {
	return fmt.Sprintf("%s v%s (built w/%s)", app, BinaryVersion, runtime.Version())
}
