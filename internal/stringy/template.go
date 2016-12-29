package stringy

import (
	"fmt"
)

func NanoSecondToHuman(v float64) string {
	var suffix string
	switch {
	case v > 1000000000:
		v /= 1000000000
		suffix = "s"
	case v > 1000000:
		v /= 1000000
		suffix = "ms"
	case v > 1000:
		v /= 1000
		suffix = "us"
	default:
		suffix = "ns"
	}
	return fmt.Sprintf("%0.1f%s", v, suffix)
}
