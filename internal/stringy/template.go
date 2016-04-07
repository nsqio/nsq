package stringy

import (
	"fmt"
)

func Commafy(i interface{}) string {
	var n int64
	switch i.(type) {
	case int:
		n = int64(i.(int))
	case int64:
		n = i.(int64)
	case int32:
		n = int64(i.(int32))
	}
	if n > 1000 {
		r := n % 1000
		n = n / 1000
		return fmt.Sprintf("%s,%03d", Commafy(n), r)
	}
	return fmt.Sprintf("%d", n)
}

func FloatToPercent(i float64) string {
	return fmt.Sprintf("%.0f", i*100.0)
}

func PercSuffix(i float64) string {
	switch int(i*100) % 10 {
	case 1:
		return "st"
	case 2:
		return "nd"
	case 3:
		return "rd"
	}
	return "th"
}

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
