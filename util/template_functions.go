package util

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
