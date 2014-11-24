package util

import (
	"errors"
)

var base10fail = errors.New("failed to convert to Base10")

func ByteToBase10(b []byte) (n uint64, err error) {
	base := uint64(10)

	n = 0
	for i := 0; i < len(b); i++ {
		var v byte
		d := b[i]
		switch {
		case '0' <= d && d <= '9':
			v = d - '0'
		default:
			n = 0
			err = base10fail
			return
		}
		n *= base
		n += uint64(v)
	}

	return n, err
}
