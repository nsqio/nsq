package util

import (
	"io"
)

type rwNopCloser struct {
	io.Reader
	io.Writer
}

func (rwNopCloser) Close() error {
	return nil
}

func ReadWriteNopCloser(rw io.ReadWriter) io.ReadWriteCloser {
	return rwNopCloser{rw, rw}
}

func ReadWriteNopClosePipe() io.ReadWriteCloser {
	pr, pw := io.Pipe()
	return rwNopCloser{pr, pw}
}
