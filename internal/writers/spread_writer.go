package writers

import (
	"io"
	"time"
)

type SpreadWriter struct {
	w        io.Writer
	interval time.Duration
	buf      [][]byte
}

func NewSpreadWriter(w io.Writer, interval time.Duration) *SpreadWriter {
	return &SpreadWriter{
		w:        w,
		interval: interval,
		buf:      make([][]byte, 0),
	}
}

func (s *SpreadWriter) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	s.buf = append(s.buf, b)
	return len(p), nil
}

func (s *SpreadWriter) Flush() {
	sleep := s.interval / time.Duration(len(s.buf))
	for _, b := range s.buf {
		start := time.Now()
		s.w.Write(b)
		latency := time.Now().Sub(start)
		time.Sleep(sleep - latency)
	}
	s.buf = s.buf[:0]
}
