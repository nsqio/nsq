package nsqd

import (
	"github.com/nsqio/nsq/internal/lg"
)

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

type backendLogger struct {
	logf lg.AppLogFunc
}

func (b backendLogger) Output(calldepth int, s string) error {
	b.logf(lg.INFO, "%s", s)
	return nil
}
