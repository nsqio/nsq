package nsqd

type BackendQueueEnd interface {
}

type BackendOffset interface {
}

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

// for topic producer
type BackendQueueWriter interface {
	Put([]byte) (BackendQueueEnd, error)
	Close() error
	Delete() error
	Empty() error
	Flush() error
	GetQueueReadEnd() BackendQueueEnd
}

type ReadResult struct {
	offset BackendOffset
	data   []byte
	err    error
}

// for channel consumer
type BackendQueueReader interface {
	ReadChan() chan ReadResult
	ConfirmRead(BackendOffset) error
	Close() error
	Depth() int64
	Delete() error
	UpdateQueueEnd(BackendQueueEnd)
}
