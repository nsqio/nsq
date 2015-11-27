package nsqd

type BackendQueueEnd interface {
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
}

type ReadResult struct {
	data []byte
	err  error
}

// for channel consumer
type BackendQueueReader interface {
	ReadChan() chan ReadResult
	Close() error
	Depth() int64
	UpdateQueueEnd(BackendQueueEnd)
}
