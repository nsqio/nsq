package nsqd

type BackendOffset int64

type BackendQueueEnd interface {
	GetOffset() BackendOffset
	GetTotalMsgCnt() int64
	IsSame(BackendQueueEnd) bool
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
	Put([]byte) (BackendOffset, error)
	Close() error
	Delete() error
	Empty() error
	Flush() error
	GetQueueWriteEnd() BackendQueueEnd
	GetQueueReadEnd() BackendQueueEnd
	ResetWriteEnd(BackendOffset, uint64) error
}

type ReadResult struct {
	offset    BackendOffset
	movedSize BackendOffset
	data      []byte
	err       error
}

// for channel consumer
type BackendQueueReader interface {
	ReadChan() <-chan ReadResult
	ConfirmRead(BackendOffset) error
	Close() error
	Depth() int64
	Delete() error
	UpdateQueueEnd(BackendQueueEnd)
}
