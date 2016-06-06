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
	Put([]byte) (BackendOffset, int32, int64, error)
	Close() error
	Delete() error
	Empty() error
	Flush() error
	GetQueueWriteEnd() BackendQueueEnd
	GetQueueReadEnd() BackendQueueEnd
	RollbackWrite(BackendOffset, uint64) error
	ResetWriteEnd(BackendOffset, int64) error
}

type ReadResult struct {
	Offset    BackendOffset
	MovedSize BackendOffset
	Data      []byte
	Err       error
}

// for channel consumer
type BackendQueueReader interface {
	ReadChan() <-chan ReadResult
	ConfirmRead(BackendOffset) error
	ResetReadToConfirmed() (BackendOffset, error)
	SkipReadToOffset(BackendOffset) (BackendOffset, error)
	Close() error
	// left data to be read
	Depth() int64
	GetQueueReadEnd() BackendQueueEnd
	Delete() error
	UpdateQueueEnd(BackendQueueEnd) error
}
