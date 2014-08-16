package nsqd

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
