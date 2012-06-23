package nsq

type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte
	Close() error
	Depth() int64
}
