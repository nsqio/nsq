package nsq

type BackendQueue interface {
	Get() ([]byte, error)
	Put([]byte) error
	ReadReadyChan() chan int
	Close() error
}
