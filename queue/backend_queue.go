package queue

type BackendQueue interface {
	Get() ([]byte, error)
	Put([]byte) error
	ReadReadyChan() chan int
	Close() error
}
