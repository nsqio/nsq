package main

type NSQueue interface {
	Get() ([]byte, error)
	Put([]byte) error
	ReadReadyChan() chan int
	Close() error
}
