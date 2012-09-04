package main

import (
	"errors"
)

type DummyBackendQueue struct {
	readChan chan []byte
}

func NewDummyBackendQueue() BackendQueue {
	d := DummyBackendQueue{
		readChan: make(chan []byte),
	}
	return &d
}

func (d *DummyBackendQueue) Put([]byte) error {
	return errors.New("Dummy Backend")
}
func (d *DummyBackendQueue) ReadChan() chan []byte {
	return d.readChan
}
func (d *DummyBackendQueue) Close() error {
	return nil
}
func (d *DummyBackendQueue) Depth() int64 {
	return int64(0)
}
func (d *DummyBackendQueue) Empty() error {
	return nil
}
