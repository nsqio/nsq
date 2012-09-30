package main

type DummyBackendQueue struct {
	readChan chan []byte
}

func NewDummyBackendQueue() BackendQueue           { return &DummyBackendQueue{readChan: make(chan []byte)} }
func (d *DummyBackendQueue) Put([]byte) error      { return nil }
func (d *DummyBackendQueue) ReadChan() chan []byte { return d.readChan }
func (d *DummyBackendQueue) Close() error          { return nil }
func (d *DummyBackendQueue) Depth() int64          { return int64(0) }
func (d *DummyBackendQueue) Empty() error          { return nil }
