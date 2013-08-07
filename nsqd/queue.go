package main

import (
	"bytes"
	"github.com/bitly/go-nsq"
)

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

type DummyBackendQueue struct {
	readChan chan []byte
}

func NewDummyBackendQueue() BackendQueue {
	return &DummyBackendQueue{readChan: make(chan []byte)}
}

func (d *DummyBackendQueue) Put([]byte) error {
	return nil
}

func (d *DummyBackendQueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *DummyBackendQueue) Close() error {
	return nil
}

func (d *DummyBackendQueue) Delete() error {
	return nil
}

func (d *DummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *DummyBackendQueue) Empty() error {
	return nil
}

func WriteMessageToBackend(buf *bytes.Buffer, msg *nsq.Message, bq BackendQueue) error {
	buf.Reset()
	err := msg.Write(buf)
	if err != nil {
		return err
	}
	err = bq.Put(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}
