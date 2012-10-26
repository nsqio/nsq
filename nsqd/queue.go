package main

import (
	"../nsq"
	"../util/pqueue"
	"bytes"
	"log"
)

// BackendQueue represents the behavior for the secondary message
// storage system
type BackendQueue interface {
	Put([]byte) error
	ReadChan() chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Depth() int64
	Empty() error
}

type Queue interface {
	MemoryChan() chan *nsq.Message
	BackendQueue() BackendQueue
	InFlight() map[string]*pqueue.Item
	Deferred() map[string]*pqueue.Item
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

func (d *DummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *DummyBackendQueue) Empty() error {
	return nil
}

func EmptyQueue(q Queue) error {
	for {
		select {
		case <-q.MemoryChan():
		default:
			goto disk
		}
	}

disk:
	return q.BackendQueue().Empty()
}

func FlushQueue(q Queue) error {
	var msgBuf bytes.Buffer

	for {
		select {
		case msg := <-q.MemoryChan():
			err := WriteMessageToBackend(&msgBuf, msg, q)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		default:
			goto finish
		}
	}

finish:
	for _, item := range q.InFlight() {
		msg := item.Value.(*inFlightMessage).msg
		err := WriteMessageToBackend(&msgBuf, msg, q)
		if err != nil {
			log.Printf("ERROR: failed to write message to backend - %s", err.Error())
		}
	}

	for _, item := range q.Deferred() {
		msg := item.Value.(*nsq.Message)
		err := WriteMessageToBackend(&msgBuf, msg, q)
		if err != nil {
			log.Printf("ERROR: failed to write message to backend - %s", err.Error())
		}
	}

	return nil
}

func WriteMessageToBackend(buf *bytes.Buffer, msg *nsq.Message, q Queue) error {
	buf.Reset()
	err := msg.Encode(buf)
	if err != nil {
		return err
	}
	err = q.BackendQueue().Put(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}
