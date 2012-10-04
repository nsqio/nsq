package main

import (
	"../nsq"
	"../util/pqueue"
	"bytes"
	"log"
)

type Queue interface {
	MemoryChan() chan *nsq.Message
	BackendQueue() BackendQueue
	InFlight() map[string]*pqueue.Item
	Deferred() map[string]*pqueue.Item
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
