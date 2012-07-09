package main

import (
	"../nsq"
	"log"
)

type Queue interface {
	MemoryChan() chan *nsq.Message
	BackendQueue() nsq.BackendQueue
	InFlight() map[string]*nsq.Message
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
	for {
		select {
		case msg := <-q.MemoryChan():
			err := WriteMessageToBackend(msg, q)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		default:
			goto inflight
		}
	}

inflight:
	inFlight := q.InFlight()
	if inFlight != nil {
		for _, msg := range inFlight {
			err := WriteMessageToBackend(msg, q)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		}
	}

	return nil
}

func WriteMessageToBackend(msg *nsq.Message, q Queue) error {
	data, err := msg.Encode()
	if err != nil {
		return err
	}
	err = q.BackendQueue().Put(data)
	if err != nil {
		return err
	}
	return nil
}
