package main

import (
	"../nsq"
	"../util/pqueue"
	"log"
)

type Queue interface {
	MemoryChan() chan *nsq.Message
	BackendQueue() nsq.BackendQueue
	InFlight() map[string]interface{}
	Deferred() map[string]interface{}
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
			goto finish
		}
	}

finish:
	inFlight := q.InFlight()
	if inFlight != nil {
		for _, item := range inFlight {
			msg := item.(*pqueue.Item).Value.(*inFlightMessage).msg
			err := WriteMessageToBackend(msg, q)
			if err != nil {
				log.Printf("ERROR: failed to write message to backend - %s", err.Error())
			}
		}
	}

	deferred := q.Deferred()
	if deferred != nil {
		for _, item := range deferred {
			msg := item.(*pqueue.Item).Value.(*nsq.Message)
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
