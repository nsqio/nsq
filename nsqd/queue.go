package main

import (
	"../nsq"
	"log"
)

type Queue interface {
	MemoryChan() chan *nsq.Message
	BackendQueue() nsq.BackendQueue
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
			data, err := msg.Encode()
			if err != nil {
				log.Printf("ERROR: failed to Encode() message - %s", err.Error())
				continue
			}
			err = q.BackendQueue().Put(data)
			if err != nil {
				log.Printf("ERROR: BackendQueue().Put() - %s", err.Error())
			}
		default:
			return nil
		}
	}
	return nil
}
