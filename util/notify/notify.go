package notify

import (
	"../../util"
	"log"
	"sync/atomic"
)

type postOp struct {
	event string
	data  interface{}
}

var events = make(map[string][]chan interface{})
var addObserverChan = make(chan util.ChanReq)
var removeObserverChan = make(chan util.ChanReq)
var postNotificationChan = make(chan util.ChanReq)
var routerStarted = int32(0)

func Observe(event string, outputChan chan interface{}) {
	startRouter()
	addReq := util.ChanReq{event, outputChan}
	addObserverChan <- addReq
}

func StopObserving(event string, removeChan chan interface{}) {
	startRouter()
	removeReq := util.ChanReq{event, removeChan}
	removeObserverChan <- removeReq
}

func Post(event string, data interface{}) {
	startRouter()
	postOp := postOp{event, data}
	postReq := util.ChanReq{postOp, nil}
	postNotificationChan <- postReq
}

func startRouter() {
	if atomic.CompareAndSwapInt32(&routerStarted, 0, 1) {
		go notificationRouter()
	}
}

func notificationRouter() {
	for {
		select {
		case addObserverReq := <-addObserverChan:
			event := addObserverReq.Variable.(string)
			outputChan := addObserverReq.RetChan
			events[event] = append(events[event], outputChan)
		case postNotificationReq := <-postNotificationChan:
			postOp := postNotificationReq.Variable.(postOp)
			event := postOp.event
			data := postOp.data
			if _, ok := events[event]; !ok {
				log.Printf("NOTIFICATION: %s is not a valid event", event)
				continue
			}
			for _, outputChan := range events[event] {
				go func(event string, c chan interface{}, d interface{}) {
					c <- d
				}(event, outputChan, data)
			}
		case removeObserverReq := <-removeObserverChan:
			event := removeObserverReq.Variable.(string)
			removeChan := removeObserverReq.RetChan
			newArray := make([]chan interface{}, 0)
			if _, ok := events[event]; !ok {
				log.Printf("NOTIFICATION: %s is not a valid event", event)
				continue
			}
			for _, outputChan := range events[event] {
				if outputChan != removeChan {
					newArray = append(newArray, outputChan)
				} else {
					close(outputChan)
				}
			}
			events[event] = newArray
		}
	}
}
