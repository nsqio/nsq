// Package notify enables independent components of an application to 
// observe notable events in a decoupled fashion.
//
// It generalizes the pattern of *multiple* consumers of an event (ie: 
// a message over a single channel needing to be consumed by N consumers) 
// while obviating the need for components to have intimate knowledge of 
// each other (only `import notify` and the name of the event is required).
//
// The internal goroutines are started lazily, no initialization is required.
//
// Example:
//     // producer of "my_event" 
//     for {
//         select {
//         case <-time.Tick(time.Duration(1) * time.Second):
//             notify.Post("my_event", time.Now().Unix())
//         }
//     }
//     
//     // observer of "my_event" (normally some independent component that
//     // needs to be notified)
//     myEventChan := make(chan interface{})
//     notify.Observe("my_event", myEventChan)
//     go func() {
//         for {
//             data := <-myEventChan
//             log.Printf("MY_EVENT: %#v", data)
//         }
//     }()
package notify

import (
	"../../util"
	"log"
	"sync/atomic"
)

// internal helper type to pass more data through ChanReq
type postOp struct {
	event string
	data  interface{}
}

// internal mapping of event names to observing channels
var events = make(map[string][]chan interface{})

// internal channel to add an observer
var observeChan = make(chan util.ChanReq)

// internal channel to remove an observer
var ignoreChan = make(chan util.ChanReq)

// internal channel to post a notification
var postNotificationChan = make(chan util.ChanReq)

// internal bool to determine whether or not the goroutine has
// been started
var routerStarted = int32(0)

// observe the specified event via provided output channel
func Observe(event string, outputChan chan interface{}) {
	startRouter()
	addReq := util.ChanReq{event, outputChan}
	observeChan <- addReq
}

// ignore the specified event on the provided output channel
func Ignore(event string, outputChan chan interface{}) {
	startRouter()
	removeReq := util.ChanReq{event, outputChan}
	ignoreChan <- removeReq
}

// post a notification (arbitrary data) to the specified event
func Post(event string, data interface{}) {
	startRouter()
	postOp := postOp{event, data}
	postReq := util.ChanReq{postOp, nil}
	postNotificationChan <- postReq
}

// internal helper function to start the message routing goroutine
func startRouter() {
	if atomic.CompareAndSwapInt32(&routerStarted, 0, 1) {
		go notificationRouter()
	}
}

// internal function executed in a goroutine to select
// over the relevant channels, perform state
// mutations, and post notifications
func notificationRouter() {
	for {
		select {
		case addObserverReq := <-observeChan:
			event := addObserverReq.Variable.(string)
			outputChan := addObserverReq.RetChan
			events[event] = append(events[event], outputChan)
		case postNotificationReq := <-postNotificationChan:
			postOp := postNotificationReq.Variable.(postOp)
			event := postOp.event
			data := postOp.data
			if _, ok := events[event]; !ok {
				log.Printf("NOTIFY: %s is not a valid event", event)
				continue
			}
			for _, outputChan := range events[event] {
				go func(event string, c chan interface{}, d interface{}) {
					c <- d
				}(event, outputChan, data)
			}
		case removeObserverReq := <-ignoreChan:
			event := removeObserverReq.Variable.(string)
			removeChan := removeObserverReq.RetChan
			newArray := make([]chan interface{}, 0)
			if _, ok := events[event]; !ok {
				log.Printf("NOTIFY: %s is not a valid event", event)
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
