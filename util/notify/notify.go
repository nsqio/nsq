// Package notify enables independent components of an application to 
// observe notable events in a decoupled fashion.
//
// It generalizes the pattern of *multiple* consumers of an event (ie: 
// a message over a single channel needing to be consumed by N consumers) 
// and obviates the need for components to have intimate knowledge of 
// each other (only `import notify` and the name of the event are shared).
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
	"errors"
	"sync"
)

// internal mapping of event names to observing channels
var events = make(map[string][]chan interface{})

// mutex for touching the event map
var rwMutex sync.RWMutex

// observe the specified event via provided output channel
func Observe(event string, outputChan chan interface{}) {
	rwMutex.Lock()
	defer rwMutex.Unlock()

	events[event] = append(events[event], outputChan)
}

// ignore the specified event on the provided output channel
func Ignore(event string, outputChan chan interface{}) error {
	rwMutex.Lock()
	defer rwMutex.Unlock()

	newArray := make([]chan interface{}, 0)
	if _, ok := events[event]; !ok {
		return errors.New("E_NOT_FOUND")
	}
	for _, ch := range events[event] {
		if ch != outputChan {
			newArray = append(newArray, ch)
		} else {
			close(ch)
		}
	}
	events[event] = newArray

	return nil
}

// post a notification (arbitrary data) to the specified event
func Post(event string, data interface{}) error {
	rwMutex.RLock()
	defer rwMutex.RUnlock()

	if _, ok := events[event]; !ok {
		return errors.New("E_NOT_FOUND")
	}
	for _, outputChan := range events[event] {
		outputChan <- data
	}

	return nil
}
