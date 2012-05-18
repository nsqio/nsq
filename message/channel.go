package message

import (
	"../queue"
	"../util"
	"errors"
	"log"
	"time"
)

type Channel struct {
	name                string
	addClientChan       chan util.ChanReq
	removeClientChan    chan util.ChanReq
	backend             queue.BackendQueue
	incomingMessageChan chan *Message
	msgChan             chan *Message
	inFlightMessageChan chan *Message
	requeueMessageChan  chan util.ChanReq
	finishMessageChan   chan util.ChanReq
	exitChan            chan int
	inFlightMessages    map[string]*Message
}

// Channel constructor
func NewChannel(channelName string, inMemSize int) *Channel {
	channel := &Channel{name: channelName,
		addClientChan:       make(chan util.ChanReq),
		removeClientChan:    make(chan util.ChanReq),
		backend:             queue.NewDiskQueue(channelName),
		incomingMessageChan: make(chan *Message, 5),
		msgChan:             make(chan *Message, inMemSize),
		inFlightMessageChan: make(chan *Message),
		requeueMessageChan:  make(chan util.ChanReq),
		finishMessageChan:   make(chan util.ChanReq),
		exitChan:            make(chan int),
		inFlightMessages:    make(map[string]*Message)}
	go channel.Router()
	return channel
}

// PutMessage writes to the appropriate incoming
// message channel
func (c *Channel) PutMessage(msg *Message) {
	c.incomingMessageChan <- msg
}

func (c *Channel) FinishMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.finishMessageChan <- util.ChanReq{uuidStr, errChan}
	err, _ := (<-errChan).(error)
	return err
}

func (c *Channel) RequeueMessage(uuidStr string) error {
	errChan := make(chan interface{})
	c.requeueMessageChan <- util.ChanReq{uuidStr, errChan}
	err, _ := (<-errChan).(error)
	return err
}

// Router handles the muxing of Channel messages including
// the addition of a Client to the Channel
func (c *Channel) Router() {
	helperCloseChan := make(chan int)

	go func() {
		for {
			select {
			case msg := <-c.inFlightMessageChan:
				c.pushInFlightMessage(msg)
				go func(msg *Message) {
					select {
					case <-time.After(60 * time.Second):
						log.Printf("CHANNEL(%s): auto requeue of message(%s)", c.name, util.UuidToStr(msg.Uuid()))
					case <-msg.timerChan:
						return
					}
					err := c.RequeueMessage(util.UuidToStr(msg.Uuid()))
					if err != nil {
						log.Printf("ERROR: channel(%s) - %s", c.name, err.Error())
					}
				}(msg)
			case requeueReq := <-c.requeueMessageChan:
				uuidStr := requeueReq.Variable.(string)
				msg, err := c.popInFlightMessage(uuidStr)
				if err != nil {
					log.Printf("ERROR: failed to requeue message(%s) - %s", uuidStr, err.Error())
				} else {
					go func(msg *Message) {
						c.PutMessage(msg)
					}(msg)
				}
				requeueReq.RetChan <- err
			case finishReq := <-c.finishMessageChan:
				uuidStr := finishReq.Variable.(string)
				_, err := c.popInFlightMessage(uuidStr)
				if err != nil {
					log.Printf("ERROR: failed to finish message(%s) - %s", uuidStr, err.Error())
				}
				finishReq.RetChan <- err
			case <-helperCloseChan:
				return
			}
		}
	}()

	for {
		select {
		case msg := <-c.incomingMessageChan:
			select {
			case c.msgChan <- msg:
				log.Printf("CHANNEL(%s): wrote to msgChan", c.name)
			default:
				err := c.backend.Put(msg.Data)
				if err != nil {
					log.Printf("ERROR: t.backend.Put() - %s", err.Error())
					// TODO: requeue?
				}
				log.Printf("CHANNEL(%s): wrote to backend", c.name)
			}
		case <-c.exitChan:
			helperCloseChan <- 1
			return
		}
	}
}

func (c *Channel) pushInFlightMessage(msg *Message) {
	uuidStr := util.UuidToStr(msg.Uuid())
	c.inFlightMessages[uuidStr] = msg
}

func (c *Channel) popInFlightMessage(uuidStr string) (*Message, error) {
	msg, ok := c.inFlightMessages[uuidStr]
	if !ok {
		return nil, errors.New("UUID not in flight")
	}
	delete(c.inFlightMessages, uuidStr)
	msg.EndTimer()
	return msg, nil
}

// GetMessage pulls a single message off the client channel
func (c *Channel) GetMessage(block bool) *Message {
	var msg *Message

	for {
		if block {
			select {
			case msg = <-c.msgChan:
			case <-c.backend.ReadReadyChan():
				buf, err := c.backend.Get()
				if err != nil {
					log.Printf("ERROR: c.backend.Get() - %s", err.Error())
					continue
				}
				msg = NewMessage(buf)
			}
		} else {
			select {
			case msg = <-c.msgChan:
			case <-c.backend.ReadReadyChan():
				buf, err := c.backend.Get()
				if err != nil {
					log.Printf("ERROR: c.backend.Get() - %s", err.Error())
					continue
				}
				msg = NewMessage(buf)
			default:
				msg = nil
			}
		}

		if msg != nil {
			c.inFlightMessageChan <- msg
		}

		break
	}

	return msg
}

func (c *Channel) Close() error {
	var err error

	log.Printf("CHANNEL(%s): closing", c.name)

	c.exitChan <- 1

	err = c.backend.Close()
	if err != nil {
		return err
	}

	return nil
}
