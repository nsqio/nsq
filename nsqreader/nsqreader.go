package nsqreader

import (
	"../nsq"
	"../util"
	"bitly/simplejson"
	"log"
	"net"
)

type NSQReader interface {
	HandleMessage(*simplejson.Json) error
}

type Queue struct {
	TopicName        string       // Name of Topic to subscribe to
	ChannelName      string       // Named Channel to consume messages from
	ServerAddress    *net.TCPAddr // TODO: switch to lookup hosts
	BatchSize        int          // number of messages to request at a time
	IncomingMessages chan *nsq.Message
	FinishedMessages chan *FinishedMessage

	runFlag          bool
	runningHandlers  int
	messagesInFlight int
	consumer         *nsq.Consumer
}

type FinishedMessage struct {
	uuid    []byte
	success bool
}

func NewNSQReader(topic string, channel string, server *net.TCPAddr) (*Queue, error) {
	q := &Queue{
		TopicName:        topic,
		ChannelName:      channel,
		ServerAddress:    server,
		BatchSize:        1,
		IncomingMessages: make(chan *nsq.Message),
		FinishedMessages: make(chan *FinishedMessage),
		runFlag:          true,
		runningHandlers:  0,
		consumer:         nil,
	}
	return q, nil
}

func (q *Queue) Run() {
	log.Println("starting queue run")
	if q.runningHandlers == 0 {
		log.Fatal("there are no handlers running")
	}
	go q.FinishLoop()

	q.consumer = nsq.NewConsumer(q.ServerAddress)
	err := q.consumer.Connect()
	if err != nil {
		log.Fatal(err)
	}
	q.consumer.Version(nsq.ProtocolV2Magic)
	q.consumer.WriteCommand(q.consumer.Subscribe(q.TopicName, q.ChannelName))
	q.messagesInFlight = q.BatchSize
	q.consumer.WriteCommand(q.consumer.Ready(q.messagesInFlight))
	for {
		if !q.runFlag {
			close(q.IncomingMessages) // stops the handlers. will chain to requeue loop
			break
		}

		resp, err := q.consumer.ReadResponse()
		if err != nil {
			log.Fatal(err)
		}

		frameType, data, err := q.consumer.UnpackResponse(resp)
		if err != nil {
			log.Fatal(err)
		}
		switch frameType {
		case nsq.FrameTypeMessage:
			msg := data.(*nsq.Message)
			log.Printf("%s - %s", util.UuidToStr(msg.Uuid), msg.Body)
			q.messagesInFlight -= 1
			q.IncomingMessages <- msg
		default:
			log.Println("unknown message type", frameType)
			// note: a bunch of messages in this state could take us out of 'ready'
		}

	}
}

// stops a QueueReader gracefully
func (q *Queue) Stop() {
	q.runFlag = false // kicks out the GET loop
}

// this starts a handler on the queue
// it's ok to start more than one handler simultaneously
func (q *Queue) AddHandler(handler NSQReader) {
	q.runningHandlers += 1
	log.Println("starting handle go-routine")
	go func() {
		for {
			msg, ok := <-q.IncomingMessages
			if !ok {
				q.runningHandlers -= 1
				if q.runningHandlers == 0 {
					close(q.FinishedMessages)
				}
				break
			}

			data, err := simplejson.NewJson(msg.Body)
			if err != nil {
				log.Println(err)
				q.FinishedMessages <- &FinishedMessage{msg.Uuid, false}
				continue
			}

			_, ok = data.CheckGet("__heartbeat__")
			if ok {
				// OK this message
				q.FinishedMessages <- &FinishedMessage{msg.Uuid, true}
				continue
			}

			// log.Println("got IncomingMessages", msg)
			err = handler.HandleMessage(data)
			if err != nil {
				q.FinishedMessages <- &FinishedMessage{msg.Uuid, false}
			} else {
				q.FinishedMessages <- &FinishedMessage{msg.Uuid, true}
			}
		}
	}()
}

// read q.FinishedMessages and act accordingly
func (q *Queue) FinishLoop() {
	for {
		msg, ok := <-q.FinishedMessages
		if !ok {
			break
		}
		if msg.success {
			log.Println("successfully finished", msg)
			q.consumer.WriteCommand(q.consumer.Finish(util.UuidToStr(msg.uuid)))
		} else {
			log.Println("failed message", msg)
			q.consumer.WriteCommand(q.consumer.Requeue(util.UuidToStr(msg.uuid)))
		}
		q.messagesInFlight += 1
		// TODO: don't write this every time (for when we have batch requesting enabled)
		q.consumer.WriteCommand(q.consumer.Ready(q.messagesInFlight))
	}
}
