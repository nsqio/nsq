package nsqadmin

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

type MessageHandler struct {
	messageCount int
	maxMessages  int
	messages     []TopicMessage
	messageChan  chan int
}

type TopicMessage struct {
	MessageText string `json:"message_text"`
	MessageTime string `json:"message_time"`
}

func (h *MessageHandler) HandleMessage(m *nsq.Message) error {
	h.messageCount++
	newMessage := TopicMessage{
		MessageText: string(m.Body),
		MessageTime: time.Now().Format("03:04:05.9999"),
	}
	h.messages = append(h.messages, newMessage)
	if h.messageCount >= h.maxMessages {
		h.messageChan <- 1
	}
	return nil
}

func (s *httpServer) getLastMessages(topicName string) ([]TopicMessage, error) {
	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("nsqadmin/%s go-nsq/%s", version.Binary, nsq.VERSION)

	rand.Seed(time.Now().UnixNano())
	channel := fmt.Sprintf("nsqadmin%06d#ephemeral", rand.Int()%999999)

	consumer, err := nsq.NewConsumer(topicName, channel, cfg)
	if err != nil {
		return nil, err
	}

	messageHandler := &MessageHandler{maxMessages: s.ctx.nsqadmin.opts.MessageTailMaxCount}
	consumer.AddHandler(messageHandler)

	log.Printf("Connecting...")
	err = consumer.ConnectToNSQDs(s.ctx.nsqadmin.opts.NSQDHTTPAddresses)
	if err != nil {
		return nil, err
	}

	err = consumer.ConnectToNSQLookupds(s.ctx.nsqadmin.opts.NSQLookupdHTTPAddresses)
	if err != nil {
		return nil, err
	}

	messageHandler.messageChan = make(chan int)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		<-messageHandler.messageChan
		consumer.Stop()
		<-consumer.StopChan
		wg.Done()

	}()

	// put a time limit on how long we'll wait for messages, too
	go func() {

		time.Sleep(s.ctx.nsqadmin.opts.MessageTailMaxWait)
		messageHandler.messageChan <- 1
	}()

	wg.Wait()

	return messageHandler.messages, nil
}
