package main

import (
	"github.com/bitly/go-nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNsqdOptions()
	tcpAddr, _, nsqd := mustStartNSQd(options)
	defer nsqd.Exit()

	topicName := "test_stats" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, []byte("test body"))
	topic.PutMessage(msg)

	conn, err := mustConnectNSQd(tcpAddr)
	assert.Equal(t, err, nil)

	identify(t, conn)
	sub(t, conn, topicName, "ch")

	stats := nsqd.getStats()
	assert.Equal(t, len(stats), 1)
	assert.Equal(t, len(stats[0].Channels), 1)
	assert.Equal(t, len(stats[0].Channels[0].Clients), 1)
	log.Printf("stats: %+v", stats)
}
