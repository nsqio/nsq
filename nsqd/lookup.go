package main

import (
	"../nsq"
	"../util/notify"
	"log"
	"net"
	"strconv"
)

var notifyChannelChan = make(chan interface{})
var notifyTopicChan = make(chan interface{})
var lookupPeers = make([]*nsq.LookupPeer, 0)

func LookupConnect(lookupHosts []string) error {
	for _, host := range lookupHosts {
		lookupPeer := nsq.NewLookupPeer()
		tcpAddr, err := net.ResolveTCPAddr("tcp", host)
		if err != nil {
			return err
		}
		err = lookupPeer.Connect(tcpAddr)
		if err != nil {
			log.Printf("ERROR: failed to connect to lookup host %s", host)
			continue
		}
		lookupPeer.Version(nsq.LookupProtocolV1Magic)
		lookupPeers = append(lookupPeers, lookupPeer)
	}

	go lookupRouter()
	notify.Observe("new_channel", notifyChannelChan)
	notify.Observe("new_topic", notifyTopicChan)

	return nil
}

func lookupRouter() {
	for {
		select {
		case newChannel := <-notifyChannelChan:
			channel := newChannel.(*Channel)
			log.Printf("LOOKUP: new channel %#v", channel)
		case newTopic := <-notifyTopicChan:
			topic := newTopic.(*Topic)
			log.Printf("LOOKUP: new topic %#v", topic)
			for _, lookupPeer := range lookupPeers {
				lookupPeer.WriteCommand(lookupPeer.Announce(topic.name, *bindAddress, strconv.Itoa(*tcpPort)))
			}
		}
	}
}
