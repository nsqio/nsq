package main

import (
	"../nsq"
	"../util/notify"
	"log"
	"net"
	"time"
)

var notifyChannelChan = make(chan interface{})
var notifyTopicChan = make(chan interface{})
var lookupPeers = make([]*nsq.LookupPeer, 0)

func LookupRouter(lookupHosts []string) {
	for _, host := range lookupHosts {
		tcpAddr, err := net.ResolveTCPAddr("tcp", host)
		if err != nil {
			log.Fatal("LOOKUP: could not resolve TCP address for %s", host)
		}
		lookupPeer := nsq.NewLookupPeer(tcpAddr)
		lookupPeers = append(lookupPeers, lookupPeer)
	}

	notify.Observe("new_channel", notifyChannelChan)
	notify.Observe("new_topic", notifyTopicChan)

	for {
		select {
		case <-time.After(10 * time.Second):
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				log.Printf("LOOKUP: sending heartbeat to %s", lookupPeer.String())
				if !lookupPeer.IsConnected() {
					err := lookupPeer.Connect()
					if err != nil {
						log.Printf("LOOKUP: failed to connect to %s", lookupPeer.String())
						continue
					}
					lookupPeer.Version(nsq.LookupProtocolV1Magic)
				}
				err := lookupPeer.WriteCommand(lookupPeer.Ping())
				if err != nil {
					lookupPeer.Close()
					continue
				}
				_, err = lookupPeer.ReadResponse()
				if err != nil {
					lookupPeer.Close()
					continue
				}
			}
		case newChannel := <-notifyChannelChan:
			channel := newChannel.(*Channel)
			log.Printf("LOOKUP: new channel %s", channel.name)
		case newTopic := <-notifyTopicChan:
			topic := newTopic.(*Topic)
			log.Printf("LOOKUP: new topic %s", topic.name)
			for _, lookupPeer := range lookupPeers {
				if !lookupPeer.IsConnected() {
					err := lookupPeer.Connect()
					if err != nil {
						log.Printf("LOOKUP: failed to connect to %s", lookupPeer.String())
						continue
					}
					lookupPeer.Version(nsq.LookupProtocolV1Magic)
				}
				err := lookupPeer.WriteCommand(lookupPeer.Announce(topic.name, *bindAddress, *tcpPort))
				if err != nil {
					log.Printf("LOOKUP: error announcing to %s... closing", lookupPeer.String())
					lookupPeer.Close()
					continue
				}
				_, err = lookupPeer.ReadResponse()
				if err != nil {
					lookupPeer.Close()
					continue
				}
			}
		}
	}
}
