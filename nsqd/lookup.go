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
				lookupCommand(lookupPeer, lookupPeer.Ping())
			}
		case newChannel := <-notifyChannelChan:
			channel := newChannel.(*Channel)
			log.Printf("LOOKUP: new channel %s", channel.name)
			// TODO: notify all nsds that a new channel exists
		case newTopic := <-notifyTopicChan:
			// notify all nsds that a new topic exists
			topic := newTopic.(*Topic)
			log.Printf("LOOKUP: new topic %s", topic.name)
			for _, lookupPeer := range lookupPeers {
				lookupCommand(lookupPeer, lookupPeer.Announce(topic.name, *bindAddress, *tcpPort))
			}
		}
	}
}

func lookupCommand(peer *nsq.LookupPeer, cmd *nsq.ProtocolCommand) ([]byte, error) {
	if !peer.IsConnected() {
		err := peer.Connect()
		if err != nil {
			log.Printf("LOOKUP: failed to connect to %s", peer.String())
			return nil, err
		}
		peer.Version(nsq.LookupProtocolV1Magic)
	}
	err := peer.WriteCommand(cmd)
	if err != nil {
		peer.Close()
		return nil, err
	}
	resp, err := peer.ReadResponse()
	if err != nil {
		peer.Close()
		return nil, err
	}
	return resp, nil
}
