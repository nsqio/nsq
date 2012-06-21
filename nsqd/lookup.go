package main

import (
	"../nsq"
	"bitly/notify"
	"log"
	"net"
	"os"
	"time"
)

var notifyChannelChan = make(chan interface{})
var notifyTopicChan = make(chan interface{})
var syncTopicChan = make(chan *LookupPeerWrapper)
var lookupPeers = make([]*LookupPeerWrapper, 0)

var (
	LookupPeerStateDisconnected int32 = 0
	LookupPeerStateConnected    int32 = 1
	LookupPeerStateSyncing      int32 = 2
)

type LookupPeerWrapper struct {
	state int32
	peer  *nsq.LookupPeer
}

func (w *LookupPeerWrapper) Command(cmd *nsq.ProtocolCommand) ([]byte, error) {
	peer := w.peer
	initialState := w.state
	if !peer.IsConnected() {
		err := peer.Connect()
		if err != nil {
			log.Printf("LOOKUP: failed to connect to %s", peer.String())
			return nil, err
		}
		w.state = LookupPeerStateConnected
		peer.Version(nsq.LookupProtocolV1Magic)
		if initialState == LookupPeerStateDisconnected {
			go func() {
				syncTopicChan <- w
			}()
		}
	}
	err := peer.WriteCommand(cmd)
	if err != nil {
		peer.Close()
		w.state = 0
		return nil, err
	}
	resp, err := peer.ReadResponse()
	if err != nil {
		peer.Close()
		w.state = 0
		return nil, err
	}
	return resp, nil
}

// TODO: this needs a clean shutdown
func LookupRouter(lookupHosts []string) {
	if len(lookupHosts) == 0 {
		return
	}

	tcpAddr, _ := net.ResolveTCPAddr("tcp", *tcpAddress)
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("ERROR: failed to get hostname - %s", err.Error())
		return
	}

	for _, host := range lookupHosts {
		tcpAddr, err := net.ResolveTCPAddr("tcp", host)
		if err != nil {
			log.Fatal("LOOKUP: could not resolve TCP address for %s", host)
		}
		log.Printf("LOOKUP: adding peer %s", tcpAddr.String())
		lookupPeer := nsq.NewLookupPeer(tcpAddr)
		lookupPeerWrapper := &LookupPeerWrapper{
			peer: lookupPeer,
		}
		lookupPeers = append(lookupPeers, lookupPeerWrapper)
	}

	notify.Observe("new_channel", notifyChannelChan)
	notify.Observe("new_topic", notifyTopicChan)

	ticker := time.Tick(15 * time.Second)
	for {
		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				log.Printf("LOOKUP: sending heartbeat to %s", lookupPeer.peer.String())
				lookupPeer.Command(lookupPeer.peer.Ping())
			}
		case newChannel := <-notifyChannelChan:
			channel := newChannel.(*Channel)
			log.Printf("LOOKUP: new channel %s", channel.name)
			// TODO: notify all nsqds that a new channel exists
		case newTopic := <-notifyTopicChan:
			// notify all nsqds that a new topic exists
			topic := newTopic.(*Topic)
			log.Printf("LOOKUP: new topic %s", topic.name)
			for _, lookupPeer := range lookupPeers {
				// TODO: we can't use hostname here because it doesn't work when a host has multiple addresses
				// and you only listen on one of them, or when you confnigure for 127.0.0.1 only.
				// this should just be blank, and the lookupd should determine the host it receives a request
				// from automatically
				lookupPeer.Command(lookupPeer.peer.Announce(topic.name, hostname, tcpAddr.Port))
			}
		case lookupPeer := <-syncTopicChan:
			topicMutex.RLock()
			lookupPeer.state = LookupPeerStateSyncing
			for _, topic := range topicMap {
				lookupPeer.Command(lookupPeer.peer.Announce(topic.name, hostname, tcpAddr.Port))
			}
			topicMutex.RUnlock()
		}
	}
}
