package main

import (
	"../nsq"
	"bitly/notify"
	"log"
	"net"
	"time"
)

var notifyChannelChan = make(chan interface{})
var notifyTopicChan = make(chan interface{})
var syncTopicChan = make(chan *LookupPeerWrapper)
var lookupPeers = make([]*LookupPeerWrapper, 0)

const (
	LookupPeerStateDisconnected = 0
	LookupPeerStateConnected    = 1
	LookupPeerStateSyncing      = 2
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

func LookupRouter(lookupHosts []string, exitChan chan int) {
	if len(lookupHosts) == 0 {
		return
	}

	tcpAddr, _ := net.ResolveTCPAddr("tcp", *tcpAddress)

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

	// for announcements, lookupd determines the host automatically
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
			// notify all nsqds that a new channel exists
			channel := newChannel.(*Channel)
			log.Printf("LOOKUP: new channel %s", channel.name)
			for _, lookupPeer := range lookupPeers {
				lookupPeer.Command(lookupPeer.peer.Announce(channel.topicName, channel.name, tcpAddr.Port))
			}
		case newTopic := <-notifyTopicChan:
			// notify all nsqds that a new topic exists
			topic := newTopic.(*Topic)
			log.Printf("LOOKUP: new topic %s", topic.name)
			for _, lookupPeer := range lookupPeers {
				lookupPeer.Command(lookupPeer.peer.Announce(topic.name, ".", tcpAddr.Port))
			}
		case lookupPeer := <-syncTopicChan:
			nsqd.RLock()
			lookupPeer.state = LookupPeerStateSyncing
			for _, topic := range nsqd.topicMap {
				topic.RLock()
				// either send a single topic announcement or send an announcement for each of the channels
				if len(topic.channelMap) == 0 {
					lookupPeer.Command(lookupPeer.peer.Announce(topic.name, ".", tcpAddr.Port))
				} else {
					for _, channel := range topic.channelMap {
						lookupPeer.Command(lookupPeer.peer.Announce(channel.topicName, channel.name, tcpAddr.Port))
					}
				}
				topic.RUnlock()
			}
			nsqd.RUnlock()
		case <-exitChan:
			notify.Ignore("new_channel", notifyChannelChan)
			notify.Ignore("new_topic", notifyTopicChan)
			return
		}
	}
}
