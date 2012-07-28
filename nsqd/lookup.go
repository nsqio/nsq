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
var syncTopicChan = make(chan *nsq.LookupPeer)
var lookupPeers = make([]*nsq.LookupPeer, 0)

func lookupRouter(lookupHosts []string, exitChan chan int, exitSyncChan chan int) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", *tcpAddress)

	for _, host := range lookupHosts {
		tcpAddr, err := net.ResolveTCPAddr("tcp", host)
		if err != nil {
			log.Fatal("LOOKUP: could not resolve TCP address for %s", host)
		}
		log.Printf("LOOKUP: adding peer %s", tcpAddr.String())
		lookupPeer := nsq.NewLookupPeer(tcpAddr, func(lp *nsq.LookupPeer) {
			go func() {
				syncTopicChan <- lp
			}()
		})
		lookupPeers = append(lookupPeers, lookupPeer)
	}

	if len(lookupPeers) > 0 {
		notify.Observe("new_channel", notifyChannelChan)
		notify.Observe("new_topic", notifyTopicChan)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				log.Printf("LOOKUP: [%s] sending heartbeat", lookupPeer)
				_, err := lookupPeer.Command(nsq.Ping())
				if err != nil {
					log.Printf("ERROR: [%s] ping failed - %s", lookupPeer, err.Error())
				}
			}
		case newChannel := <-notifyChannelChan:
			// notify all nsqds that a new channel exists
			channel := newChannel.(*Channel)
			log.Printf("LOOKUP: new channel %s", channel.name)
			for _, lookupPeer := range lookupPeers {
				_, err := lookupPeer.Command(nsq.Announce(channel.topicName, channel.name, tcpAddr.Port))
				if err != nil {
					log.Printf("ERROR: [%s] announce failed - %s", lookupPeer, err.Error())
				}
			}
		case newTopic := <-notifyTopicChan:
			// notify all nsqds that a new topic exists
			topic := newTopic.(*Topic)
			log.Printf("LOOKUP: new topic %s", topic.name)
			for _, lookupPeer := range lookupPeers {
				_, err := lookupPeer.Command(nsq.Announce(topic.name, ".", tcpAddr.Port))
				if err != nil {
					log.Printf("ERROR: [%s] announce failed - %s", lookupPeer, err.Error())
				}
			}
		case lookupPeer := <-syncTopicChan:
			nsqd.RLock()
			for _, topic := range nsqd.topicMap {
				topic.RLock()
				// either send a single topic announcement or send an announcement for each of the channels
				if len(topic.channelMap) == 0 {
					_, err := lookupPeer.Command(nsq.Announce(topic.name, ".", tcpAddr.Port))
					if err != nil {
						log.Printf("ERROR: [%s] announce failed - %s", lookupPeer, err.Error())
					}
				} else {
					for _, channel := range topic.channelMap {
						_, err := lookupPeer.Command(nsq.Announce(channel.topicName, channel.name, tcpAddr.Port))
						if err != nil {
							log.Printf("ERROR: [%s] announce failed - %s", lookupPeer, err.Error())
						}
					}
				}
				topic.RUnlock()
			}
			nsqd.RUnlock()
		case <-exitChan:
			goto exit
		}
	}

exit:
	log.Printf("LOOKUP: closing")
	if len(lookupPeers) > 0 {
		notify.Ignore("new_channel", notifyChannelChan)
		notify.Ignore("new_topic", notifyTopicChan)
	}
	exitSyncChan <- 1
}
