package main

import (
	"../nsq"
	"bitly/notify"
	"log"
	"os"
	"time"
)

var lookupPeers = make([]*nsq.LookupPeer, 0)

func lookupRouter(lookupHosts []string, exitChan chan int) {
	notifyChannelChan := make(chan interface{})
	notifyTopicChan := make(chan interface{})
	syncTopicChan := make(chan *nsq.LookupPeer)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: failed to get hostname - %s", err.Error())
	}

	for _, host := range lookupHosts {
		log.Printf("LOOKUP: adding peer %s", host)
		lookupPeer := nsq.NewLookupPeer(host, func(lp *nsq.LookupPeer) {
			cmd := nsq.Identify(VERSION, nsqd.tcpAddr.Port, nsqd.httpAddr.Port, hostname)
			_, err := lp.Command(cmd)
			if err != nil {
				log.Printf("Error writing to %s %s", host, err.Error())
			}
			go func() {
				syncTopicChan <- lp
			}()
		})
		lookupPeers = append(lookupPeers, lookupPeer)
	}

	if len(lookupPeers) > 0 {
		notify.Start("channel_change", notifyChannelChan)
		notify.Start("new_topic", notifyTopicChan)
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
		case channelObj := <-notifyChannelChan:
			// notify all nsqds that a new channel exists, or that it's removed
			channel := channelObj.(*Channel)
			var cmd *nsq.Command
			if channel.Exiting() == true {
				cmd = nsq.UnRegister(channel.topicName, channel.name)
			} else {
				cmd = nsq.Register(channel.topicName, channel.name)
			}
			for _, lookupPeer := range lookupPeers {
				log.Printf("LOOKUP: [%s] channel %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("ERROR: [%s] %s failed - %s", lookupPeer, cmd, err.Error())
				}
			}
		case newTopic := <-notifyTopicChan:
			// notify all nsqds that a new topic exists
			topic := newTopic.(*Topic)
			cmd := nsq.Register(topic.name, "")
			for _, lookupPeer := range lookupPeers {
				log.Printf("LOOKUP: [%s] new topic %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("ERROR: [%s] announce failed - %s", lookupPeer, err.Error())
				}
			}
		case lookupPeer := <-syncTopicChan:
			commands := make([]*nsq.Command, 0)
			// build all the commands first so we exit the lock(s) as fast as possible
			nsqd.RLock()
			for _, topic := range nsqd.topicMap {
				topic.RLock()
				if len(topic.channelMap) == 0 {
					commands = append(commands, nsq.Register(topic.name, ""))
				} else {
					for _, channel := range topic.channelMap {
						commands = append(commands, nsq.Register(channel.topicName, channel.name))
					}
				}
				topic.RUnlock()
			}
			nsqd.RUnlock()

			for _, cmd := range commands {
				log.Printf("LOOKUP: [%s] %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("ERROR: [%s] announce %v failed - %s", lookupPeer, cmd, err.Error())
					break
				}
			}
		case <-exitChan:
			goto exit
		}
	}

exit:
	log.Printf("LOOKUP: closing")
	if len(lookupPeers) > 0 {
		notify.Stop("channel_change", notifyChannelChan)
		notify.Stop("new_topic", notifyTopicChan)
	}
}
