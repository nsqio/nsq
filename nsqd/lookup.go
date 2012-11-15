package main

import (
	"../nsq"
	"../util"
	"bytes"
	"encoding/json"
	"github.com/bitly/go-notify"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

func (n *NSQd) lookupLoop() {
	notifyChannelChan := make(chan interface{})
	notifyTopicChan := make(chan interface{})
	syncTopicChan := make(chan *nsq.LookupPeer)

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: failed to get hostname - %s", err.Error())
	}

	for _, host := range n.lookupdTCPAddrs {
		log.Printf("LOOKUP: adding peer %s", host)
		lookupPeer := nsq.NewLookupPeer(host, func(lp *nsq.LookupPeer) {
			ci := make(map[string]interface{})
			ci["version"] = util.BINARY_VERSION
			ci["tcp_port"] = n.tcpAddr.Port
			ci["http_port"] = n.httpAddr.Port
			ci["address"] = hostname
			cmd, err := nsq.Identify(ci)
			if err != nil {
				lp.Close()
				return
			}
			resp, err := lp.Command(cmd)
			if err != nil {
				log.Printf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err.Error())
			} else if bytes.Equal(resp, []byte("E_INVALID")) {
				log.Printf("LOOKUPD(%s): lookupd returned %s", lp, resp)
			} else {
				err = json.Unmarshal(resp, &lp.Info)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR parsing response - %v", lp, resp)
				} else {
					log.Printf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
				}
			}

			go func() {
				syncTopicChan <- lp
			}()
		})
		lookupPeer.Command(nil) // start the connection
		n.lookupPeers = append(n.lookupPeers, lookupPeer)
	}

	if len(n.lookupPeers) > 0 {
		notify.Start("channel_change", notifyChannelChan)
		notify.Start("topic_change", notifyTopicChan)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range n.lookupPeers {
				log.Printf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
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
			for _, lookupPeer := range n.lookupPeers {
				log.Printf("LOOKUPD(%s): channel %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
				}
			}
		case newTopic := <-notifyTopicChan:
			// notify all nsqds that a new topic exists, or that it's removed
			topic := newTopic.(*Topic)
			var cmd *nsq.Command
			if topic.Exiting() == true {
				cmd = nsq.UnRegister(topic.name, "")
			} else {
				cmd = nsq.Register(topic.name, "")
			}
			for _, lookupPeer := range n.lookupPeers {
				log.Printf("LOOKUPD(%s): topic %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
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
				log.Printf("LOOKUPD(%s): %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					log.Printf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err.Error())
					break
				}
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("LOOKUP: closing")
	if len(n.lookupPeers) > 0 {
		notify.Stop("channel_change", notifyChannelChan)
		notify.Stop("topic_change", notifyTopicChan)
	}
}

func (n *NSQd) lookupHttpAddrs() []string {
	var lookupHttpAddrs []string
	for _, lp := range n.lookupPeers {
		if len(lp.Info.Address) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.Address, strconv.Itoa(lp.Info.HttpPort))
		lookupHttpAddrs = append(lookupHttpAddrs, addr)
	}
	return lookupHttpAddrs
}
