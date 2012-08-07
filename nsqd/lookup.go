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
var syncTopicChan = make(chan *nsq.LookupPeer)
var lookupPeers = make([]*nsq.LookupPeer, 0)

func lookupRouter(lookupHosts []string, exitChan chan int, exitSyncChan chan int) {
	tcpAddr, _ := net.ResolveTCPAddr("tcp", *tcpAddress)
	port := tcpAddr.Port

	netIps := getNetworkIPs(tcpAddr)
	for _, ip := range netIps {
		log.Printf("LOOKUP: identified interface %s", ip)
	}

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
		notify.Start("new_channel", notifyChannelChan)
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
		case newChannel := <-notifyChannelChan:
			// notify all nsqds that a new channel exists
			channel := newChannel.(*Channel)
			log.Printf("LOOKUP: new channel %s", channel.name)
			for _, lookupPeer := range lookupPeers {
				_, err := lookupPeer.Command(nsq.Announce(channel.topicName, channel.name, port, netIps))
				if err != nil {
					log.Printf("ERROR: [%s] announce failed - %s", lookupPeer, err.Error())
				}
			}
		case newTopic := <-notifyTopicChan:
			// notify all nsqds that a new topic exists
			topic := newTopic.(*Topic)
			log.Printf("LOOKUP: new topic %s", topic.name)
			for _, lookupPeer := range lookupPeers {
				_, err := lookupPeer.Command(nsq.Announce(topic.name, ".", port, netIps))
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
					_, err := lookupPeer.Command(nsq.Announce(topic.name, ".", port, netIps))
					if err != nil {
						log.Printf("ERROR: [%s] announce failed - %s", lookupPeer, err.Error())
					}
				} else {
					for _, channel := range topic.channelMap {
						_, err := lookupPeer.Command(nsq.Announce(channel.topicName, channel.name, port, netIps))
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
		notify.Stop("new_channel", notifyChannelChan)
		notify.Stop("new_topic", notifyTopicChan)
	}
	exitSyncChan <- 1
}

func getNetworkIPs(tcpAddr *net.TCPAddr) []string {
	interfaceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatalf("ERROR: failed to identify interface addresses - %s", err.Error())
	}

	netIps := make([]string, 0)
	if tcpAddr.IP.Equal(net.IPv4zero) || tcpAddr.IP.Equal(net.IPv6zero) {
		// we're listening on all interfaces so send them all
		for _, intAddr := range interfaceAddrs {
			ip, _, err := net.ParseCIDR(intAddr.String())
			if err != nil {
				log.Fatalf("ERROR: %s", err.Error())
			}
			// eliminate any link local addresses, for simplicity
			if !ip.IsLinkLocalMulticast() && !ip.IsLinkLocalUnicast() {
				netIps = append(netIps, ip.String())
			}
		}
	} else {
		netIps = append(netIps, tcpAddr.IP.String())
	}

	// always append the hostname last
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: failed to get hostname - %s", err.Error())
	}
	netIps = append(netIps, hostname)

	return netIps
}
