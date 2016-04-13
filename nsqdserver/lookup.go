package nsqdserver

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
)

func connectCallback(ctx *context, hostname string, syncTopicChan chan *lookupPeer, exitChan chan int) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = ctx.realTCPAddr().Port
		ci["http_port"] = ctx.realHTTPAddr().Port
		ci["hostname"] = hostname
		ci["broadcast_address"] = ctx.getOpts().BroadcastAddress

		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		resp, err := lp.Command(cmd)
		if err != nil {
			nsqd.NsqLogger().Logf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			nsqd.NsqLogger().Logf("LOOKUPD(%s): lookupd returned %s", lp, resp)
		} else {
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				nsqd.NsqLogger().Logf("LOOKUPD(%s): ERROR parsing response - %s", lp, resp)
			} else {
				nsqd.NsqLogger().Logf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
			}
		}

		go func() {
			select {
			case syncTopicChan <- lp:
			case <-exitChan:
				return
			}
		}()
	}
}

func (n *NsqdServer) lookupLoop(metaNotifyChan chan interface{}, optsNotifyChan chan struct{}, exitChan chan int) {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	syncTopicChan := make(chan *lookupPeer)
	connect := true

	hostname, err := os.Hostname()
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	allHosts := make([]string, 0)
	discoveryAddrs := make([]string, 0)
	for {
		if connect {
			allHosts = allHosts[:0]
			allHosts = append(allHosts, n.ctx.getOpts().NSQLookupdTCPAddresses...)
			allHosts = append(allHosts, discoveryAddrs...)

			var tmpPeers []*lookupPeer
			var tmpAddrs []string

			for _, lp := range lookupPeers {
				if in(lp.addr, allHosts) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				nsqd.NsqLogger().Logf("LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs

			for _, host := range allHosts {
				if in(host, lookupAddrs) {
					continue
				}
				nsqd.NsqLogger().Logf("LOOKUP(%s): adding peer", host)
				lookupPeer := newLookupPeer(host, n.ctx.getOpts().MaxBodySize, n.ctx.getOpts().Logger,
					connectCallback(n.ctx, hostname, syncTopicChan, exitChan))
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}

		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				nsqd.NsqLogger().Logf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					nsqd.NsqLogger().Logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}
			}
			// discovery the new lookup
			if n.ctx.nsqdCoord != nil {
				discoveried, err := n.ctx.nsqdCoord.GetAllLookupdNodes()
				if err != nil {
					nsqd.NsqLogger().Logf("discovery lookup failed: %v", err)
				} else {
					discoveryAddrs = discoveryAddrs[:0]
					for _, l := range discoveried {
						discoveryAddrs = append(discoveryAddrs, net.JoinHostPort(l.NodeIp, l.TcpPort))
					}
					nsqd.NsqLogger().Logf("discovery lookup nodes: %v", discoveryAddrs)
				}
			}
		case val := <-metaNotifyChan:
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *nsqd.Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				// For channel, we just know the full topic name without
				// knowing the partition
				branch = "channel"
				channel := val.(*nsqd.Channel)
				if channel.Exiting() == true || channel.IsConsumeDisabled() {
					cmd = nsq.UnRegister(channel.GetTopicName(),
						strconv.Itoa(channel.GetTopicPart()), channel.GetName())
				} else {
					cmd = nsq.Register(channel.GetTopicName(),
						strconv.Itoa(channel.GetTopicPart()), channel.GetName())
				}
			case *nsqd.Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*nsqd.Topic)
				if topic.Exiting() == true || !topic.IsMasterEnabled() {
					cmd = nsq.UnRegister(topic.GetTopicName(),
						strconv.Itoa(topic.GetTopicPart()), "")
				} else {
					cmd = nsq.Register(topic.GetTopicName(),
						strconv.Itoa(topic.GetTopicPart()), "")
				}
			}

			errLookupPeers := make([]*lookupPeer, 0)
			for _, lookupPeer := range lookupPeers {
				nsqd.NsqLogger().Logf("LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					nsqd.NsqLogger().LogErrorf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
					errLookupPeers = append(errLookupPeers, lookupPeer)
				}
			}
			if len(errLookupPeers) > 0 {
				go func() {
					time.Sleep(time.Second * 3)
					for _, l := range errLookupPeers {
						select {
						case syncTopicChan <- l:
						case <-exitChan:
							return
						}
					}
				}()
			}
		case lookupPeer := <-syncTopicChan:
			var commands []*nsq.Command
			// build all the commands first so we exit the lock(s) as fast as possible
			topicMap := n.ctx.nsqd.GetTopicMapCopy()
			for _, topicParts := range topicMap {
				for _, topic := range topicParts {
					if !topic.IsMasterEnabled() {
						continue
					}
					channelMap := topic.GetChannelMapCopy()
					commands = append(commands,
						nsq.Register(topic.GetTopicName(),
							strconv.Itoa(topic.GetTopicPart()), ""))
					for _, channel := range channelMap {
						commands = append(commands,
							nsq.Register(channel.GetTopicName(),
								strconv.Itoa(channel.GetTopicPart()), channel.GetName()))
					}
				}
			}

			// avoid too much command once, we need sleep here
			for _, cmd := range commands {
				nsqd.NsqLogger().Logf("LOOKUPD(%s): %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					nsqd.NsqLogger().LogErrorf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
					go func() {
						time.Sleep(time.Second * 3)
						select {
						case syncTopicChan <- lookupPeer:
						case <-exitChan:
							return
						}
					}()
					break
				}
				time.Sleep(time.Millisecond * 100)
			}
		case <-optsNotifyChan:
			connect = true
		case <-exitChan:

			goto exit
		}
	}

exit:
	nsqd.NsqLogger().Logf("LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

func (n *NsqdServer) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
