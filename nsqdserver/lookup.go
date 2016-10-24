package nsqdserver

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/clusterinfo"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
)

func connectCallback(ctx *context, hostname string, syncTopicChan chan *clusterinfo.LookupPeer, exitChan chan int) func(*clusterinfo.LookupPeer) {
	return func(lp *clusterinfo.LookupPeer) {
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = ctx.realTCPAddr().Port
		if ctx.reverseProxyPort == "" {
			ci["http_port"] = ctx.realHTTPAddr().Port
		} else {
			port, _ := strconv.Atoi(ctx.reverseProxyPort)
			ci["http_port"] = port
		}
		ci["hostname"] = hostname
		ci["broadcast_address"] = ctx.getOpts().BroadcastAddress
		ci["distributed_id"] = ctx.GetDistributedID()

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

func (n *NsqdServer) discoverLookupdNodes(discoveryAddrs []string) ([]string, bool) {
	changed := false
	// discovery the new lookup
	if n.ctx.nsqdCoord != nil {
		newDiscoveried, err := n.ctx.nsqdCoord.GetAllLookupdNodes()
		if err != nil {
			nsqd.NsqLogger().Logf("discovery lookup failed: %v", err)
		} else {
			if len(newDiscoveried) != len(discoveryAddrs) {
				changed = true
			} else {
				for _, l := range newDiscoveried {
					if !in(net.JoinHostPort(l.NodeIP, l.TcpPort), discoveryAddrs) {
						changed = true
						break
					}
				}
			}
			if changed {
				discoveryAddrs = discoveryAddrs[:0]
				for _, l := range newDiscoveried {
					discoveryAddrs = append(discoveryAddrs, net.JoinHostPort(l.NodeIP, l.TcpPort))
				}
				nsqd.NsqLogger().LogDebugf("discovery lookup nodes: %v", discoveryAddrs)
			}
		}
	}
	return discoveryAddrs, changed
}

func (n *NsqdServer) lookupLoop(pingInterval time.Duration, metaNotifyChan chan interface{}, optsNotifyChan chan struct{}, exitChan chan int) {
	var lookupPeers []*clusterinfo.LookupPeer
	var lookupAddrs []string
	syncTopicChan := make(chan *clusterinfo.LookupPeer)
	changed := true

	hostname, err := os.Hostname()
	if err != nil {
		nsqd.NsqLogger().LogErrorf("failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(pingInterval)
	allHosts := make([]string, 0)
	discoveryAddrs := make([]string, 0)
	discoveryAddrs, _ = n.discoverLookupdNodes(discoveryAddrs)

	for {
		if changed {
			allHosts = allHosts[:0]
			allHosts = append(allHosts, n.ctx.getOpts().NSQLookupdTCPAddresses...)
			allHosts = append(allHosts, discoveryAddrs...)
			nsqd.NsqLogger().Logf("all lookup hosts: %v", allHosts)

			var tmpPeers []*clusterinfo.LookupPeer
			var tmpAddrs []string

			for _, lp := range lookupPeers {
				if in(lp.String(), allHosts) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.String())
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
				lookupPeer := clusterinfo.NewLookupPeer(host, n.ctx.getOpts().MaxBodySize, n.ctx.getOpts().Logger,
					connectCallback(n.ctx, hostname, syncTopicChan, exitChan))
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			changed = false
		}

		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lp := range lookupPeers {
				nsqd.NsqLogger().LogDebugf("LOOKUPD(%s): sending heartbeat", lp)
				cmd := nsq.Ping()
				_, err := lp.Command(cmd)
				if err != nil {
					nsqd.NsqLogger().Logf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
				}
			}
			discoveryChanged := false
			discoveryAddrs, discoveryChanged = n.discoverLookupdNodes(discoveryAddrs)
			if discoveryChanged {
				changed = true
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
				if topic.Exiting() == true || topic.IsWriteDisabled() {
					cmd = nsq.UnRegister(topic.GetTopicName(),
						strconv.Itoa(topic.GetTopicPart()), "")
				} else {
					cmd = nsq.Register(topic.GetTopicName(),
						strconv.Itoa(topic.GetTopicPart()), "")
				}
			}

			errLookupPeers := make([]*clusterinfo.LookupPeer, 0)
			for _, lp := range lookupPeers {
				nsqd.NsqLogger().Logf("LOOKUPD(%s): %s %s", lp, branch, cmd)
				_, err := lp.Command(cmd)
				if err != nil {
					nsqd.NsqLogger().LogErrorf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
					errLookupPeers = append(errLookupPeers, lp)
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
		case lp := <-syncTopicChan:
			if in(lp.String(), allHosts) {
			} else {
				nsqd.NsqLogger().Logf("LOOKUP(%s): already removed ", lp)
				lp.Close()
				continue
			}

			nsqd.NsqLogger().Logf("do full sync with lookup: %v", lp)
			var commands []*nsq.Command
			// build all the commands first so we exit the lock(s) as fast as possible
			topicMap := n.ctx.nsqd.GetTopicMapCopy()
			for _, topicParts := range topicMap {
				for _, topic := range topicParts {
					if topic.IsWriteDisabled() {
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

			for index, cmd := range commands {
				nsqd.NsqLogger().Logf("LOOKUPD(%s): %s", lp, cmd)
				_, err := lp.Command(cmd)
				if err != nil {
					nsqd.NsqLogger().LogErrorf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
					if in(lp.String(), allHosts) {
						go func() {
							time.Sleep(time.Second * 3)
							select {
							case syncTopicChan <- lp:
							case <-exitChan:
								return
							}
						}()
					}
					break
				}
				// avoid too much command once, we need sleep here
				if index%10 == 0 {
					time.Sleep(time.Millisecond * 10)
				}
			}
		case <-optsNotifyChan:
			nsqd.NsqLogger().Logf("got opts notify, check new lookup")
			changed = true
			discoveryChanged := false
			discoveryAddrs, discoveryChanged = n.discoverLookupdNodes(discoveryAddrs)
			if discoveryChanged {
				changed = true
			}
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
	for _, lp := range lookupPeers.([]*clusterinfo.LookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
