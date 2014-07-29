package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/lookupd"
	"github.com/bitly/nsq/util/registrationdb"
	"github.com/hashicorp/serf/serf"
)

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex

	opts *nsqdOptions

	healthMtx sync.RWMutex
	healthy   int32
	err       error

	topicMap map[string]*Topic

	lookupPeers []*lookupPeer

	tcpAddr       *net.TCPAddr
	httpAddr      *net.TCPAddr
	httpsAddr     *net.TCPAddr
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	serf          *serf.Serf
	serfEventChan chan serf.Event
	gossipChan    chan interface{}
	rdb           *registrationdb.RegistrationDB

	idChan     chan MessageID
	notifyChan chan interface{}
	exitChan   chan int
	waitGroup  util.WaitGroupWrapper
}

func NewNSQD(opts *nsqdOptions) *NSQD {
	n := &NSQD{
		opts:       opts,
		healthy:    1,
		topicMap:   make(map[string]*Topic),
		idChan:     make(chan MessageID, 4096),
		exitChan:   make(chan int),
		notifyChan: make(chan interface{}),
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		n.logf("FATAL: --max-deflate-level must be [1,9]")
		os.Exit(1)
	}

	if opts.ID < 0 || opts.ID >= 4096 {
		n.logf("FATAL: --worker-id must be [0,4096)")
		os.Exit(1)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", opts.TCPAddress)
	if err != nil {
		n.logf("FATAL: failed to resolve TCP address (%s) - %s", opts.TCPAddress, err)
		os.Exit(1)
	}
	n.tcpAddr = tcpAddr

	httpAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPAddress)
	if err != nil {
		n.logf("FATAL: failed to resolve HTTP address (%s) - %s", opts.HTTPAddress, err)
		os.Exit(1)
	}
	n.httpAddr = httpAddr

	if opts.HTTPSAddress != "" {
		httpsAddr, err := net.ResolveTCPAddr("tcp", opts.HTTPSAddress)
		if err != nil {
			n.logf("FATAL: failed to resolve HTTPS address (%s) - %s", opts.HTTPSAddress, err)
			os.Exit(1)
		}
		n.httpsAddr = httpsAddr
	}

	if opts.StatsdPrefix != "" {
		statsdHostKey := util.StatsdHostKey(net.JoinHostPort(opts.BroadcastAddress,
			strconv.Itoa(httpAddr.Port)))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	gossipAddr, err := net.ResolveTCPAddr("tcp", options.GossipAddress)
	if err != nil {
		log.Fatal(err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	serfEventChan := make(chan serf.Event, 256)
	serfConfig := serf.DefaultConfig()
	serfConfig.Init()
	serfConfig.Tags["role"] = "nsqd"
	serfConfig.Tags["tp"] = strconv.Itoa(tcpAddr.Port)
	serfConfig.Tags["hp"] = strconv.Itoa(httpAddr.Port)
	if httpsAddr != nil {
		serfConfig.Tags["hps"] = strconv.Itoa(httpsAddr.Port)
	}
	serfConfig.Tags["ba"] = options.BroadcastAddress
	serfConfig.Tags["h"] = hostname
	serfConfig.Tags["v"] = util.BINARY_VERSION
	serfConfig.NodeName = net.JoinHostPort(options.BroadcastAddress, strconv.Itoa(tcpAddr.Port))
	serfConfig.MemberlistConfig.BindAddr = gossipAddr.IP.String()
	serfConfig.MemberlistConfig.BindPort = gossipAddr.Port
	serfConfig.MemberlistConfig.GossipInterval = 100 * time.Millisecond
	serfConfig.MemberlistConfig.GossipNodes = 5
	serfConfig.EventCh = serfEventChan
	serfConfig.EventBuffer = 1024
	serf, err := serf.Create(serfConfig)
	if err != nil {
		log.Fatal(err)
	}
	n.serf = serf
	n.serfEventChan = serfEventChan
	n.gossipChan = make(chan interface{})
	n.rdb = registrationdb.New()

	if opts.TLSClientAuthPolicy != "" {
		opts.TLSRequired = true
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		n.logf("FATAL: failed to build TLS config - %s", err)
		os.Exit(1)
	}
	if tlsConfig == nil && n.opts.TLSRequired {
		n.logf("FATAL: cannot require TLS client connections without TLS key and cert")
		os.Exit(1)
	}
	n.tlsConfig = tlsConfig

	n.waitGroup.Wrap(func() { n.idPump() })
	n.waitGroup.Wrap(func() { n.serfEventLoop() })
	n.waitGroup.Wrap(func() { n.gossipLoop() })

	n.logf(util.Version("nsqd"))
	n.logf("ID: %d", n.opts.ID)

	return n
}

func (n *NSQD) logf(f string, args ...interface{}) {
	if n.opts.Logger == nil {
		return
	}
	n.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (n *NSQD) SetHealth(err error) {
	n.healthMtx.Lock()
	defer n.healthMtx.Unlock()
	n.err = err
	if err != nil {
		atomic.StoreInt32(&n.healthy, 0)
	} else {
		atomic.StoreInt32(&n.healthy, 1)
	}
}

func (n *NSQD) IsHealthy() bool {
	return atomic.LoadInt32(&n.healthy) == 1
}

func (n *NSQD) GetError() error {
	n.healthMtx.RLock()
	defer n.healthMtx.RUnlock()
	return n.err
}

func (n *NSQD) GetHealth() string {
	if !n.IsHealthy() {
		return fmt.Sprintf("NOK - %s", n.GetError())
	}
	return "OK"
}

func (n *NSQD) Main() {
	var httpListener net.Listener
	var httpsListener net.Listener

	ctx := &context{n}

	n.waitGroup.Wrap(func() { n.lookupLoop() })

	tcpListener, err := net.Listen("tcp", n.tcpAddr.String())
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.tcpAddr, err)
		os.Exit(1)
	}
	n.tcpListener = tcpListener
	tcpServer := &tcpServer{ctx: ctx}
	n.waitGroup.Wrap(func() {
		util.TCPServer(n.tcpListener, tcpServer, n.opts.Logger)
	})

	if n.tlsConfig != nil && n.httpsAddr != nil {
		httpsListener, err = tls.Listen("tcp", n.httpsAddr.String(), n.tlsConfig)
		if err != nil {
			n.logf("FATAL: listen (%s) failed - %s", n.httpsAddr, err)
			os.Exit(1)
		}
		n.httpsListener = httpsListener
		httpsServer := &httpServer{
			ctx:         ctx,
			tlsEnabled:  true,
			tlsRequired: true,
		}
		n.waitGroup.Wrap(func() {
			util.HTTPServer(n.httpsListener, httpsServer, n.opts.Logger, "HTTPS")
		})
	}
	httpListener, err = net.Listen("tcp", n.httpAddr.String())
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.httpAddr, err)
		os.Exit(1)
	}
	n.httpListener = httpListener
	httpServer := &httpServer{
		ctx:         ctx,
		tlsEnabled:  false,
		tlsRequired: n.opts.TLSRequired,
	}
	n.waitGroup.Wrap(func() {
		util.HTTPServer(n.httpListener, httpServer, n.opts.Logger, "HTTP")
	})

	if n.opts.StatsdAddress != "" {
		n.waitGroup.Wrap(func() { n.statsdLoop() })
	}
}

func (n *NSQD) LoadMetadata() {
	fn := fmt.Sprintf(path.Join(n.opts.DataPath, "nsqd.%d.dat"), n.opts.ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			n.logf("ERROR: failed to read channel metadata from %s - %s", fn, err)
		}
		return
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		n.logf("ERROR: failed to parse metadata - %s", err)
		return
	}

	topics, err := js.Get("topics").Array()
	if err != nil {
		n.logf("ERROR: failed to parse metadata - %s", err)
		return
	}

	for ti := range topics {
		topicJs := js.Get("topics").GetIndex(ti)

		topicName, err := topicJs.Get("name").String()
		if err != nil {
			n.logf("ERROR: failed to parse metadata - %s", err)
			return
		}
		if !util.IsValidTopicName(topicName) {
			n.logf("WARNING: skipping creation of invalid topic %s", topicName)
			continue
		}
		topic := n.GetTopic(topicName)

		paused, _ := topicJs.Get("paused").Bool()
		if paused {
			topic.Pause()
		}

		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			n.logf("ERROR: failed to parse metadata - %s", err)
			return
		}

		for ci := range channels {
			channelJs := topicJs.Get("channels").GetIndex(ci)

			channelName, err := channelJs.Get("name").String()
			if err != nil {
				n.logf("ERROR: failed to parse metadata - %s", err)
				return
			}
			if !util.IsValidChannelName(channelName) {
				n.logf("WARNING: skipping creation of invalid channel %s", channelName)
				continue
			}
			channel := topic.GetChannel(channelName)

			paused, _ = channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}
		}
	}
}

func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.opts.DataPath, "nsqd.%d.dat"), n.opts.ID)
	n.logf("NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := make([]interface{}, 0)
	for _, topic := range n.topicMap {
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := make([]interface{}, 0)
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if !channel.ephemeral {
				channelData := make(map[string]interface{})
				channelData["name"] = channel.name
				channelData["paused"] = channel.IsPaused()
				channels = append(channels, channelData)
			}
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = util.BINARY_VERSION
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fileName + ".tmp"
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = atomic_rename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	// TODO: should only the "bootstrap" node leave?
	// n.serf.Leave()
	n.serf.Shutdown()

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf("ERROR: failed to persist metadata - %s", err)
	}
	n.logf("NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
	n.waitGroup.Wait()
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string) *Topic {
	n.Lock()
	t, ok := n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	} else {
		deleteCallback := func(t *Topic) {
			n.DeleteExistingTopic(t.name)
		}
		t = NewTopic(topicName, &context{n}, deleteCallback)
		n.topicMap[topicName] = t

		n.logf("TOPIC(%s): created", t.name)

		// release our global nsqd lock, and switch to a more granular topic lock while we init our
		// channels from lookupd. This blocks concurrent PutMessages to this topic.
		t.Lock()
		n.Unlock()
		// if using lookupd, make a blocking call to get the topics, and immediately create them.
		// this makes sure that any message received is buffered to the right channels
		if len(n.lookupPeers) > 0 {
			channelNames, _ := lookupd.GetLookupdTopicChannels(t.name, n.lookupHttpAddrs())
			for _, channelName := range channelNames {
				t.getOrCreateChannel(channelName)
			}
		}
		t.Unlock()

		// NOTE: I would prefer for this to only happen in topic.GetChannel() but we're special
		// casing the code above so that we can control the locks such that it is impossible
		// for a message to be written to a (new) topic while we're looking up channels
		// from lookupd...
		//
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

func (n *NSQD) idPump() {
	factory := &guidFactory{}
	lastError := time.Now()
	for {
		id, err := factory.NewGUID(n.opts.ID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				n.logf("ERROR: %s", err)
				lastError = now
			}
			runtime.Gosched()
			continue
		}
		select {
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf("ID: closing")
}

func (n *NSQD) Notify(v interface{}) {
	// by selecting on exitChan we guarantee that
	// we do not block exit, see issue #123
	select {
	case <-n.exitChan:
	case n.notifyChan <- v:
		n.Lock()
		err := n.PersistMetadata()
		if err != nil {
			n.logf("ERROR: failed to persist metadata - %s", err)
		}
		n.Unlock()
	}

	select {
	case <-n.exitChan:
	case n.gossipChan <- v:
	}
}

func memberToProducer(member serf.Member) registrationdb.Producer {
	tcpPort, _ := strconv.Atoi(member.Tags["tp"])
	httpPort, _ := strconv.Atoi(member.Tags["hp"])
	return registrationdb.Producer{
		ID: member.Name,
		RemoteAddress: net.JoinHostPort(member.Addr.String(),
			strconv.Itoa(int(member.Port))),
		LastUpdate:       time.Now().UnixNano(),
		BroadcastAddress: member.Tags["ba"],
		Hostname:         member.Tags["h"],
		TCPPort:          tcpPort,
		HTTPPort:         httpPort,
		Version:          member.Tags["v"],
	}
}

func (n *NSQD) serfMemberJoin(ev serf.Event) {
	memberEv := ev.(serf.MemberEvent)
	log.Printf("MEMBER EVENT: %+v - members: %+v", memberEv, memberEv.Members)
	for _, member := range memberEv.Members {
		producer := memberToProducer(member)
		r := registrationdb.Registration{"client", "", ""}
		n.rdb.AddProducer(r, producer)
		log.Printf("DB: member(%s) REGISTER category:%s key:%s subkey:%s",
			producer.ID,
			r.Category,
			r.Key,
			r.SubKey)
	}
}

func (n *NSQD) serfMemberFailed(ev serf.Event) {
	memberEv := ev.(serf.MemberEvent)
	log.Printf("MEMBER EVENT: %+v - members: %+v", memberEv, memberEv.Members)
	for _, member := range memberEv.Members {
		registrations := n.rdb.LookupRegistrations(member.Name)
		for _, r := range registrations {
			if removed, _ := n.rdb.RemoveProducer(r, member.Name); removed {
				log.Printf("DB: member(%s) UNREGISTER category:%s key:%s subkey:%s",
					member.Name, r.Category, r.Key, r.SubKey)
			}
		}
	}
}

func (n *NSQD) serfUserEvent(ev serf.Event) {
	var gev gossipEvent
	var member serf.Member
	userEv := ev.(serf.UserEvent)
	err := json.Unmarshal(userEv.Payload, &gev)
	if err != nil {
		log.Printf("ERROR: failed to Unmarshal gossipEvent - %s", err)
		return
	}
	log.Printf("gossipEvent: %+v", gev)
	found := false
	for _, m := range n.serf.Members() {
		if m.Name == gev.Name {
			member = m
			found = true
		}
	}
	if !found {
		log.Printf("ERROR: received gossipEvent for member not in list - %s",
			userEv.Name)
		return
	}
	producer := memberToProducer(member)
	operation := userEv.Name[len(userEv.Name)-1]
	switch operation {
	case '+', '=':
		// a create event
		var registrations []registrationdb.Registration
		if gev.Channel != "" {
			registrations = append(registrations, registrationdb.Registration{
				Category: "channel",
				Key:      gev.Topic,
				SubKey:   gev.Channel,
			})
		}
		registrations = append(registrations, registrationdb.Registration{
			Category: "topic",
			Key:      gev.Topic,
		})
		for _, r := range registrations {
			if n.rdb.AddProducer(r, producer) {
				log.Printf("DB: member(%s) REGISTER category:%s key:%s subkey:%s",
					gev.Name,
					r.Category,
					r.Key,
					r.SubKey)
			}
			if operation == '=' && n.rdb.TouchRegistration(r.Category, r.Key, r.SubKey, producer.ID) {
				log.Printf("DB: member(%s) TOUCH category:%s key:%s subkey:%s",
					gev.Name,
					r.Category,
					r.Key,
					r.SubKey)
			}
		}
	case '-':
		// a delete event
		if gev.Channel != "" {
			r := registrationdb.Registration{
				Category: "channel",
				Key:      gev.Topic,
				SubKey:   gev.Channel,
			}
			removed, left := n.rdb.RemoveProducer(r, producer.ID)
			if removed {
				log.Printf("DB: member(%s) UNREGISTER category:%s key:%s subkey:%s",
					gev.Name,
					r.Category,
					r.Key,
					r.SubKey)
			}
			// for ephemeral channels, remove the channel as well if it has no producers
			if left == 0 && strings.HasSuffix(gev.Channel, "#ephemeral") {
				n.rdb.RemoveRegistration(r)
			}
		} else {
			// no channel was specified so this is a topic unregistration
			// remove all of the channel registrations...
			// normally this shouldn't happen which is why we print a warning message
			// if anything is actually removed
			registrations := n.rdb.FindRegistrations("channel", gev.Topic, "*")
			for _, r := range registrations {
				if removed, _ := n.rdb.RemoveProducer(r, producer.ID); removed {
					log.Printf("WARNING: client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
						gev.Name, r.Category, r.Key, r.SubKey)
				}
			}

			r := registrationdb.Registration{
				Category: "topic",
				Key:      gev.Topic,
				SubKey:   "",
			}
			if removed, _ := n.rdb.RemoveProducer(r, producer.ID); removed {
				log.Printf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					gev.Name, r.Category, r.Key, r.SubKey)
			}
		}
	}
}

func (n *NSQD) serfEventLoop() {
	// go func() {
	// 	for {
	// 		time.Sleep(500 * time.Millisecond)
	// 		resp, err := n.serf.Query("foo", []byte("bar"), nil)
	// 		if err != nil {
	// 			log.Printf("ERROR: query failed - %s", err)
	// 			continue
	// 		}
	// 		nr, ok := <-resp.ResponseCh()
	// 		if !ok {
	// 			log.Printf("ERROR: query timed out")
	// 			continue
	// 		}
	// 		resp.Close()
	// 		log.Printf("node response: %+v", nr)
	// 	}
	// }()

	for {
		select {
		case ev := <-n.serfEventChan:
			switch ev.EventType() {
			case serf.EventMemberJoin:
				n.serfMemberJoin(ev)
			case serf.EventMemberLeave:
				// nothing (should never happen)
			case serf.EventMemberFailed:
				n.serfMemberFailed(ev)
			case serf.EventMemberReap:
				// nothing
			case serf.EventUser:
				n.serfUserEvent(ev)
			case serf.EventQuery:
				// query := ev.(*serf.Query)
				// query.Respond([]byte("baz"))
				// log.Printf("QUERY EVENT: %+v", query)
			case serf.EventMemberUpdate:
				// nothing
			default:
				log.Printf("WARNING: unhandled Serf Event: %#v", ev)
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("SERF: exiting")
}

type gossipEvent struct {
	Name    string `json:"n"`
	Topic   string `json:"t"`
	Channel string `json:"c"`
	Rnd     int64  `json:"r"`
}

func (n *NSQD) gossip(evName string, topicName string, channelName string) error {
	gev := gossipEvent{
		Name:    n.serf.LocalMember().Name,
		Topic:   topicName,
		Channel: channelName,
		Rnd:     time.Now().UnixNano(),
	}
	payload, err := json.Marshal(&gev)
	if err != nil {
		return err
	}
	return n.serf.UserEvent(evName, payload, false)
}

func (n *NSQD) gossipLoop() {
	var evName string
	var topicName string
	var channelName string

	regossipTicker := time.NewTicker(60 * time.Second)

	if len(n.options.SeedNodeAddresses) > 0 {
		for {
			num, err := n.serf.Join(n.options.SeedNodeAddresses, false)
			if err != nil {
				log.Printf("ERROR: failed to join serf - %s", err)
				select {
				case <-time.After(15 * time.Second):
					// keep trying
				case <-n.exitChan:
					goto exit
				}
			}
			if num > 0 {
				log.Printf("SERF: joined %d nodes", num)
				break
			}
		}
	}

	for {
		select {
		case <-regossipTicker.C:
			log.Printf("SERF: re-gossiping")
			stats := n.GetStats()
			for _, topicStat := range stats {
				if len(topicStat.Channels) == 0 {
					// if there are no channels we just send a topic exists event
					err := n.gossip("topic=", topicStat.TopicName, "")
					if err != nil {
						log.Printf("ERROR: failed to send Serf user event - %s", err)
					}
					continue
				}
				// otherwise we know that by sending over a channel for a topic the topic
				// will be accounted for as well
				for _, channelStat := range topicStat.Channels {
					err := n.gossip("channel=", topicStat.TopicName, channelStat.ChannelName)
					if err != nil {
						log.Printf("ERROR: failed to send Serf user event - %s", err)
					}
				}
			}
		case v := <-n.gossipChan:
			switch v.(type) {
			case *Channel:
				channel := v.(*Channel)
				topicName = channel.topicName
				channelName = channel.name
				if !channel.Exiting() {
					evName = "chan+"
				} else {
					evName = "chan-"
				}
			case *Topic:
				topic := v.(*Topic)
				topicName = topic.name
				channelName = ""
				if !topic.Exiting() {
					evName = "topic+"
				} else {
					evName = "topic-"
				}
			}
			err := n.gossip(evName, topicName, channelName)
			if err != nil {
				log.Printf("ERROR: failed to send Serf user event - %s", err)
			}
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	regossipTicker.Stop()
	log.Printf("GOSSIP: exiting")
}

func buildTLSConfig(opts *nsqdOptions) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		ca_cert_file, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(ca_cert_file) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.opts.AuthHTTPAddresses) != 0
}
