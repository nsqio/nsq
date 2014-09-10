package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Handler is the message processing interface for Consumer
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Consumer will automatically handle FINishing.
//
// When the returned value is non-nil Consumer will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// HandlerFunc is a convenience type to avoid having to declare a struct
// to implement the Handler interface, it can be used like this:
//
// 	consumer.AddHandler(nsq.HandlerFunc(func(m *Message) error {
// 		// handle the message
// 	}))
type HandlerFunc func(message *Message) error

// HandleMessage implements the Handler interface
func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Consumer specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

var instCount int64

// Consumer is a high-level type to consume from NSQ.
//
// A Consumer instance is supplied a Handler that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel. See: Handler/HandlerFunc
// for details on implementing the interface to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Consumer struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	totalRdyCount    int64
	backoffDuration  int64
	maxInFlight      int32

	mtx sync.RWMutex

	logger logger
	logLvl LogLevel

	id      int64
	topic   string
	channel string
	config  Config

	backoffChan          chan bool
	rdyChan              chan *Conn
	needRDYRedistributed int32
	backoffCounter       int32

	incomingMessages chan *Message

	rdyRetryMtx    sync.RWMutex
	rdyRetryTimers map[string]*time.Timer

	pendingConnections map[string]bool
	connections        map[string]*Conn

	// used at connection close to force a possible reconnect
	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	wg              sync.WaitGroup
	runningHandlers int32
	stopFlag        int32
	connectedFlag   int32
	stopHandler     sync.Once

	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
	exitChan chan int
}

// NewConsumer creates a new instance of Consumer for the specified topic/channel
//
// The only valid way to create a Config is via NewConfig, using a struct literal will panic.
// After Config is passed into NewConsumer the values are no longer mutable (they are copied).
func NewConsumer(topic string, channel string, config *Config) (*Consumer, error) {
	config.assertInitialized()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	r := &Consumer{
		id: atomic.AddInt64(&instCount, 1),

		topic:   topic,
		channel: channel,
		config:  *config,

		logger:      log.New(os.Stderr, "", log.Flags()),
		logLvl:      LogLevelInfo,
		maxInFlight: int32(config.MaxInFlight),

		incomingMessages: make(chan *Message),

		rdyRetryTimers:     make(map[string]*time.Timer),
		pendingConnections: make(map[string]bool),
		connections:        make(map[string]*Conn),

		lookupdRecheckChan: make(chan int, 1),
		backoffChan:        make(chan bool),
		rdyChan:            make(chan *Conn, 1),

		StopChan: make(chan int),
		exitChan: make(chan int),
	}
	r.wg.Add(1)
	go r.rdyLoop()
	return r, nil
}

func (r *Consumer) conns() []*Conn {
	r.mtx.RLock()
	conns := make([]*Conn, 0, len(r.connections))
	for _, c := range r.connections {
		conns = append(conns, c)
	}
	r.mtx.RUnlock()
	return conns
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (r *Consumer) SetLogger(l logger, lvl LogLevel) {
	r.logger = l
	r.logLvl = lvl
}

// perConnMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Consumer
// is responsible for.
func (r *Consumer) perConnMaxInFlight() int64 {
	b := float64(r.getMaxInFlight())
	s := b / float64(len(r.conns()))
	return int64(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this consumer are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (r *Consumer) IsStarved() bool {
	for _, conn := range r.conns() {
		threshold := int64(float64(atomic.LoadInt64(&conn.lastRdyCount)) * 0.85)
		inFlight := atomic.LoadInt64(&conn.messagesInFlight)
		if inFlight >= threshold && inFlight > 0 && !conn.IsClosing() {
			return true
		}
	}
	return false
}

func (r *Consumer) getMaxInFlight() int32 {
	return atomic.LoadInt32(&r.maxInFlight)
}

// ChangeMaxInFlight sets a new maximum number of messages this comsumer instance
// will allow in-flight, and updates all existing connections as appropriate.
//
// For example, ChangeMaxInFlight(0) would pause message flow
//
// If already connected, it updates the reader RDY state for each connection.
func (r *Consumer) ChangeMaxInFlight(maxInFlight int) {
	if r.getMaxInFlight() == int32(maxInFlight) {
		return
	}

	atomic.StoreInt32(&r.maxInFlight, int32(maxInFlight))

	for _, c := range r.conns() {
		r.rdyChan <- c
	}
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Consumer instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}
	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	r.mtx.Lock()
	for _, x := range r.lookupdHTTPAddrs {
		if x == addr {
			r.mtx.Unlock()
			return nil
		}
	}
	r.lookupdHTTPAddrs = append(r.lookupdHTTPAddrs, addr)
	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		r.queryLookupd()
		r.wg.Add(1)
		go r.lookupdLoop()
	}

	return nil
}

// ConnectToNSQLookupd adds multiple nsqlookupd address to the list for this Consumer instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (r *Consumer) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

func validatedLookupAddr(addr string) error {
	if strings.Contains(addr, "/") {
		_, err := url.Parse(addr)
		if err != nil {
			return err
		}
		return nil
	}
	if !strings.Contains(addr, ":") {
		return errors.New("missing port")
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (r *Consumer) lookupdLoop() {
	var rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	jitter := time.Duration(int64(rng.Float64() *
		r.config.LookupdPollJitter * float64(r.config.LookupdPollInterval)))
	ticker := time.NewTicker(r.config.LookupdPollInterval)

	select {
	case <-time.After(jitter):
	case <-r.exitChan:
		goto exit
	}

	for {
		select {
		case <-ticker.C:
			r.queryLookupd()
		case <-r.lookupdRecheckChan:
			r.queryLookupd()
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	ticker.Stop()
	r.log(LogLevelInfo, "exiting lookupdLoop")
	r.wg.Done()
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (r *Consumer) nextLookupdEndpoint() string {
	r.mtx.RLock()
	addr := r.lookupdHTTPAddrs[r.lookupdQueryIndex]
	num := len(r.lookupdHTTPAddrs)
	r.mtx.RUnlock()
	r.lookupdQueryIndex = (r.lookupdQueryIndex + 1) % num

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	if u.Path == "/" || u.Path == "" {
		u.Path = "/lookup"
	}

	v, err := url.ParseQuery(u.RawQuery)
	v.Add("topic", r.topic)
	u.RawQuery = v.Encode()
	return u.String()
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (r *Consumer) queryLookupd() {
	endpoint := r.nextLookupdEndpoint()

	r.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	data, err := apiRequestNegotiateV1("GET", endpoint, nil)
	if err != nil {
		r.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		return
	}

	// {
	//     "channels": [],
	//     "producers": [
	//         {
	//             "broadcast_address": "jehiah-air.local",
	//             "http_port": 4151,
	//             "tcp_port": 4150
	//         }
	//     ],
	//     "timestamp": 1340152173
	// }
	for i := range data.Get("producers").MustArray() {
		producer := data.Get("producers").GetIndex(i)
		broadcastAddress := producer.Get("broadcast_address").MustString()
		port := producer.Get("tcp_port").MustInt()

		// make an address, start a connection
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		err = r.ConnectToNSQD(joined)
		if err != nil && err != ErrAlreadyConnected {
			r.log(LogLevelError, "(%s) error connecting to nsqd - %s", joined, err)
			continue
		}
	}
}

// ConnectToNSQD takes multiple nsqd addresses to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to local instance.
func (r *Consumer) ConnectToNSQDs(addresses []string) error {
	for _, addr := range addresses {
		err := r.ConnectToNSQD(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// ConnectToNSQD takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToNSQLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (r *Consumer) ConnectToNSQD(addr string) error {
	if atomic.LoadInt32(&r.stopFlag) == 1 {
		return errors.New("consumer stopped")
	}

	if atomic.LoadInt32(&r.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	atomic.StoreInt32(&r.connectedFlag, 1)

	_, pendingOk := r.pendingConnections[addr]
	r.mtx.RLock()
	_, ok := r.connections[addr]
	r.mtx.RUnlock()

	if ok || pendingOk {
		return ErrAlreadyConnected
	}

	r.log(LogLevelInfo, "(%s) connecting to nsqd", addr)

	conn := NewConn(addr, &r.config, &consumerConnDelegate{r})
	conn.SetLogger(r.logger, r.logLvl,
		fmt.Sprintf("%3d [%s/%s] (%%s)", r.id, r.topic, r.channel))

	cleanupConnection := func() {
		r.mtx.Lock()
		delete(r.pendingConnections, addr)
		r.mtx.Unlock()
		conn.Close()
	}

	r.pendingConnections[addr] = true

	resp, err := conn.Connect()
	if err != nil {
		cleanupConnection()
		return err
	}

	if resp != nil {
		if resp.MaxRdyCount < int64(r.getMaxInFlight()) {
			r.log(LogLevelWarning,
				"(%s) max RDY count %d < consumer max in flight %d, truncation possible",
				conn.String(), resp.MaxRdyCount, r.getMaxInFlight())
		}
	}

	cmd := Subscribe(r.topic, r.channel)
	err = conn.WriteCommand(cmd)
	if err != nil {
		cleanupConnection()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s",
			conn, r.topic, r.channel, err.Error())
	}

	delete(r.pendingConnections, addr)
	r.mtx.Lock()
	r.connections[addr] = conn
	r.mtx.Unlock()

	// pre-emptive signal to existing connections to lower their RDY count
	for _, c := range r.conns() {
		r.rdyChan <- c
	}

	return nil
}

func (r *Consumer) onConnMessage(c *Conn, msg *Message) {
	atomic.AddInt64(&r.totalRdyCount, -1)
	atomic.AddUint64(&r.messagesReceived, 1)
	r.incomingMessages <- msg
	r.rdyChan <- c
}

func (r *Consumer) onConnMessageFinished(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesFinished, 1)
}

func (r *Consumer) onConnMessageRequeued(c *Conn, msg *Message) {
	atomic.AddUint64(&r.messagesRequeued, 1)
}

func (r *Consumer) onConnBackoff(c *Conn) {
	r.backoffChan <- false
}

func (r *Consumer) onConnResume(c *Conn) {
	r.backoffChan <- true
}

func (r *Consumer) onConnResponse(c *Conn, data []byte) {
	switch {
	case bytes.Equal(data, []byte("CLOSE_WAIT")):
		// server is ready for us to close (it ack'd our StartClose)
		// we can assume we will not receive any more messages over this channel
		// (but we can still write back responses)
		r.log(LogLevelInfo, "(%s) received CLOSE_WAIT from nsqd", c.String())
		c.Close()
	}
}

func (r *Consumer) onConnError(c *Conn, data []byte) {
}

func (r *Consumer) onConnHeartbeat(c *Conn) {
}

func (r *Consumer) onConnIOError(c *Conn, err error) {
	c.Close()
}

func (r *Consumer) onConnClose(c *Conn) {
	var hasRDYRetryTimer bool

	// remove this connections RDY count from the consumer's total
	rdyCount := c.RDY()
	atomic.AddInt64(&r.totalRdyCount, -rdyCount)

	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok {
		// stop any pending retry of an old RDY update
		timer.Stop()
		delete(r.rdyRetryTimers, c.String())
		hasRDYRetryTimer = true
	}
	r.rdyRetryMtx.Unlock()

	r.mtx.Lock()
	delete(r.connections, c.String())
	left := len(r.connections)
	r.mtx.Unlock()

	r.log(LogLevelWarning, "there are %d connections left alive", left)

	if (hasRDYRetryTimer || rdyCount > 0) &&
		(int32(left) == r.getMaxInFlight() || r.inBackoff()) {
		// we're toggling out of (normal) redistribution cases and this conn
		// had a RDY count...
		//
		// trigger RDY redistribution to make sure this RDY is moved
		// to a new connection
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	// we were the last one (and stopping)
	if left == 0 && atomic.LoadInt32(&r.stopFlag) == 1 {
		r.stopHandlers()
		return
	}

	r.mtx.RLock()
	numLookupd := len(r.lookupdHTTPAddrs)
	r.mtx.RUnlock()
	if numLookupd != 0 && atomic.LoadInt32(&r.stopFlag) == 0 {
		// trigger a poll of the lookupd
		select {
		case r.lookupdRecheckChan <- 1:
		default:
		}
	} else if numLookupd == 0 && atomic.LoadInt32(&r.stopFlag) == 0 {
		// there are no lookupd, try to reconnect after a bit
		go func(addr string) {
			for {
				r.log(LogLevelInfo, "(%s) re-connecting in 15 seconds...", addr)
				time.Sleep(15 * time.Second)
				if atomic.LoadInt32(&r.stopFlag) == 1 {
					break
				}
				err := r.ConnectToNSQD(addr)
				if err != nil && err != ErrAlreadyConnected {
					r.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
					continue
				}
				break
			}
		}(c.RemoteAddr().String())
	}
}

func (r *Consumer) backoffDurationForCount(count int32) time.Duration {
	backoffDuration := r.config.BackoffMultiplier *
		time.Duration(math.Pow(2, float64(count)))
	if backoffDuration > r.config.MaxBackoffDuration {
		backoffDuration = r.config.MaxBackoffDuration
	}
	return backoffDuration
}

func (r *Consumer) inBackoff() bool {
	return atomic.LoadInt32(&r.backoffCounter) > 0
}

func (r *Consumer) inBackoffBlock() bool {
	return atomic.LoadInt64(&r.backoffDuration) > 0
}

func (r *Consumer) rdyLoop() {
	var rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	var backoffTimer *time.Timer
	var backoffTimerChan <-chan time.Time
	var backoffCounter int32

	redistributeTicker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-backoffTimerChan:
			var choice *Conn

			backoffTimer = nil
			backoffTimerChan = nil
			atomic.StoreInt64(&r.backoffDuration, 0)

			// pick a random connection to test the waters
			var i int
			conns := r.conns()
			if len(conns) == 0 {
				continue
			}
			idx := rng.Intn(len(conns))
			for _, c := range conns {
				if i == idx {
					choice = c
					break
				}
				i++
			}

			r.log(LogLevelWarning,
				"(%s) backoff timeout expired, sending RDY 1",
				choice.String())
			// while in backoff only ever let 1 message at a time through
			r.updateRDY(choice, 1)
		case c := <-r.rdyChan:
			if backoffTimer != nil || backoffCounter > 0 {
				continue
			}

			// send ready immediately
			remain := c.RDY()
			lastRdyCount := c.LastRDY()
			count := r.perConnMaxInFlight()
			// refill when at 1, or at 25%, or if connections have changed and we have too many RDY
			if remain <= 1 || remain < (lastRdyCount/4) || (count > 0 && count < remain) {
				r.log(LogLevelDebug, "(%s) sending RDY %d (%d remain from last RDY %d)",
					c.String(), count, remain, lastRdyCount)
				r.updateRDY(c, count)
			} else {
				r.log(LogLevelDebug, "(%s) skip sending RDY %d (%d remain out of last RDY %d)",
					c.String(), count, remain, lastRdyCount)
			}
		case success := <-r.backoffChan:
			// prevent many async failures/successes from immediately resulting in
			// max backoff/normal rate (by ensuring that we dont continually incr/decr
			// the counter during a backoff period)
			if backoffTimer != nil {
				continue
			}

			// update backoff state
			backoffUpdated := false
			if success {
				if backoffCounter > 0 {
					backoffCounter--
					backoffUpdated = true
				}
			} else {
				maxBackoffCount := int32(math.Max(1, math.Ceil(
					math.Log2(r.config.MaxBackoffDuration.Seconds()))))
				if backoffCounter < maxBackoffCount {
					backoffCounter++
					backoffUpdated = true
				}
			}

			if backoffUpdated {
				atomic.StoreInt32(&r.backoffCounter, backoffCounter)
			}

			// exit backoff
			if backoffCounter == 0 && backoffUpdated {
				count := r.perConnMaxInFlight()
				r.log(LogLevelWarning, "exiting backoff, returning all to RDY %d", count)
				for _, c := range r.conns() {
					r.updateRDY(c, count)
				}
				continue
			}

			// start or continue backoff
			if backoffCounter > 0 {
				backoffDuration := r.backoffDurationForCount(backoffCounter)
				atomic.StoreInt64(&r.backoffDuration, backoffDuration.Nanoseconds())
				backoffTimer = time.NewTimer(backoffDuration)
				backoffTimerChan = backoffTimer.C

				r.log(LogLevelWarning, "backing off for %.04f seconds (backoff level %d), setting all to RDY 0",
					backoffDuration.Seconds(), backoffCounter)

				// send RDY 0 immediately (to *all* connections)
				for _, c := range r.conns() {
					r.updateRDY(c, 0)
				}
			}
		case <-redistributeTicker.C:
			r.redistributeRDY(rng)
		case <-r.exitChan:
			goto exit
		}
	}

exit:
	redistributeTicker.Stop()
	if backoffTimer != nil {
		backoffTimer.Stop()
	}
	r.log(LogLevelInfo, "rdyLoop exiting")
	r.wg.Done()
}

func (r *Consumer) updateRDY(c *Conn, count int64) error {
	if c.IsClosing() {
		return nil
	}

	// never exceed the nsqd's configured max RDY count
	if count > c.MaxRDY() {
		count = c.MaxRDY()
	}

	// stop any pending retry of an old RDY update
	r.rdyRetryMtx.Lock()
	if timer, ok := r.rdyRetryTimers[c.String()]; ok {
		timer.Stop()
		delete(r.rdyRetryTimers, c.String())
	}
	r.rdyRetryMtx.Unlock()

	// never exceed our global max in flight. truncate if possible.
	// this could help a new connection get partial max-in-flight
	rdyCount := c.RDY()
	maxPossibleRdy := int64(r.getMaxInFlight()) - atomic.LoadInt64(&r.totalRdyCount) + rdyCount
	if maxPossibleRdy > 0 && maxPossibleRdy < count {
		count = maxPossibleRdy
	}
	if maxPossibleRdy <= 0 && count > 0 {
		if rdyCount == 0 {
			// we wanted to exit a zero RDY count but we couldn't send it...
			// in order to prevent eternal starvation we reschedule this attempt
			// (if any other RDY update succeeds this timer will be stopped)
			r.rdyRetryMtx.Lock()
			r.rdyRetryTimers[c.String()] = time.AfterFunc(5*time.Second,
				func() {
					r.updateRDY(c, count)
				})
			r.rdyRetryMtx.Unlock()
		}
		return ErrOverMaxInFlight
	}

	return r.sendRDY(c, count)
}

func (r *Consumer) sendRDY(c *Conn, count int64) error {
	if count == 0 && c.LastRDY() == 0 {
		// no need to send. It's already that RDY count
		return nil
	}

	atomic.AddInt64(&r.totalRdyCount, -c.RDY()+count)
	c.SetRDY(count)
	err := c.WriteCommand(Ready(int(count)))
	if err != nil {
		r.log(LogLevelError, "(%s) error sending RDY %d - %s", c.String(), count, err)
		return err
	}
	return nil
}

func (r *Consumer) redistributeRDY(rng *rand.Rand) {
	if r.inBackoffBlock() {
		return
	}

	numConns := int32(len(r.conns()))
	maxInFlight := r.getMaxInFlight()
	if numConns > maxInFlight {
		r.log(LogLevelDebug, "redistributing RDY state (%d conns > %d max_in_flight)",
			numConns, maxInFlight)
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if r.inBackoff() && numConns > 1 {
		r.log(LogLevelDebug, "redistributing RDY state (in backoff and %d conns > 1)", numConns)
		atomic.StoreInt32(&r.needRDYRedistributed, 1)
	}

	if !atomic.CompareAndSwapInt32(&r.needRDYRedistributed, 1, 0) {
		return
	}

	conns := r.conns()
	possibleConns := make([]*Conn, 0, len(conns))
	for _, c := range conns {
		lastMsgDuration := time.Now().Sub(c.LastMessageTime())
		rdyCount := c.RDY()
		r.log(LogLevelDebug, "(%s) rdy: %d (last message received %s)",
			c.String(), rdyCount, lastMsgDuration)
		if rdyCount > 0 && lastMsgDuration > r.config.LowRdyIdleTimeout {
			r.log(LogLevelDebug, "(%s) idle connection, giving up RDY", c.String())
			r.updateRDY(c, 0)
		}
		possibleConns = append(possibleConns, c)
	}

	availableMaxInFlight := int64(maxInFlight) - atomic.LoadInt64(&r.totalRdyCount)
	if r.inBackoff() {
		availableMaxInFlight = 1 - atomic.LoadInt64(&r.totalRdyCount)
	}

	for len(possibleConns) > 0 && availableMaxInFlight > 0 {
		availableMaxInFlight--
		i := rng.Int() % len(possibleConns)
		c := possibleConns[i]
		// delete
		possibleConns = append(possibleConns[:i], possibleConns[i+1:]...)
		r.log(LogLevelDebug, "(%s) redistributing RDY", c.String())
		r.updateRDY(c, 1)
	}
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (r *Consumer) Stop() {
	if !atomic.CompareAndSwapInt32(&r.stopFlag, 0, 1) {
		return
	}

	r.log(LogLevelInfo, "stopping...")

	if len(r.conns()) == 0 {
		r.stopHandlers()
	} else {
		for _, c := range r.conns() {
			err := c.WriteCommand(StartClose())
			if err != nil {
				r.log(LogLevelError, "(%s) error sending CLS - %s", c.String(), err)
			}
		}

		time.AfterFunc(time.Second*30, func() {
			r.stopHandlers()
		})
	}
}

func (r *Consumer) stopHandlers() {
	r.stopHandler.Do(func() {
		r.log(LogLevelInfo, "stopping handlers")
		close(r.incomingMessages)
	})
}

// AddHandler sets the Handler for messages received by this Consumer. This can be called
// multiple times to add additional handlers. Handler will have a 1:1 ratio to message handling goroutines.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddHandler(handler Handler) {
	r.AddConcurrentHandlers(handler, 1)
}

// AddConcurrentHandlers sets the Handler for messages received by this Consumer.  It
// takes a second argument which indicates the number of goroutines to spawn for
// message handling.
//
// This panics if called after connecting to NSQD or NSQ Lookupd
//
// (see Handler or HandlerFunc for details on implementing this interface)
func (r *Consumer) AddConcurrentHandlers(handler Handler, concurrency int) {
	if atomic.LoadInt32(&r.connectedFlag) == 1 {
		panic("already connected")
	}

	atomic.AddInt32(&r.runningHandlers, int32(concurrency))
	for i := 0; i < concurrency; i++ {
		go r.handlerLoop(handler)
	}
}

func (r *Consumer) handlerLoop(handler Handler) {
	r.log(LogLevelDebug, "starting Handler")

	for {
		message, ok := <-r.incomingMessages
		if !ok {
			goto exit
		}

		if r.shouldFailMessage(message, handler) {
			message.Finish()
			continue
		}

		err := handler.HandleMessage(message)
		if err != nil {
			r.log(LogLevelError, "Handler returned error (%s) for msg %s", err, message.ID)
			if !message.IsAutoResponseDisabled() {
				message.Requeue(-1)
			}
			continue
		}

		if !message.IsAutoResponseDisabled() {
			message.Finish()
		}
	}

exit:
	r.log(LogLevelDebug, "stopping Handler")
	if atomic.AddInt32(&r.runningHandlers, -1) == 0 {
		r.exit()
	}
}

func (r *Consumer) shouldFailMessage(message *Message, handler interface{}) bool {
	// message passed the max number of attempts
	if r.config.MaxAttempts > 0 && message.Attempts > r.config.MaxAttempts {
		r.log(LogLevelWarning, "msg %s attempted %d times, giving up",
			message.ID, message.Attempts)

		logger, ok := handler.(FailedMessageLogger)
		if ok {
			logger.LogFailedMessage(message)
		}

		return true
	}
	return false
}

func (r *Consumer) exit() {
	close(r.exitChan)
	r.wg.Wait()
	close(r.StopChan)
}

func (r *Consumer) log(lvl LogLevel, line string, args ...interface{}) {
	if r.logger == nil {
		return
	}

	if r.logLvl > lvl {
		return
	}

	r.logger.Output(2, fmt.Sprintf("%-4s %3d [%s/%s] %s",
		logPrefix(lvl), r.id, r.topic, r.channel,
		fmt.Sprintf(line, args...)))
}
