package nsqd

import (
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/quantile"
)

var (
	ErrMsgNotInFlight     = errors.New("Message ID not in flight")
	ErrMsgAlreadyInFlight = errors.New("Message ID already in flight")
)

type Consumer interface {
	UnPause()
	Pause()
	TimedOutMessage()
	Stats() ClientStats
	Exit()
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName  string
	topicPart  int
	name       string
	notifyCall func(v interface{})
	option     *Options

	backend BackendQueueReader

	requeuedMsgChan     chan *Message
	waitingRequeueMsgs  map[MessageID]*Message
	waitingRequeueMutex sync.Mutex
	clientMsgChan       chan *Message
	exitChan            chan int
	exitSyncChan        chan bool
	exitFlag            int32
	exitMutex           sync.RWMutex

	// state tracking
	clients        map[int64]Consumer
	paused         int32
	ephemeral      bool
	deleteCallback func(*Channel)
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex

	currentLastConfirmed BackendOffset
	confirmedMsgs        map[BackendOffset]*Message
	confirmMutex         sync.Mutex
	waitingConfirm       int32
	tryReadBackend       chan bool
	needNotifyRead       int32
	consumeDisabled      int32
	// stat counters
	EnableTrace bool
	//finMsgs     map[MessageID]*Message
	//finErrMsgs map[MessageID]string
	requireOrder bool
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, part int, channelName string, opt *Options,
	deleteCallback func(*Channel), consumeDisabled int32, notify func(v interface{})) *Channel {

	c := &Channel{
		topicName:          topicName,
		topicPart:          part,
		name:               channelName,
		requeuedMsgChan:    make(chan *Message, opt.MaxRdyCount+1),
		waitingRequeueMsgs: make(map[MessageID]*Message, 100),
		clientMsgChan:      make(chan *Message),
		exitChan:           make(chan int),
		exitSyncChan:       make(chan bool),
		clients:            make(map[int64]Consumer),
		confirmedMsgs:      make(map[BackendOffset]*Message),
		//finMsgs:            make(map[MessageID]*Message),
		//finErrMsgs:     make(map[MessageID]string),
		tryReadBackend:  make(chan bool, 1),
		deleteCallback:  deleteCallback,
		option:          opt,
		notifyCall:      notify,
		consumeDisabled: consumeDisabled,
	}
	if len(opt.E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			opt.E2EProcessingLatencyWindowTime,
			opt.E2EProcessingLatencyPercentiles,
		)
	}
	// channel no need sync so much.
	syncEvery := opt.SyncEvery * 1000
	if syncEvery < 1 {
		syncEvery = 1
	}

	c.initPQ()

	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueueReader()
	} else {
		// backend names, for uniqueness, automatically include the topic...
		backendReaderName := getBackendReaderName(c.topicName, c.topicPart, channelName)
		backendName := getBackendName(c.topicName, c.topicPart)
		c.backend = newDiskQueueReader(backendName, backendReaderName,
			path.Join(opt.DataPath, c.topicName),
			opt.MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(opt.MaxMsgSize)+minValidMsgLength,
			syncEvery,
			opt.SyncTimeout,
			false)

		c.currentLastConfirmed = c.backend.(*diskQueueReader).virtualConfirmedOffset
	}

	go c.messagePump()

	c.notifyCall(c)

	return c
}

func (c *Channel) GetName() string {
	return c.name
}

func (c *Channel) GetTopicName() string {
	return c.topicName
}

func (c *Channel) GetTopicPart() int {
	return c.topicPart
}

func (c *Channel) GetClientMsgChan() chan *Message {
	return c.clientMsgChan
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.option.MemQueueSize)/10))

	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageID]*Message, pqSize)
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) IsWaitingMoreData() bool {
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		return atomic.LoadInt32(&d.waitingMoreData) == 1
	}
	return false
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return ErrExiting
	}

	if deleted {
		nsqLog.Logf("CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.notifyCall(c)
	} else {
		nsqLog.Logf("CHANNEL(%s): closing", c.name)
	}

	// this forceably closes clients, client will be removed by client before the
	// client read loop exit.
	c.RLock()
	for _, client := range c.clients {
		client.Exit()
	}
	c.RUnlock()

	close(c.exitChan)
	<-c.exitSyncChan

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

func (c *Channel) GetClients() map[int64]Consumer {
	return c.clients
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}

	clientMsgChan := c.clientMsgChan
	for {
		select {
		case _, ok := <-clientMsgChan:
			if !ok {
				// c.clientMsgChan may be closed while in this loop
				// so just remove it from the select so we can make progress
				clientMsgChan = nil
			}
		case <-c.requeuedMsgChan:
		default:
			goto finish
		}
	}

finish:
	c.waitingRequeueMutex.Lock()
	for k, _ := range c.waitingRequeueMsgs {
		delete(c.waitingRequeueMsgs, k)
	}
	c.waitingRequeueMutex.Unlock()
	return nil
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight because it is only called in Close()
func (c *Channel) flush() error {

	if len(c.requeuedMsgChan) > 0 || len(c.inFlightMessages) > 0 {
		nsqLog.Logf("CHANNEL(%s): flushing %d requeued %d in-flight messages to backend",
			c.name, len(c.requeuedMsgChan), len(c.inFlightMessages))
	}
	d, ok := c.backend.(*diskQueueReader)
	if ok {
		d.Flush()
	}
	return nil
}

func (c *Channel) Depth() int64 {
	return c.backend.Depth()
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// When topic message is put, update the new end of the queue
func (c *Channel) UpdateQueueEnd(end BackendQueueEnd) error {
	if end == nil {
		return nil
	}
	return c.backend.UpdateQueueEnd(end)
}

// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		nsqLog.Logf("failed while touch: %v, msg not exist", id)
		return ErrMsgNotInFlight
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return fmt.Errorf("client does not own message : %v vs %v",
			msg.clientID, clientID)
	}
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.option.MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.option.MaxMsgTimeout)
	}
	msg.pri = newTimeout.UnixNano()
	c.inFlightMutex.Unlock()
	return nil
}

func (c *Channel) ConfirmBackendQueueOnSlave(offset BackendOffset) error {
	// TODO: confirm on slave may exceed the current end, because the buffered write
	// may need to be flushed on slave.
	c.confirmMutex.Lock()
	if len(c.confirmedMsgs) != 0 {
		nsqLog.LogWarningf("should empty confirmed queue on slave.")
	}
	var err error
	if offset < c.currentLastConfirmed {
		if nsqLog.Level() > levellogger.LOG_DEBUG {
			nsqLog.LogDebugf("confirm offset less than current: %v, %v", offset, c.currentLastConfirmed)
		}
	} else {
		err = c.backend.SkipReadToOffset(offset)
		if err != nil {
			if !c.Exiting() {
				nsqLog.Logf("confirm read failed: %v, offset: %v", err, offset)
			}
		} else {
			c.currentLastConfirmed = offset
		}
	}
	c.confirmMutex.Unlock()
	return err
}

// in order not to make the confirm map too large,
// we need handle this case: a old message is not confirmed,
// and we keep all the newer confirmed messages so we can confirm later.
// indicated weather the confirmed offset is changed
func (c *Channel) ConfirmBackendQueue(msg *Message) (BackendOffset, bool) {
	c.confirmMutex.Lock()
	defer c.confirmMutex.Unlock()
	//c.finMsgs[msg.ID] = msg
	if msg.offset < c.currentLastConfirmed {
		nsqLog.LogDebugf("confirmed msg is less than current confirmed offset: %v-%v, %v", msg.ID, msg.offset, c.currentLastConfirmed)
		return c.currentLastConfirmed, false
	}
	c.confirmedMsgs[msg.offset] = msg
	reduced := false
	for {
		if m, ok := c.confirmedMsgs[c.currentLastConfirmed]; ok {
			nsqLog.LogDebugf("move confirm: %v to %v, msg: %v",
				c.currentLastConfirmed, c.currentLastConfirmed+m.rawMoveSize, m.ID)
			c.currentLastConfirmed += m.rawMoveSize
			delete(c.confirmedMsgs, m.offset)
			reduced = true
		} else {
			break
		}
	}
	if reduced {
		err := c.backend.ConfirmRead(c.currentLastConfirmed)
		if err != nil {
			if !c.Exiting() {
				nsqLog.LogErrorf("confirm read failed: %v, msg: %v", err, msg)
			}
			return c.currentLastConfirmed, reduced
		}
		if int64(len(c.confirmedMsgs)) < c.option.MaxConfirmWin/2 && atomic.LoadInt32(&c.needNotifyRead) == 1 {
			select {
			case c.tryReadBackend <- true:
			default:
			}
		}
	}
	if int64(len(c.confirmedMsgs)) > c.option.MaxConfirmWin {
		if c.EnableTrace || nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.LogDebugf("lots of confirmed messages : %v, %v",
				len(c.confirmedMsgs), c.currentLastConfirmed)

			//found the message in the flight with offset c.currentLastConfirmed and
			//requeue to client again. This can force the missed message with
			//c.currentLastConfirmed offset
			c.inFlightMutex.Lock()
			var reqMsg *Message
			for _, msg := range c.inFlightMessages {
				if msg.offset == c.currentLastConfirmed {
					nsqLog.Logf("client %v message %v confirm offset %v wait too long %v ",
						msg.clientID, msg.ID, msg.offset, msg.pri-time.Now().UnixNano())
					//delete(c.inFlightMessages, msg.ID)
					//if msg.index != -1 {
					//	c.inFlightPQ.Remove(msg.index)
					//}
					reqMsg = msg
					break
				}
			}
			c.inFlightMutex.Unlock()
			if reqMsg != nil {
				//nsqLog.Logf("client %v message %v confirm offset %v requeued",
				//	reqMsg.clientID, reqMsg.ID, reqMsg.offset)
				//c.doRequeue(reqMsg)
			} else {
				nsqLog.LogWarningf("waiting confirm message offset %v not in inflight, current requeue: %v", c.currentLastConfirmed, len(c.requeuedMsgChan))
			}
		}
	}
	atomic.StoreInt32(&c.waitingConfirm, int32(len(c.confirmedMsgs)))
	return c.currentLastConfirmed, reduced
	// TODO: if some messages lost while re-queue, it may happen that some messages not
	// in inflight queue and also not wait confirm. In this way, we need reset
	// backend queue to force read the data from disk again.
}

func (c *Channel) IsConfirmed(msg *Message) bool {
	c.confirmMutex.Lock()
	//c.finMsgs[msg.ID] = msg
	_, ok := c.confirmedMsgs[msg.offset]
	c.confirmMutex.Unlock()
	return ok
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) (BackendOffset, bool, error) {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		nsqLog.LogWarningf("message %v fin error: %v from client %v", id, err,
			clientID)
		return 0, false, err
	}
	if c.EnableTrace {
		nsqLog.Logf("[TRACE] message %v, offset:%v, finished from client %v",
			msg.GetFullMsgID(), msg.offset, clientID)
	} else if nsqLog.Level() >= levellogger.LOG_DEBUG {
		nsqLog.Logf("message %v, offset:%v, finished from client %v",
			msg.ID, msg.offset, clientID)
	}
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	offset, changed := c.ConfirmBackendQueue(msg)
	if msg.notifyContinue != nil {
		select {
		case msg.notifyContinue <- 1:
		case <-c.exitChan:
		}
	}
	return offset, changed, nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message
//
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	if timeout == 0 {
		// remove from inflight first
		msg, err := c.popInFlightMessage(clientID, id)
		if err != nil {
			return err
		}
		return c.doRequeue(msg)
	}
	return nil
}

func (c *Channel) RequeueClientMessages(clientID int64) {
	if c.Exiting() {
		return
	}
	if atomic.LoadInt32(&c.consumeDisabled) == 1 {
		return
	}
	idList := make([]MessageID, 0)
	c.inFlightMutex.Lock()
	for id, msg := range c.inFlightMessages {
		if msg.clientID == clientID {
			idList = append(idList, id)
		}
	}
	c.inFlightMutex.Unlock()
	for _, id := range idList {
		c.RequeueMessage(clientID, id, 0)
	}
	if len(idList) > 0 {
		nsqLog.Logf("client: %v requeued %v messages ",
			clientID, len(idList))
	}
}

// AddClient adds a client to the Channel's client list
func (c *Channel) AddClient(clientID int64, client Consumer) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return
	}
	c.clients[clientID] = client
}

// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		nsqLog.LogWarningf("push message in flight failed: %v, %v", err,
			msg.GetFullMsgID())
		return err
	}
	if c.EnableTrace {
		nsqLog.Logf("[TRACE] message %v sending to client %v in flight", msg.GetFullMsgID(), clientID)
	} else if nsqLog.Level() > 1 {
		nsqLog.LogDebugf("message %v sending to client %v in flight, offset: %v", msg.ID, clientID, msg.offset)
	}
	return nil
}

func (c *Channel) GetInflightNum() int {
	c.inFlightMutex.Lock()
	n := len(c.inFlightMessages)
	c.inFlightMutex.Unlock()
	return n
}

func (c *Channel) GetConfirmedOffset() BackendOffset {
	c.confirmMutex.Lock()
	tmp := c.currentLastConfirmed
	c.confirmMutex.Unlock()
	return tmp
}

func (c *Channel) GetChannelEnd() BackendOffset {
	return c.backend.GetQueueReadEnd().GetOffset()
}

// doRequeue performs the low level operations to requeue a message
func (c *Channel) doRequeue(m *Message) error {
	if c.Exiting() {
		return ErrExiting
	}
	select {
	case <-c.exitChan:
		nsqLog.LogDebugf("requeue message failed for existing: %v ", m.ID)
		return ErrExiting
	case c.requeuedMsgChan <- m:
	default:
		c.waitingRequeueMutex.Lock()
		c.waitingRequeueMsgs[m.ID] = m
		c.waitingRequeueMutex.Unlock()
	}
	atomic.AddUint64(&c.requeueCount, 1)
	if m.notifyContinue != nil {
		select {
		case m.notifyContinue <- 1:
		case <-c.exitChan:
		}
	}
	if c.EnableTrace {
		nsqLog.Logf("[TRACE] message %v requeued.", m.GetFullMsgID())
	} else if nsqLog.Level() > 1 {
		nsqLog.Logf("message %v requeued from client %v, offset: %v", m.ID, m.clientID, m.offset)
	}
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return ErrMsgAlreadyInFlight
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, ErrMsgNotInFlight
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, fmt.Errorf("client does not own message : %v vs %v",
			msg.clientID, clientID)
	}
	delete(c.inFlightMessages, id)
	if msg.index != -1 {
		c.inFlightPQ.Remove(msg.index)
	}
	c.inFlightMutex.Unlock()
	return msg, nil
}

func (c *Channel) IsConsumeDisabled() bool {
	return atomic.LoadInt32(&c.consumeDisabled) == 1
}

func (c *Channel) DisableConsume(disable bool) {
	c.Lock()
	defer c.Unlock()
	if disable {
		if !atomic.CompareAndSwapInt32(&c.consumeDisabled, 0, 1) {
			return
		}
		nsqLog.Logf("channel %v disabled for consume", c.name)
		for cid, client := range c.clients {
			client.Exit()
			delete(c.clients, cid)
		}
		c.initPQ()
		c.waitingRequeueMutex.Lock()
		for k, _ := range c.waitingRequeueMsgs {
			delete(c.waitingRequeueMsgs, k)
		}
		c.waitingRequeueMutex.Unlock()
		c.confirmMutex.Lock()
		c.confirmedMsgs = make(map[BackendOffset]*Message)
		c.confirmMutex.Unlock()

		done := false
		for !done {
			select {
			case m := <-c.clientMsgChan:
				nsqLog.Logf("ignored a read message %v at offset %v while disable consume", m.ID, m.offset)
			case <-c.requeuedMsgChan:
			default:
				done = true
			}
		}
	} else {
		nsqLog.Logf("channel %v enabled for consume", c.name)
		// we need reset backend read position to confirm position
		// since we dropped all inflight and requeue data while disable consume.
		atomic.StoreInt32(&c.consumeDisabled, 0)

		done := false
		for !done {
			select {
			case m := <-c.clientMsgChan:
				nsqLog.Logf("ignored a read message %v at offset %v while enable consume", m.ID, m.offset)
			case <-c.requeuedMsgChan:
			default:
				done = true
			}
		}

		c.resetReaderToConfirmed()
		select {
		case c.tryReadBackend <- true:
		default:
		}
	}
	c.notifyCall(c)
}

func (c *Channel) resetReaderToConfirmed() {
	offset := c.GetConfirmedOffset()
	c.backend.SkipReadToOffset(offset)
	d, ok := c.backend.(*diskQueueReader)
	vc := int64(0)
	if ok {
		vc = int64(d.virtualConfirmedOffset)
	}
	nsqLog.Logf("reset channel %v reader confirm: %v, disk queue: %v", c.name, offset, vc)
}

// messagePump reads messages from either memory or backend and sends
// messages to clients over a go chan
func (c *Channel) messagePump() {
	var msg *Message
	var data ReadResult
	var err error
	var lastMsg Message
	isSkipped := false
	var readChan <-chan ReadResult
	maxWin := int32(c.option.MaxConfirmWin)
	continuNotifyChan := make(chan int, 1)
	resumedFirst := true

LOOP:
	for {
		// do an extra check for closed exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is draining clientMsgChan into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&c.exitFlag) == 1 {
			goto exit
		}

		if atomic.LoadInt32(&c.waitingConfirm) > maxWin ||
			len(c.requeuedMsgChan) > 0 {
			atomic.StoreInt32(&c.needNotifyRead, 1)
			readChan = nil
		} else {
			readChan = c.backend.ReadChan()
		}

		if atomic.LoadInt32(&c.consumeDisabled) != 0 {
			readChan = nil
			nsqLog.Logf("channel consume is disabled : %v", c.name)
		}

		if readChan == nil {
			nsqLog.LogDebugf("channel reader is holding: %v, %v",
				atomic.LoadInt32(&c.waitingConfirm),
				c.name)
		}

		select {
		case msg = <-c.requeuedMsgChan:
		case data = <-readChan:
			if data.err != nil {
				nsqLog.LogErrorf("failed to read message - %s", data.err)
				// TODO: fix corrupt file from other replica.
				// and should handle the confirm offset, since some skipped data
				// may never be confirmed any more
				c.backend.(*diskQueueReader).SkipToNext()
				isSkipped = true
				time.Sleep(time.Millisecond * 100)
				continue LOOP
			}
			msg, err = decodeMessage(data.data)
			if err != nil {
				nsqLog.LogErrorf("failed to decode message - %s - %v", err, data)
				continue LOOP
			}
			msg.offset = data.offset
			msg.rawMoveSize = data.movedSize
			if isSkipped {
				// TODO: store the skipped info to retry error if possible.
				nsqLog.LogWarningf("skipped message from %v to the : %v", lastMsg, *msg)
			}
			if resumedFirst {
				nsqLog.Logf("resumed first messsage offset: %v", msg.offset)
				resumedFirst = false
			}
			isSkipped = false
			lastMsg = *msg
		case <-c.exitChan:
			goto exit
		case <-c.tryReadBackend:
			atomic.StoreInt32(&c.needNotifyRead, 0)
			resumedFirst = true
			continue LOOP
		}

		if msg == nil {
			continue
		}
		if atomic.LoadInt32(&c.consumeDisabled) != 0 {
			continue
		}
		if c.requireOrder {
			msg.notifyContinue = continuNotifyChan
		} else {
			msg.notifyContinue = nil
		}
		msg.Attempts++
		select {
		case c.clientMsgChan <- msg:
		case <-c.exitChan:
			goto exit
		}
		if c.requireOrder {
			select {
			case <-msg.notifyContinue:
			case <-c.exitChan:
				goto exit
			}
		}
		msg = nil
		// the client will call back to mark as in-flight w/ its info
	}

exit:
	nsqLog.Logf("CHANNEL(%s): closing ... messagePump", c.name)
	close(c.clientMsgChan)
	close(c.exitSyncChan)
}

func (c *Channel) GetChannelDebugStats() string {
	debugStr := ""
	c.inFlightMutex.Lock()
	inFlightCount := len(c.inFlightMessages)
	debugStr += fmt.Sprintf("inflight %v messages : ", inFlightCount)
	for _, msg := range c.inFlightMessages {
		debugStr += fmt.Sprintf("%v(%v),", msg.ID, msg.offset)
	}
	c.inFlightMutex.Unlock()
	debugStr += "\n"
	c.confirmMutex.Lock()
	debugStr += fmt.Sprintf("channel end : %v, current confirm %v, confirmed %v messages: ",
		c.GetChannelEnd(),
		c.currentLastConfirmed, len(c.confirmedMsgs))
	for _, msg := range c.confirmedMsgs {
		debugStr += fmt.Sprintf("%v(%v), ", msg.ID, msg.offset)
	}
	c.confirmMutex.Unlock()
	debugStr += "\n"
	return debugStr
}

func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		if atomic.LoadInt32(&c.consumeDisabled) == 1 {
			goto exit
		}
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		flightCnt := len(c.inFlightMessages)
		if msg == nil {
			if atomic.LoadInt32(&c.waitingConfirm) > 1 || flightCnt > 1 {
				nsqLog.LogDebugf("no timeout, inflight %v, waiting confirm: %v, confirmed: %v",
					flightCnt, atomic.LoadInt32(&c.waitingConfirm),
					c.currentLastConfirmed)
			}
			c.inFlightMutex.Unlock()
			goto exit
		}
		dirty = true

		_, ok := c.inFlightMessages[msg.ID]
		if !ok {
			c.inFlightMutex.Unlock()
			goto exit
		}
		delete(c.inFlightMessages, msg.ID)
		c.inFlightMutex.Unlock()

		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		nsqLog.Logf("message %v, offset: %v timeout, client: %v",
			msg.ID, msg.offset, msg.clientID)
		c.doRequeue(msg)
	}

exit:
	// try requeue the messages that waiting.
	stopScan := false
	c.waitingRequeueMutex.Lock()
	if len(c.waitingRequeueMsgs) > 1 {
		nsqLog.LogDebugf("requeue waiting messages: %v", len(c.waitingRequeueMsgs))
	}
	for k, m := range c.waitingRequeueMsgs {
		select {
		case c.requeuedMsgChan <- m:
			delete(c.waitingRequeueMsgs, k)
		default:
			stopScan = true
		}
		if stopScan {
			break
		}
	}
	c.waitingRequeueMutex.Unlock()
	if atomic.LoadInt32(&c.waitingConfirm) >
		int32(c.option.MaxConfirmWin) {
		// check if lastconfirmed message offset not in inflight
	}

	return dirty
}
