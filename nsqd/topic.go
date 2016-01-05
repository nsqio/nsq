package nsqd

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)

const (
	MAX_TOPIC_PARTITION = 1023
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64

	sync.RWMutex

	tname             string
	fullName          string
	partition         int
	channelMap        map[string]*Channel
	backend           BackendQueueWriter
	exitChan          chan int
	channelUpdateChan chan []*Channel
	flushChan         chan int
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32

	ephemeral      bool
	deleteCallback func(*Topic)
	deleter        sync.Once

	ctx         *context
	msgIDCursor uint64
	needFlush   int32
}

func GetTopicFullName(topic string, part int) string {
	return topic + "-" + strconv.Itoa(part)
}

// Topic constructor
func NewTopic(topicName string, part int, ctx *context, deleteCallback func(*Topic)) *Topic {
	if part > MAX_TOPIC_PARTITION {
		return nil
	}
	t := &Topic{
		tname:             topicName,
		partition:         part,
		channelMap:        make(map[string]*Channel),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan []*Channel),
		flushChan:         make(chan int, 10),
		ctx:               ctx,
		deleteCallback:    deleteCallback,
	}

	t.fullName = GetTopicFullName(t.tname, t.partition)

	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueueWriter()
	} else {
		backendName := getBackendName(t.tname, t.partition)
		t.backend = newDiskQueueWriter(backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout)
	}

	t.waitGroup.Wrap(func() { t.messagePump() })

	t.ctx.nsqd.Notify(t)

	return t
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

func (t *Topic) NextMsgID() MessageID {
	// TODO: read latest logid and incr. combine the partition id at high.
	id := atomic.AddUint64(&t.msgIDCursor, 1)
	return MessageID(id)
}

func (t *Topic) GetFullName() string {
	return t.fullName
}

func (t *Topic) GetTopicName() string {
	return t.tname
}

func (t *Topic) GetTopicPart() int {
	return t.partition
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		t.NotifyReloadChannels()
	}

	return channel
}

func (t *Topic) NotifyReloadChannels() {
	t.RLock()

	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return
	}
	chans := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.RUnlock()

	select {
	case t.channelUpdateChan <- chans:
	case <-t.exitChan:
	}
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.GetTopicName(), t.GetTopicPart(), channelName, t.ctx, deleteCallback)
		channel.UpdateQueueEnd(t.backend.GetQueueReadEnd())
		t.channelMap[channelName] = channel
		nsqLog.Logf("TOPIC(%s): new channel(%s), end: %v", t.GetFullName(),
			channel.name, channel.backend.Depth())
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	chans := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.Unlock()

	nsqLog.Logf("TOPIC(%s): deleting channel %s", t.GetFullName(), channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	// update messagePump state
	select {
	case t.channelUpdateChan <- chans:
	case <-t.exitChan:
	}

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	for _, m := range msgs {
		err := t.put(m)
		if err != nil {
			return err
		}
	}
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

func (t *Topic) put(m *Message) error {
	b := bufferPoolGet()
	_, err := writeMessageToBackend(b, m, t.backend)
	bufferPoolPut(b)
	t.ctx.nsqd.SetHealth(err)
	atomic.StoreInt32(&t.needFlush, 1)
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write message to backend - %s",
			t.GetFullName(), err)
		return err
	}
	select {
	case t.flushChan <- 1:
	default:
	}
	return nil
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
// Note: never use Lock here.
func (t *Topic) messagePump() {
	var err error
	var chans []*Channel
	flushCnt := 0

	var lastEnd BackendQueueEnd

	for {
		select {
		case chans = <-t.channelUpdateChan:
			lastEnd = nil
		case flag := <-t.flushChan:
			flushCnt++
			if flag > 1 {
				flushCnt = 10
			}
			if flushCnt >= 10 {
				flushCnt = 0
				t.flush()
			}
		case <-t.exitChan:
			goto exit
		}

		e := t.backend.GetQueueReadEnd()
		if lastEnd != nil && lastEnd.IsSame(e) {
			continue
		}
		lastEnd = e
		for _, channel := range chans {
			err = channel.UpdateQueueEnd(e)
			if err != nil {
				nsqLog.LogErrorf(
					"TOPIC(%s) : failed to update topic end to channel(%s) - %s",
					t.GetFullName(), channel.name, err)
			}
		}
	}

exit:
	nsqLog.Logf("TOPIC(%s): closing ... messagePump", t.GetFullName())
}

func (t *Topic) totalSize() int64 {
	e := t.backend.GetQueueReadEnd()
	return int64(e.GetOffset())
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	t.Lock()
	defer t.Unlock()
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		nsqLog.Logf("TOPIC(%s): deleting", t.GetFullName())

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.ctx.nsqd.Notify(t)
	} else {
		nsqLog.Logf("TOPIC(%s): closing", t.GetFullName())
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	t.waitGroup.Wait()

	if deleted {
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		// empty the queue (deletes the backend files, too)
		t.empty()
		return t.backend.Delete()
	}

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			nsqLog.Logf(" channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

func (t *Topic) empty() error {
	return t.backend.Empty()
}

func (t *Topic) ForceFlush() {
	select {
	case t.flushChan <- 2:
	default:
	}
}

func (t *Topic) flush() error {
	ok := atomic.CompareAndSwapInt32(&t.needFlush, 1, 0)
	if !ok {
		return nil
	}
	err := t.backend.Flush()
	if err != nil {
		nsqLog.LogErrorf("failed flush: %v", err)
	}
	return err
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	for _, c := range t.channelMap {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}
