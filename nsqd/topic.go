package nsqd

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/nsqio/nsq/internal/quantile"
)

const (
	MAX_TOPIC_PARTITION = 1023
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64

	sync.RWMutex

	tname      string
	fullName   string
	partition  int
	channelMap map[string]*Channel
	backend    BackendQueueWriter
	flushChan  chan int
	exitFlag   int32

	ephemeral      bool
	deleteCallback func(*Topic)
	deleter        sync.Once

	ctx            *context
	msgIDCursor    uint64
	needFlush      int32
	needNotifyChan bool
	enableTrace    bool
	syncEvery      int64
	putBuffer      bytes.Buffer
	bp             sync.Pool
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
		tname:          topicName,
		partition:      part,
		channelMap:     make(map[string]*Channel),
		flushChan:      make(chan int, 10),
		ctx:            ctx,
		deleteCallback: deleteCallback,
		syncEvery:      ctx.nsqd.getOpts().SyncEvery,
		putBuffer:      bytes.Buffer{},
	}
	if t.syncEvery < 1 {
		t.syncEvery = 1
	}
	t.bp.New = func() interface{} {
		return &bytes.Buffer{}
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

	t.ctx.nsqd.Notify(t)

	return t
}

func (t *Topic) bufferPoolGet(capacity int) *bytes.Buffer {
	b := t.bp.Get().(*bytes.Buffer)
	b.Reset()
	b.Grow(capacity)
	return b
}

func (t *Topic) bufferPoolPut(b *bytes.Buffer) {
	t.bp.Put(b)
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

func (t *Topic) nextMsgID() MessageID {
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
}

func (t *Topic) GetTopicChannelStat(channelName string) string {
	statStr := ""
	t.Lock()
	for n, channel := range t.channelMap {
		if channelName == "" || channelName == n {
			statStr += channel.GetChannelStats()
		}
	}
	t.Unlock()
	return statStr
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

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	t.Lock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		t.Unlock()
		return errors.New("exiting")
	}

	err := t.put(m)
	t.Unlock()
	if err != nil {
		return err
	}
	cnt := atomic.AddUint64(&t.messageCount, 1)
	if cnt%uint64(t.syncEvery) == 0 {
		t.Lock()
		t.flush(false)
		t.Unlock()
	}
	return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.Lock()
	defer t.Unlock()
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

	if int64(len(msgs)) >= t.syncEvery {
		t.flush(false)
	}
	return nil
}

func (t *Topic) put(m *Message) error {
	m.ID = t.nextMsgID()
	_, err := writeMessageToBackend(&t.putBuffer, m, t.backend)
	t.ctx.nsqd.SetHealth(err)
	atomic.StoreInt32(&t.needFlush, 1)
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write message to backend - %s",
			t.GetFullName(), err)
		return err
	}

	if t.enableTrace {
		nsqLog.Logf("[TRACE] message %v put in topic: %v", m.GetFullMsgID(),
			t.GetFullName())
	}
	return nil
}

func updateChannelsEnd(chans map[string]*Channel, e BackendQueueEnd) {
	if e == nil {
		return
	}
	for _, channel := range chans {
		err := channel.UpdateQueueEnd(e)
		if err != nil {
			nsqLog.LogErrorf(
				"failed to update topic end to channel(%s) - %s",
				channel.name, err)
		}
	}
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

	if deleted {
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		// empty the queue (deletes the backend files, too)
		t.empty()
		return t.backend.Delete()
	}

	// write anything leftover to disk
	t.flush(true)
	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			nsqLog.Logf(" channel(%s) close - %s", channel.name, err)
		}
	}

	return t.backend.Close()
}

func (t *Topic) empty() error {
	nsqLog.Logf("TOPIC(%s): empty", t.GetFullName())
	return t.backend.Empty()
}

func (t *Topic) ForceFlush() {
	t.Lock()
	t.flush(true)
	t.Unlock()
}

func (t *Topic) flush(notifyChan bool) error {
	ok := atomic.CompareAndSwapInt32(&t.needFlush, 1, 0)
	if !ok {
		if notifyChan && t.needNotifyChan {
			t.needNotifyChan = false
			e := t.backend.GetQueueReadEnd()
			updateChannelsEnd(t.channelMap, e)
		}
		return nil
	}
	err := t.backend.Flush()
	if err != nil {
		nsqLog.LogErrorf("failed flush: %v", err)
		return err
	}
	if notifyChan {
		t.needNotifyChan = false
		e := t.backend.GetQueueReadEnd()
		updateChannelsEnd(t.channelMap, e)
	} else {
		t.needNotifyChan = true
	}
	return err
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
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
