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

var (
	ErrMissingMessageID    = errors.New("missing message id")
	ErrWriteOffsetMismatch = errors.New("write offset mismatch")
	ErrExiting             = errors.New("exiting")
)

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueueWriter) (BackendOffset, int32, int64, error) {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return 0, 0, 0, err
	}
	return bq.Put(buf.Bytes())
}

type MsgIDGenerator interface {
	NextID() uint64
	Reset(uint64)
}

type Topic struct {
	sync.RWMutex

	tname       string
	fullName    string
	partition   int
	channelMap  map[string]*Channel
	channelLock sync.RWMutex
	backend     BackendQueueWriter
	dataPath    string
	flushChan   chan int
	exitFlag    int32

	ephemeral      bool
	deleteCallback func(*Topic)
	deleter        sync.Once

	notifyCall     func(v interface{})
	option         *Options
	MsgIDCursor    MsgIDGenerator
	defaultIDSeq   uint64
	needFlush      int32
	needNotifyChan bool
	EnableTrace    bool
	syncEvery      int64
	putBuffer      bytes.Buffer
	bp             sync.Pool
}

func GetTopicFullName(topic string, part int) string {
	return topic + "-" + strconv.Itoa(part)
}

// Topic constructor
func NewTopic(topicName string, part int, opt *Options, deleteCallback func(*Topic), notify func(v interface{})) *Topic {
	if part > MAX_TOPIC_PARTITION {
		return nil
	}
	t := &Topic{
		tname:          topicName,
		partition:      part,
		channelMap:     make(map[string]*Channel),
		flushChan:      make(chan int, 10),
		option:         opt,
		deleteCallback: deleteCallback,
		syncEvery:      opt.SyncEvery,
		putBuffer:      bytes.Buffer{},
		notifyCall:     notify,
	}
	if t.syncEvery < 1 {
		t.syncEvery = 1
	}
	t.bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
	t.fullName = GetTopicFullName(t.tname, t.partition)

	t.dataPath = opt.DataPath
	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueueWriter()
	} else {
		backendName := getBackendName(t.tname, t.partition)
		t.backend = newDiskQueueWriter(backendName,
			opt.DataPath,
			opt.MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(opt.MaxMsgSize)+minValidMsgLength,
			opt.SyncEvery,
			opt.SyncTimeout)
	}

	t.notifyCall(t)
	nsqLog.LogDebugf("new topic created: %v", t.tname)

	return t
}

func (t *Topic) GetDiskQueueSnapshot() *DiskQueueSnapshot {
	e := t.backend.GetQueueReadEnd().(*diskQueueEndInfo)
	return newDiskQueueSnapshot(getBackendName(t.tname, t.partition), t.dataPath, e)
}

func (t *Topic) BufferPoolGet(capacity int) *bytes.Buffer {
	b := t.bp.Get().(*bytes.Buffer)
	b.Reset()
	b.Grow(capacity)
	return b
}

func (t *Topic) BufferPoolPut(b *bytes.Buffer) {
	t.bp.Put(b)
}

// should be protected by read lock outside
func (t *Topic) GetChannelMap() map[string]*Channel {
	return t.channelMap
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

func (t *Topic) nextMsgID() MessageID {
	// TODO: read latest logid and incr. combine the partition id at high.
	id := uint64(0)
	if t.MsgIDCursor != nil {
		id = t.MsgIDCursor.NextID()
	} else {
		id = atomic.AddUint64(&t.defaultIDSeq, 1)
	}
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
	t.channelLock.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.channelLock.Unlock()

	if isNew {
		// update messagePump state
		t.NotifyReloadChannels()
	}

	return channel
}

func (t *Topic) NotifyReloadChannels() {
}

func (t *Topic) GetTopicChannelDebugStat(channelName string) string {
	statStr := ""
	t.channelLock.RLock()
	for n, channel := range t.channelMap {
		if channelName == "" || channelName == n {
			statStr += channel.GetChannelDebugStats()
		}
	}
	t.channelLock.RUnlock()
	return statStr
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.GetTopicName(), t.GetTopicPart(), channelName,
			t.option, deleteCallback, t.notifyCall)
		channel.UpdateQueueEnd(t.backend.GetQueueReadEnd())
		t.channelMap[channelName] = channel
		nsqLog.Logf("TOPIC(%s): new channel(%s), end: %v", t.GetFullName(),
			channel.name, channel.backend.Depth())
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.channelLock.RLock()
	channel, ok := t.channelMap[channelName]
	t.channelLock.RUnlock()
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.channelLock.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.channelLock.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	chans := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		chans = append(chans, c)
	}
	t.channelLock.Unlock()

	nsqLog.Logf("TOPIC(%s): deleting channel %s", t.GetFullName(), channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

func (t *Topic) RollbackNoLock(vend BackendOffset, diffCnt uint64) error {
	err := t.backend.RollbackWrite(vend, diffCnt)
	if err == nil {
		t.updateChannelsEnd()
	}
	return err
}

func (t *Topic) ResetBackendEndNoLock(vend BackendOffset, totalCnt int64) error {
	err := t.backend.ResetWriteEnd(vend, totalCnt)
	if err == nil {
		t.updateChannelsEnd()
	}

	return err
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) (MessageID, BackendOffset, int32, int64, error) {
	t.Lock()
	id, offset, writeBytes, totalCnt, err := t.PutMessageNoLock(m)
	t.Unlock()
	if err != nil {
		return id, offset, writeBytes, totalCnt, err
	}
	if totalCnt%t.syncEvery == 0 {
		t.Lock()
		t.flush(false)
		t.Unlock()
	}
	return id, offset, writeBytes, totalCnt, nil
}

func (t *Topic) PutMessageNoLock(m *Message) (MessageID, BackendOffset, int32, int64, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		t.Unlock()
		return 0, 0, 0, 0, errors.New("exiting")
	}

	id, offset, writeBytes, totalCnt, err := t.put(m)
	return id, offset, writeBytes, totalCnt, err
}

func (t *Topic) FlushAsNeedNoLock() {
	cnt := t.TotalMessageCnt()
	if cnt%uint64(t.syncEvery) == 0 {
		t.flush(false)
	}
}

func (t *Topic) PutMessageOnReplica(m *Message, offset BackendOffset) error {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return ErrExiting
	}
	wend := t.backend.GetQueueWriteEnd()
	if wend.GetOffset() != offset {
		return ErrWriteOffsetMismatch
	}
	_, _, _, totalCnt, err := t.put(m)
	if err != nil {
		return err
	}
	if totalCnt%t.syncEvery == 0 {
		t.flush(false)
	}
	return nil
}

func (t *Topic) PutMessagesOnReplica(msgs []*Message, offset BackendOffset) error {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return ErrExiting
	}

	wend := t.backend.GetQueueWriteEnd()
	if wend.GetOffset() != offset {
		return ErrWriteOffsetMismatch
	}

	for _, m := range msgs {
		_, _, _, _, err := t.put(m)
		if err != nil {
			return err
		}
	}

	if int64(len(msgs)) >= t.syncEvery {
		t.flush(false)
	}
	return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) (MessageID, BackendOffset, int32, int64, error) {
	t.Lock()
	defer t.Unlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return 0, 0, 0, 0, ErrExiting
	}

	firstOffset := BackendOffset(-1)
	firstMsgID := MessageID(0)
	totalCnt := int64(0)
	batchBytes := int32(0)
	for _, m := range msgs {
		id, offset, bytes, cnt, err := t.put(m)
		if err != nil {
			return firstMsgID, firstOffset, batchBytes, cnt, err
		}
		batchBytes += bytes
		if firstOffset == BackendOffset(-1) {
			firstOffset = offset
			firstMsgID = id
			totalCnt = cnt
		}
	}

	if int64(len(msgs)) >= t.syncEvery {
		t.flush(false)
	}
	return firstMsgID, firstOffset, batchBytes, totalCnt, nil
}

func (t *Topic) put(m *Message) (MessageID, BackendOffset, int32, int64, error) {
	if m.ID <= 0 {
		m.ID = t.nextMsgID()
	}
	offset, writeBytes, totalCnt, err := writeMessageToBackend(&t.putBuffer, m, t.backend)
	atomic.StoreInt32(&t.needFlush, 1)
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write message to backend - %s",
			t.GetFullName(), err)
		return m.ID, offset, writeBytes, totalCnt, err
	}

	if t.EnableTrace {
		nsqLog.Logf("[TRACE] message %v put in topic: %v", m.GetFullMsgID(),
			t.GetFullName())
	}
	return m.ID, offset, writeBytes, totalCnt, nil
}

func (t *Topic) updateChannelsEnd() {
	e := t.backend.GetQueueReadEnd()
	t.channelLock.RLock()
	if e != nil {
		for _, channel := range t.channelMap {
			err := channel.UpdateQueueEnd(e)
			if err != nil {
				nsqLog.LogErrorf(
					"failed to update topic end to channel(%s) - %s",
					channel.name, err)
			}
		}
	}
	t.channelLock.RUnlock()
}

func (t *Topic) TotalMessageCnt() uint64 {
	return uint64(t.backend.GetQueueWriteEnd().GetTotalMsgCnt())
}

func (t *Topic) TotalSize() int64 {
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
		t.notifyCall(t)
	} else {
		nsqLog.Logf("TOPIC(%s): closing", t.GetFullName())
	}

	if deleted {
		t.channelLock.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.channelLock.Unlock()
		// empty the queue (deletes the backend files, too)
		t.Empty()
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

func (t *Topic) DisableForSlave() {
	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(true)
	}
	t.channelLock.RUnlock()
}

func (t *Topic) EnableForMaster() {
	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(false)
	}
	t.channelLock.RUnlock()
}

func (t *Topic) Empty() error {
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
			t.updateChannelsEnd()
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
		t.updateChannelsEnd()
	} else {
		t.needNotifyChan = true
	}
	return err
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.channelLock.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.channelLock.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.option.E2EProcessingLatencyWindowTime,
				t.option.E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}
