package nsqd

import (
	"bytes"
	"errors"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/quantile"
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

	notifyCall    func(v interface{})
	option        *Options
	MsgIDCursor   MsgIDGenerator
	defaultIDSeq  uint64
	needFlush     int32
	EnableTrace   bool
	syncEvery     int64
	putBuffer     bytes.Buffer
	bp            sync.Pool
	writeDisabled int32
}

func GetTopicFullName(topic string, part int) string {
	return topic + "-" + strconv.Itoa(part)
}

// Topic constructor
func NewTopic(topicName string, part int, opt *Options, deleteCallback func(*Topic), writeDisabled int32, notify func(v interface{})) *Topic {
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
		writeDisabled:  writeDisabled,
	}
	if t.syncEvery < 1 {
		t.syncEvery = 1
	}
	t.bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
	t.fullName = GetTopicFullName(t.tname, t.partition)

	t.dataPath = path.Join(opt.DataPath, topicName)
	err := os.MkdirAll(t.dataPath, 0755)
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to create directory: %v ", t.fullName, err)
		return nil
	}

	if strings.HasSuffix(topicName, "#ephemeral") {
		t.ephemeral = true
		t.backend = newDummyBackendQueueWriter()
	} else {
		backendName := getBackendName(t.tname, t.partition)
		t.backend = newDiskQueueWriter(backendName,
			t.dataPath,
			opt.MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(opt.MaxMsgSize)+minValidMsgLength,
			opt.SyncEvery)
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
func (t *Topic) GetChannelMapCopy() map[string]*Channel {
	tmpMap := make(map[string]*Channel)
	t.channelLock.RLock()
	for k, v := range t.channelMap {
		tmpMap[k] = v
	}
	t.channelLock.RUnlock()
	return tmpMap
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

func (t *Topic) nextMsgID() MessageID {
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
			t.option, deleteCallback, atomic.LoadInt32(&t.writeDisabled), t.notifyCall)
		channel.UpdateQueueEnd(t.backend.GetQueueReadEnd())
		if t.IsWriteDisabled() {
			channel.DisableConsume(true)
		}
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
	old := t.backend.GetQueueWriteEnd()
	nsqLog.Logf("reset the backend from %v to : %v, %v", old, vend, diffCnt)
	err := t.backend.RollbackWrite(vend, diffCnt)
	if err == nil {
		t.updateChannelsEnd()
	}
	return err
}

func (t *Topic) ResetBackendEndNoLock(vend BackendOffset, totalCnt int64) error {
	old := t.backend.GetQueueWriteEnd()
	nsqLog.Logf("reset the backend from %v to : %v, %v", old, vend, totalCnt)
	err := t.backend.ResetWriteEnd(vend, totalCnt)
	if err != nil {
		nsqLog.LogErrorf("reset backend to %v error: %v", vend, err)
	} else {
		t.updateChannelsEnd()
	}

	return err
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) (MessageID, BackendOffset, int32, int64, error) {
	t.Lock()
	id, offset, writeBytes, totalCnt, err := t.PutMessageNoLock(m)
	t.Unlock()
	return id, offset, writeBytes, totalCnt, err
}

func (t *Topic) PutMessageNoLock(m *Message) (MessageID, BackendOffset, int32, int64, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		t.Unlock()
		return 0, 0, 0, 0, errors.New("exiting")
	}

	id, offset, writeBytes, totalCnt, err := t.put(m)
	if totalCnt%t.syncEvery == 0 {
		t.flush(true)
	} else {
		t.flushForChannels()
	}
	return id, offset, writeBytes, totalCnt, err
}

func (t *Topic) flushForChannels() {
	needFlush := false
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		if ch.IsWaitingMoreData() {
			needFlush = true
			break
		}
	}
	t.channelLock.RUnlock()
	if needFlush {
		t.flush(true)
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
		nsqLog.LogErrorf(
			"TOPIC(%s) : write message offset mismatch %v, %v",
			t.GetFullName(), offset, wend)
		return ErrWriteOffsetMismatch
	}

	totalCnt := int64(0)
	var err error
	for _, m := range msgs {
		_, _, _, totalCnt, err = t.put(m)
		if err != nil {
			t.ResetBackendEndNoLock(wend.GetOffset(), wend.GetTotalMsgCnt())
			return err
		}
	}

	if int64(len(msgs)) >= t.syncEvery || totalCnt%t.syncEvery == 0 {
		t.flush(false)
	}
	return nil
}

func (t *Topic) PutMessagesNoLock(msgs []*Message) (MessageID, BackendOffset, int32, int64, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return 0, 0, 0, 0, ErrExiting
	}

	wend := t.backend.GetQueueWriteEnd()
	firstMsgID := MessageID(0)
	firstOffset := BackendOffset(-1)
	totalCnt := int64(0)
	firstCnt := int64(0)
	batchBytes := int32(0)
	for _, m := range msgs {
		id, offset, bytes, cnt, err := t.put(m)
		if err != nil {
			t.ResetBackendEndNoLock(wend.GetOffset(), wend.GetTotalMsgCnt())
			return firstMsgID, firstOffset, batchBytes, cnt, err
		}
		batchBytes += bytes
		totalCnt = cnt
		if firstOffset == BackendOffset(-1) {
			firstOffset = offset
			firstMsgID = id
			firstCnt = cnt
		}
	}

	if int64(len(msgs)) >= t.syncEvery || totalCnt%t.syncEvery == 0 {
		t.flush(true)
	} else {
		t.flushForChannels()
	}
	return firstMsgID, firstOffset, batchBytes, firstCnt, nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) (MessageID, BackendOffset, int32, int64, error) {
	t.Lock()
	firstMsgID, firstOffset, batchBytes, totalCnt, err := t.PutMessagesNoLock(msgs)
	t.Unlock()
	return firstMsgID, firstOffset, batchBytes, totalCnt, err
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

func (t *Topic) TotalDataSize() int64 {
	e := t.backend.GetQueueWriteEnd()
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
	nsqLog.Logf("[TRACE_DATA] exiting topic end: %v, cnt: %v", t.TotalDataSize(), t.TotalMessageCnt())
	t.channelLock.RLock()
	// close all the channels
	for _, channel := range t.channelMap {
		nsqLog.Logf("[TRACE_DATA] exiting channel : %v, %v, %v, %v", channel.GetName(), channel.currentLastConfirmed, channel.Depth(), channel.backend.GetQueueReadEnd())
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			nsqLog.Logf(" channel(%s) close - %s", channel.name, err)
		}
	}
	t.channelLock.RUnlock()

	return t.backend.Close()
}

func (t *Topic) IsWriteDisabled() bool {
	return atomic.LoadInt32(&t.writeDisabled) == 1
}

func (t *Topic) DisableForSlave() {
	atomic.StoreInt32(&t.writeDisabled, 1)
	nsqLog.Logf("[TRACE_DATA] while disable topic end: %v, cnt: %v", t.TotalDataSize(), t.TotalMessageCnt())
	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(true)
		nsqLog.Logf("[TRACE_DATA] while disable channel : %v, %v, %v, %v", c.GetName(), c.currentLastConfirmed, c.Depth(), c.backend.GetQueueReadEnd())
	}
	t.channelLock.RUnlock()
	// notify de-register from lookup
	t.notifyCall(t)
}

func (t *Topic) EnableForMaster() {
	nsqLog.Logf("[TRACE_DATA] while enable topic end: %v, cnt: %v", t.TotalDataSize(), t.TotalMessageCnt())
	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(false)
		nsqLog.Logf("[TRACE_DATA] while enable channel : %v, %v, %v, %v", c.GetName(), c.currentLastConfirmed, c.Depth(), c.backend.GetQueueReadEnd())
	}
	t.channelLock.RUnlock()
	atomic.StoreInt32(&t.writeDisabled, 0)
	// notify re-register to lookup
	t.notifyCall(t)
}

func (t *Topic) Empty() error {
	nsqLog.Logf("TOPIC(%s): empty", t.GetFullName())
	return t.backend.Empty()
}

func (t *Topic) ForceFlush() {
	s := time.Now()
	t.flush(true)
	cost := time.Now().Sub(s)
	if cost > time.Second {
		nsqLog.Logf("topic(%s): flush cost: %v", t.GetFullName(), cost)
	}
}

func (t *Topic) flush(notifyChan bool) error {
	ok := atomic.CompareAndSwapInt32(&t.needFlush, 1, 0)
	if !ok {
		if notifyChan {
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
		t.updateChannelsEnd()
	}
	return err
}

func (t *Topic) PrintCurrentStats() {
	nsqLog.Logf("topic(%s) status: write end %v", t.GetFullName(), t.backend.GetQueueWriteEnd())
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		nsqLog.Logf("channel(%s) depth: %v, confirmed: %v, debug: %v", ch.GetName(), ch.Depth(),
			ch.currentLastConfirmed, ch.GetChannelDebugStats())
	}
	t.channelLock.RUnlock()
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
