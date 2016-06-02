package nsqd

import (
	"bytes"
	"errors"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/levellogger"
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

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq *diskQueueWriter) (BackendOffset, int32, diskQueueEndInfo, error) {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return 0, 0, diskQueueEndInfo{}, err
	}
	return bq.PutV2(buf.Bytes())
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
	backend     *diskQueueWriter
	dataPath    string
	flushChan   chan int
	exitFlag    int32

	ephemeral      bool
	deleteCallback func(*Topic)
	deleter        sync.Once

	notifyCall      func(v interface{})
	option          *Options
	MsgIDCursor     MsgIDGenerator
	defaultIDSeq    uint64
	needFlush       int32
	EnableTrace     bool
	syncEvery       int64
	putBuffer       bytes.Buffer
	bp              sync.Pool
	writeDisabled   int32
	committedOffset atomic.Value
	autoCommit      bool
}

func GetTopicFullName(topic string, part int) string {
	return topic + "-" + strconv.Itoa(part)
}

// Topic constructor
func NewTopic(topicName string, part int, opt *Options,
	deleteCallback func(*Topic), writeDisabled int32,
	notify func(v interface{})) *Topic {
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
		autoCommit:     true,
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

	backendName := getBackendName(t.tname, t.partition)
	t.backend = newDiskQueueWriter(backendName,
		t.dataPath,
		opt.MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(opt.MaxMsgSize)+minValidMsgLength,
		opt.SyncEvery).(*diskQueueWriter)
	t.UpdateCommittedOffset(t.backend.GetQueueWriteEnd())

	t.notifyCall(t)
	nsqLog.LogDebugf("new topic created: %v", t.tname)

	return t
}

func (t *Topic) SetAutoCommit(enable bool) {
	t.autoCommit = enable
}

func (t *Topic) GetCommitted() BackendQueueEnd {
	l := t.committedOffset.Load()
	if l == nil {
		return nil
	}
	return l.(BackendQueueEnd)
}

// note: multiple writer should be protected by lock
func (t *Topic) UpdateCommittedOffset(offset BackendQueueEnd) {
	if offset == nil {
		return
	}
	cur := t.GetCommitted()
	if cur != nil && offset.GetOffset() < cur.GetOffset() {
		nsqLog.LogDebugf("commited is rollbacked: %v, %v", cur, offset)
	}
	t.committedOffset.Store(offset)
}

func (t *Topic) GetDiskQueueSnapshot() *DiskQueueSnapshot {
	e := t.backend.GetQueueReadEndV2()
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
		e := t.backend.GetQueueReadEnd()
		curCommit := t.GetCommitted()
		if curCommit != nil && e.GetOffset() > curCommit.GetOffset() {
			if nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.Logf("channel %v, end to commit: %v, read end: %v", channel.GetName(), curCommit, e)
			}
			e = curCommit
		}
		channel.UpdateQueueEnd(e)
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
	dend, err := t.backend.RollbackWriteV2(vend, diffCnt)
	if err == nil {
		t.UpdateCommittedOffset(&dend)
		t.updateChannelsEnd()
	}
	return err
}

func (t *Topic) ResetBackendEndNoLock(vend BackendOffset, totalCnt int64) error {
	old := t.backend.GetQueueWriteEnd()
	nsqLog.Logf("topic %v reset the backend from %v to : %v, %v", t.GetFullName(), old, vend, totalCnt)
	dend, err := t.backend.ResetWriteEndV2(vend, totalCnt)
	if err != nil {
		nsqLog.LogErrorf("reset backend to %v error: %v", vend, err)
	} else {
		t.UpdateCommittedOffset(&dend)
		t.updateChannelsEnd()
	}

	return err
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	t.Lock()
	id, offset, writeBytes, dend, err := t.PutMessageNoLock(m)
	t.Unlock()
	return id, offset, writeBytes, dend, err
}

func (t *Topic) PutMessageNoLock(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		t.Unlock()
		return 0, 0, 0, nil, errors.New("exiting")
	}

	id, offset, writeBytes, dend, err := t.put(m)
	if err == nil {
		if dend.GetTotalMsgCnt()%t.syncEvery == 0 {
			if !t.IsWriteDisabled() {
				t.flush(true)
			}
		} else {
			t.flushForChannels()
		}
	}
	return id, offset, writeBytes, &dend, err
}

func (t *Topic) flushForChannels() {
	if t.IsWriteDisabled() {
		return
	}
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

func (t *Topic) PutMessageOnReplica(m *Message, offset BackendOffset) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return nil, ErrExiting
	}
	wend := t.backend.GetQueueWriteEnd()
	if wend.GetOffset() != offset {
		nsqLog.LogErrorf("topic %v: write offset mismatch: %v, %v", t.GetFullName(), offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	_, _, _, dend, err := t.put(m)
	if err != nil {
		return nil, err
	}
	return &dend, nil
}

func (t *Topic) PutMessagesOnReplica(msgs []*Message, offset BackendOffset) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return nil, ErrExiting
	}

	wend := t.backend.GetQueueWriteEnd()
	if wend.GetOffset() != offset {
		nsqLog.LogErrorf(
			"TOPIC(%s) : write message offset mismatch %v, %v",
			t.GetFullName(), offset, wend)
		return nil, ErrWriteOffsetMismatch
	}

	var dend diskQueueEndInfo
	var err error
	for _, m := range msgs {
		_, _, _, dend, err = t.put(m)
		if err != nil {
			t.ResetBackendEndNoLock(wend.GetOffset(), wend.GetTotalMsgCnt())
			return nil, err
		}
	}

	return &dend, nil
}

func (t *Topic) PutMessagesNoLock(msgs []*Message) (MessageID, BackendOffset, int32, int64, BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return 0, 0, 0, 0, nil, ErrExiting
	}

	wend := t.backend.GetQueueWriteEnd()
	firstMsgID := MessageID(0)
	firstOffset := BackendOffset(-1)
	totalCnt := int64(0)
	firstCnt := int64(0)
	var diskEnd diskQueueEndInfo
	batchBytes := int32(0)
	for _, m := range msgs {
		id, offset, bytes, end, err := t.put(m)
		if err != nil {
			t.ResetBackendEndNoLock(wend.GetOffset(), wend.GetTotalMsgCnt())
			return firstMsgID, firstOffset, batchBytes, firstCnt, &diskEnd, err
		}
		diskEnd = end
		batchBytes += bytes
		totalCnt = diskEnd.GetTotalMsgCnt()
		if firstOffset == BackendOffset(-1) {
			firstOffset = offset
			firstMsgID = id
			firstCnt = diskEnd.GetTotalMsgCnt()
		}
	}

	if int64(len(msgs)) >= t.syncEvery || totalCnt%t.syncEvery == 0 {
		if !t.IsWriteDisabled() {
			t.flush(true)
		}
	} else {
		t.flushForChannels()
	}
	return firstMsgID, firstOffset, batchBytes, firstCnt, &diskEnd, nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) (MessageID, BackendOffset, int32, int64, BackendQueueEnd, error) {
	t.Lock()
	firstMsgID, firstOffset, batchBytes, totalCnt, dend, err := t.PutMessagesNoLock(msgs)
	t.Unlock()
	return firstMsgID, firstOffset, batchBytes, totalCnt, dend, err
}

func (t *Topic) put(m *Message) (MessageID, BackendOffset, int32, diskQueueEndInfo, error) {
	if m.ID <= 0 {
		m.ID = t.nextMsgID()
	}
	offset, writeBytes, dend, err := writeMessageToBackend(&t.putBuffer, m, t.backend)
	atomic.StoreInt32(&t.needFlush, 1)
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write message to backend - %s",
			t.GetFullName(), err)
		return m.ID, offset, writeBytes, dend, err
	}

	if t.autoCommit {
		t.UpdateCommittedOffset(&dend)
	}

	if t.EnableTrace || nsqLog.Level() >= levellogger.LOG_DEBUG {
		nsqLog.Logf("[TRACE] message %v put in topic: %v, at offset: %v, disk end: %v", m.GetFullMsgID(),
			t.GetFullName(), offset, dend)
	}
	return m.ID, offset, writeBytes, dend, nil
}

func (t *Topic) updateChannelsEnd() {
	e := t.backend.GetQueueReadEnd()
	curCommit := t.GetCommitted()
	// if not committed, we need wait to notify channel.
	if curCommit != nil && e.GetOffset() > curCommit.GetOffset() {
		if nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.Logf("topic %v, end to commit: %v, read end: %v", t.fullName, curCommit, e)
		}
		e = curCommit
	}
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
	if e == nil {
		return 0
	}
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
	// TODO : log the last logid and message offset
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
	// TODO : log the last logid and message offset
	// TODO : log the first logid and message offset after enable master
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
	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		e := t.backend.GetQueueReadEnd()
		curCommit := t.GetCommitted()
		nsqLog.Logf("topic %v, end to commit: %v, read end: %v", t.fullName, curCommit, e)
	}

	s := time.Now()
	t.flush(true)
	cost := time.Now().Sub(s)
	if cost > time.Second {
		nsqLog.Logf("topic(%s): flush cost: %v", t.GetFullName(), cost)
	}
	s = time.Now()
	t.channelLock.RLock()
	for _, channel := range t.channelMap {
		channel.flush()
	}
	t.channelLock.RUnlock()
	cost = time.Now().Sub(s)
	if cost > time.Second {
		nsqLog.Logf("topic(%s): flush channel cost: %v", t.GetFullName(), cost)
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
