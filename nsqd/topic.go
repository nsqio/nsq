package nsqd

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/quantile"
	"github.com/absolute8511/nsq/internal/util"
)

const (
	MAX_TOPIC_PARTITION = 1023
)

var (
	ErrInvalidMessageID    = errors.New("message id is invalid")
	ErrWriteOffsetMismatch = errors.New("write offset mismatch")
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

type TopicDynamicConf struct {
	AutoCommit   int32
	RetentionDay int32
	SyncEvery    int64
}

type Topic struct {
	sync.Mutex

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
	msgIDCursor     MsgIDGenerator
	defaultIDSeq    uint64
	needFlush       int32
	EnableTrace     int32
	lastSyncCnt     int64
	putBuffer       bytes.Buffer
	bp              sync.Pool
	writeDisabled   int32
	dynamicConf     *TopicDynamicConf
	magicCode       int64
	committedOffset atomic.Value
	// 100bytes, 1KB, 4KB, 16KB, 64KB, 256KB, 512KB, 1MB, 2MB, 4MB
	msgSizeStats [10]int64
	// 1ms, 8ms, 16ms, 32ms, 64ms, 128ms, 512ms, 1024ms, 2046ms, 4098ms
	msgWriteLatencyStats [10]int64
	writeErrCnt          int64
	statsMutex           sync.Mutex
	clientPubStats       map[string]*ClientPubStats
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
		dynamicConf:    &TopicDynamicConf{SyncEvery: opt.SyncEvery, AutoCommit: 1},
		putBuffer:      bytes.Buffer{},
		notifyCall:     notify,
		writeDisabled:  writeDisabled,
		clientPubStats: make(map[string]*ClientPubStats),
	}
	if t.dynamicConf.SyncEvery < 1 {
		t.dynamicConf.SyncEvery = 1
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
	err = t.loadMagicCode()
	if err != nil {
		nsqLog.LogErrorf("topic %v failed to load magic code: %v", t.fullName, err)
		return nil
	}

	t.notifyCall(t)
	nsqLog.LogDebugf("new topic created: %v", t.tname)

	return t
}

func (t *Topic) getMagicCodeFileName() string {
	return path.Join(t.dataPath, "magic"+strconv.Itoa(t.partition))
}

func (t *Topic) saveMagicCode() error {
	var f *os.File
	var err error

	fileName := t.getMagicCodeFileName()
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "%d\n",
		atomic.LoadInt64(&t.magicCode))
	if err != nil {
		return err
	}
	f.Sync()
	nsqLog.Infof("saved topic %v as magic code: %v", t.fullName, atomic.LoadInt64(&t.magicCode))
	return nil
}

func (t *Topic) removeMagicCode() {
	fileName := t.getMagicCodeFileName()
	err := os.Remove(fileName)
	if err != nil {
		nsqLog.Errorf("remove the magic file %v failed:%v", fileName, err)
	}
}

func (t *Topic) loadMagicCode() error {
	var f *os.File
	var err error

	fileName := t.getMagicCodeFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	var code int64
	_, err = fmt.Fscanf(f, "%d\n",
		&code)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&t.magicCode, code)
	nsqLog.Infof("loading topic %v as magic code: %v", t.fullName, code)
	return nil
}

func (t *Topic) MarkAsRemoved() (string, error) {
	t.Lock()
	defer t.Unlock()
	atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1)
	nsqLog.Logf("TOPIC(%s): deleting", t.GetFullName())
	// since we are explicitly deleting a topic (not just at system exit time)
	// de-register this from the lookupd
	t.notifyCall(t)

	t.channelLock.Lock()
	for _, channel := range t.channelMap {
		delete(t.channelMap, channel.name)
		channel.Delete()
	}
	t.channelLock.Unlock()
	// we should move our partition only
	renamePath := t.dataPath + "-removed-" + strconv.Itoa(int(time.Now().Unix()))
	nsqLog.Warningf("mark the topic %v as removed: %v", t.GetFullName(), renamePath)
	os.MkdirAll(renamePath, 0755)
	err := t.backend.RemoveTo(renamePath)
	if err != nil {
		nsqLog.Errorf("failed to mark the topic %v as removed %v failed: %v", t.GetFullName(), renamePath, err)
	}
	util.AtomicRename(t.getMagicCodeFileName(), path.Join(renamePath, "magic"+strconv.Itoa(t.partition)))
	t.removeMagicCode()
	return renamePath, err
}

// should be protected by the topic lock for all partitions
func (t *Topic) SetMagicCode(code int64) error {
	if t.magicCode == code {
		return nil
	} else if t.magicCode > 0 {
		nsqLog.Errorf("magic code set more than once :%v, %v", t.magicCode, code)
		return errors.New("magic code can not be changed.")
	}
	t.magicCode = code
	return t.saveMagicCode()
}

// should be protected by the topic lock for all partitions
func (t *Topic) GetMagicCode() int64 {
	return t.magicCode
}

func (t *Topic) SetTrace(enable bool) {
	if enable {
		atomic.StoreInt32(&t.EnableTrace, 1)
	} else {
		atomic.StoreInt32(&t.EnableTrace, 0)
	}
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
	if cur != nil && offset.Offset() < cur.Offset() {
		nsqLog.LogDebugf("commited is rollbacked: %v, %v", cur, offset)
	}
	t.committedOffset.Store(offset)
}

func (t *Topic) GetDiskQueueSnapshot() *DiskQueueSnapshot {
	e := t.backend.GetQueueReadEnd()
	commit := t.GetCommitted()
	if commit != nil && e.Offset() > commit.Offset() {
		e = commit
	}
	return NewDiskQueueSnapshot(getBackendName(t.tname, t.partition), t.dataPath, e)
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

func (t *Topic) SetMsgGenerator(idGen MsgIDGenerator) {
	t.Lock()
	t.msgIDCursor = idGen
	t.Unlock()
}

func (t *Topic) GetMsgGenerator() MsgIDGenerator {
	t.Lock()
	cursor := t.msgIDCursor
	t.Unlock()
	return cursor
}

func (t *Topic) SetDynamicInfo(dynamicConf TopicDynamicConf, idGen MsgIDGenerator) {
	t.Lock()
	if idGen != nil {
		t.msgIDCursor = idGen
	}
	atomic.StoreInt64(&t.dynamicConf.SyncEvery, dynamicConf.SyncEvery)
	atomic.StoreInt32(&t.dynamicConf.AutoCommit, dynamicConf.AutoCommit)
	atomic.StoreInt32(&t.dynamicConf.RetentionDay, dynamicConf.RetentionDay)
	t.Unlock()
}

func (t *Topic) nextMsgID() MessageID {
	id := uint64(0)
	if t.msgIDCursor != nil {
		id = t.msgIDCursor.NextID()
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
		readEnd := t.backend.GetQueueReadEnd()
		curCommit := t.GetCommitted()
		if curCommit != nil && readEnd.Offset() > curCommit.Offset() {
			if nsqLog.Level() >= levellogger.LOG_DEBUG {
				nsqLog.Logf("channel %v, end to commit: %v, read end: %v", channelName, curCommit, readEnd)
			}
			readEnd = curCommit
		}

		channel = NewChannel(t.GetTopicName(), t.GetTopicPart(), channelName, readEnd,
			t.option, deleteCallback, atomic.LoadInt32(&t.writeDisabled), t.notifyCall)

		channel.UpdateQueueEnd(readEnd, false)
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
		t.updateChannelsEnd(true)
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
		t.updateChannelsEnd(true)
	}

	return err
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	t.Lock()
	if m.ID > 0 {
		nsqLog.Logf("should not pass id in message while pub: %v", m.ID)
		t.Unlock()
		return 0, 0, 0, nil, ErrInvalidMessageID
	}
	id, offset, writeBytes, dend, err := t.PutMessageNoLock(m)
	t.Unlock()
	return id, offset, writeBytes, dend, err
}

func (t *Topic) PutMessageNoLock(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return 0, 0, 0, nil, errors.New("exiting")
	}
	if m.ID > 0 {
		nsqLog.Logf("should not pass id in message while pub: %v", m.ID)
		return 0, 0, 0, nil, ErrInvalidMessageID
	}

	id, offset, writeBytes, dend, err := t.put(m)
	if err == nil {
		if t.dynamicConf.SyncEvery == 1 || dend.TotalMsgCnt()-atomic.LoadInt64(&t.lastSyncCnt) >= t.dynamicConf.SyncEvery {
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
		// flush buffer only to allow the channel read recent write
		// no need sync to disk, since sync is heavy IO.
		t.backend.FlushBuffer()
		t.updateChannelsEnd(false)
	}
}

func (t *Topic) PutMessageOnReplica(m *Message, offset BackendOffset) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return nil, ErrExiting
	}
	wend := t.backend.GetQueueWriteEnd()
	if wend.Offset() != offset {
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
	if wend.Offset() != offset {
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
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
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
		if m.ID > 0 {
			nsqLog.Logf("should not pass id in message while pub: %v", m.ID)
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
			return 0, 0, 0, 0, nil, ErrInvalidMessageID
		}
		id, offset, bytes, end, err := t.put(m)
		if err != nil {
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
			return firstMsgID, firstOffset, batchBytes, firstCnt, &diskEnd, err
		}
		diskEnd = end
		batchBytes += bytes
		totalCnt = diskEnd.TotalMsgCnt()
		if firstOffset == BackendOffset(-1) {
			firstOffset = offset
			firstMsgID = id
			firstCnt = diskEnd.TotalMsgCnt()
		}
	}

	if int64(len(msgs)) >= t.dynamicConf.SyncEvery || totalCnt-atomic.LoadInt64(&t.lastSyncCnt) >= t.dynamicConf.SyncEvery {
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

	if atomic.LoadInt32(&t.dynamicConf.AutoCommit) == 1 {
		t.UpdateCommittedOffset(&dend)
	}

	if atomic.LoadInt32(&t.EnableTrace) == 1 || nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqMsgTracer.TracePub(t.GetFullName(), m.TraceID, m, offset, dend.TotalMsgCnt())
	}
	return m.ID, offset, writeBytes, dend, nil
}

func (t *Topic) updateChannelsEnd(forceReload bool) {
	s := time.Now()
	e := t.backend.GetQueueReadEnd()
	curCommit := t.GetCommitted()
	// if not committed, we need wait to notify channel.
	if curCommit != nil && e.Offset() > curCommit.Offset() {
		if nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.Logf("topic %v, end to commit: %v, read end: %v", t.fullName, curCommit, e)
		}
		e = curCommit
	}
	t.channelLock.RLock()
	if e != nil {
		for _, channel := range t.channelMap {
			err := channel.UpdateQueueEnd(e, forceReload)
			if err != nil {
				nsqLog.LogErrorf(
					"failed to update topic end to channel(%s) - %s",
					channel.name, err)
			}
		}
	}
	t.channelLock.RUnlock()
	cost := time.Now().Sub(s)
	if cost > time.Second {
		nsqLog.Logf("topic(%s): update channels end cost: %v", t.GetFullName(), cost)
	}
}

func (t *Topic) TotalMessageCnt() uint64 {
	return uint64(t.backend.GetQueueWriteEnd().TotalMsgCnt())
}

func (t *Topic) GetQueueReadStart() int64 {
	return int64(t.backend.GetQueueReadStart().Offset())
}

func (t *Topic) TotalDataSize() int64 {
	e := t.backend.GetQueueWriteEnd()
	if e == nil {
		return 0
	}
	return int64(e.Offset())
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
		t.removeMagicCode()
		return t.backend.Delete()
	}

	// write anything leftover to disk
	t.flush(true)
	nsqLog.Logf("[TRACE_DATA] exiting topic end: %v, cnt: %v", t.TotalDataSize(), t.TotalMessageCnt())
	t.channelLock.RLock()
	// close all the channels
	for _, channel := range t.channelMap {
		nsqLog.Logf("[TRACE_DATA] exiting channel : %v, %v, %v, %v", channel.GetName(), channel.GetConfirmed(), channel.Depth(), channel.backend.GetQueueReadEnd())
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
	nsqLog.Logf("[TRACE_DATA] while disable topic %v end: %v, cnt: %v", t.GetFullName(), t.TotalDataSize(), t.TotalMessageCnt())
	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(true)
		d, ok := c.backend.(*diskQueueReader)
		var curRead BackendQueueEnd
		if ok {
			curRead = d.GetQueueCurrentRead()
		}

		nsqLog.Logf("[TRACE_DATA] while disable channel : %v, %v, %v, %v, %v", c.GetName(),
			c.GetConfirmed(), c.Depth(), c.backend.GetQueueReadEnd(), curRead)
	}
	t.channelLock.RUnlock()
	// notify de-register from lookup
	t.notifyCall(t)
}

func (t *Topic) EnableForMaster() {
	// TODO : log the last logid and message offset
	// TODO : log the first logid and message offset after enable master
	nsqLog.Logf("[TRACE_DATA] while enable topic %v end: %v, cnt: %v", t.GetFullName(), t.TotalDataSize(), t.TotalMessageCnt())
	t.channelLock.RLock()
	for _, c := range t.channelMap {
		c.DisableConsume(false)
		d, ok := c.backend.(*diskQueueReader)
		var curRead BackendQueueEnd
		if ok {
			curRead = d.GetQueueCurrentRead()
		}
		nsqLog.Logf("[TRACE_DATA] while enable channel : %v, %v, %v, %v, %v", c.GetName(),
			c.GetConfirmed(), c.Depth(), c.backend.GetQueueReadEnd(), curRead)
	}
	t.channelLock.RUnlock()
	atomic.StoreInt32(&t.writeDisabled, 0)
	// notify re-register to lookup
	t.notifyCall(t)
}

func (t *Topic) Empty() error {
	nsqLog.Logf("TOPIC(%s): empty", t.GetFullName())
	t.removeMagicCode()
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
			t.updateChannelsEnd(false)
		}
		return nil
	}
	atomic.StoreInt64(&t.lastSyncCnt, t.backend.GetQueueWriteEnd().TotalMsgCnt())
	err := t.backend.Flush()
	if err != nil {
		nsqLog.LogErrorf("failed flush: %v", err)
		return err
	}
	if notifyChan {
		t.updateChannelsEnd(false)
	}
	return err
}

func (t *Topic) PrintCurrentStats() {
	nsqLog.Logf("topic(%s) status: write end %v", t.GetFullName(), t.backend.GetQueueWriteEnd())
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		nsqLog.Logf("channel(%s) depth: %v, confirmed: %v, debug: %v", ch.GetName(), ch.Depth(),
			ch.GetConfirmed(), ch.GetChannelDebugStats())
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

func (t *Topic) UpdatePubStats(remote string, agent string, protocol string, count int64, hasErr bool) {
	t.statsMutex.Lock()
	s, ok := t.clientPubStats[remote]
	if !ok {
		// too much clients pub to this topic
		// we just ignore stats
		if len(t.clientPubStats) > 1000 {
			scanStart := time.Now()
			scanCnt := 0
			cleanCnt := 0
			for _, s := range t.clientPubStats {
				scanCnt++
				if time.Since(scanStart) > time.Millisecond*200 {
					break
				}
				if time.Now().Unix()-s.LastPubTs > 60*60 {
					delete(t.clientPubStats, s.RemoteAddress)
					cleanCnt++
				}
			}
			nsqLog.Logf("clean pub stats cost %v, scan: %v, clean:%v, left: %v", time.Since(scanStart),
				scanCnt, cleanCnt, len(t.clientPubStats))
			t.statsMutex.Unlock()
			return
		}
		s = &ClientPubStats{
			RemoteAddress: remote,
			UserAgent:     agent,
			Protocol:      protocol,
		}
		t.clientPubStats[remote] = s
	}

	if hasErr {
		s.ErrCount++
	} else {
		s.PubCount += count
		s.LastPubTs = time.Now().Unix()
	}
	t.statsMutex.Unlock()
}

func (t *Topic) RemovePubStats(remote string, protocol string) {
	t.statsMutex.Lock()
	delete(t.clientPubStats, remote)
	t.statsMutex.Unlock()
}

func (t *Topic) GetPubStats() []ClientPubStats {
	t.statsMutex.Lock()
	stats := make([]ClientPubStats, 0, len(t.clientPubStats))
	for _, s := range t.clientPubStats {
		stats = append(stats, *s)
	}
	t.statsMutex.Unlock()
	return stats
}

// maybe should return the cleaned offset to allow commit log clean
func (t *Topic) TryCleanOldData(retentionSize int64) error {
	// clean the data that has been consumed and keep the retention policy
	var oldestPos BackendQueueEnd
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		pos := ch.GetConfirmed()
		if oldestPos == nil {
			oldestPos = pos
		} else if oldestPos.Offset() > pos.Offset() {
			oldestPos = pos
		}
	}
	t.channelLock.RUnlock()
	if oldestPos == nil {
		nsqLog.Logf("no consume position found")
		return nil
	}
	if BackendOffset(retentionSize) >= oldestPos.Offset() {
		return nil
	}
	nsqLog.Logf("clean topic %v data current oldest confirmed %v",
		t.GetFullName(), oldestPos)

	snapReader := NewDiskQueueSnapshot(getBackendName(t.tname, t.partition), t.dataPath, oldestPos)
	cleanStart := t.backend.GetQueueReadStart()
	snapReader.SetQueueStart(cleanStart)
	err := snapReader.SeekTo(cleanStart.Offset())
	if err != nil {
		nsqLog.Errorf("topic: %v failed to seek to %v: %v", t.GetFullName(), cleanStart, err)
		return err
	}
	readNum := snapReader.GetQueueCurrentReadFile().FileNum
	data := snapReader.ReadOne()
	if data.Err != nil {
		return data.Err
	}
	cleanOffset := BackendOffset(0)
	cleanFileNum := int64(0)
	t.Lock()
	cleanTime := time.Now().Add(-1 * time.Hour * 24 * time.Duration(t.dynamicConf.RetentionDay))
	t.Unlock()
	for {
		if retentionSize > 0 {
			// clean data ignore the retention day
			// only keep the retention size (start from the last consumed)
			if data.Offset > oldestPos.Offset()-BackendOffset(retentionSize) {
				break
			}
			cleanOffset = data.Offset
			cleanFileNum = readNum
		} else {
			msg, err := decodeMessage(data.Data)
			if err != nil {
				nsqLog.LogErrorf("failed to decode message - %s - %v", err, data)
			} else {
				if msg.Timestamp >= cleanTime.UnixNano() {
					break
				}
				cleanOffset = data.Offset
				cleanFileNum = readNum
			}
		}
		err = snapReader.SkipToNext()
		if err != nil {
			nsqLog.LogErrorf("failed to skip - %s ", err)
			break
		}
		readNum = snapReader.GetQueueCurrentReadFile().FileNum
		data = snapReader.ReadOne()
		if data.Err != nil {
			nsqLog.LogErrorf("failed to read - %s ", data.Err)
			break
		}
	}

	nsqLog.Warningf("clean topic %v data from %v:%v under retention %v, %v",
		t.GetFullName(), cleanFileNum, cleanOffset, cleanTime, retentionSize)
	if cleanOffset >= oldestPos.Offset() {
		nsqLog.Warningf("clean topic %v data could not exceed current oldest confirmed %v",
			t.GetFullName(), oldestPos)
		return nil
	}
	t.backend.CleanOldDataByRetention(cleanFileNum, cleanOffset)
	return nil
}
