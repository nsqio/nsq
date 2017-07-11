package nsqd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/ext"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/quantile"
	"github.com/absolute8511/nsq/internal/util"
)

const (
	MAX_TOPIC_PARTITION    = 1023
	HISTORY_STAT_FILE_NAME = ".stat.history.dat"
)

var (
	ErrInvalidMessageID           = errors.New("message id is invalid")
	ErrWriteOffsetMismatch        = errors.New("write offset mismatch")
	ErrOperationInvalidState      = errors.New("the operation is not allowed under current state")
	ErrMessageInvalidDelayedState = errors.New("the message is invalid for delayed")
)

func writeMessageToBackend(writeExt bool, buf *bytes.Buffer, msg *Message, bq *diskQueueWriter) (BackendOffset, int32, diskQueueEndInfo, error) {
	buf.Reset()
	_, err := msg.WriteTo(buf, writeExt)
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
	OrderedMulti bool
	Ext          bool
}

type PubInfo struct {
	Done       chan struct{}
	MsgBody    *bytes.Buffer
	ExtContent ext.IExtContent
	StartPub   time.Time
	Err        error
}

type PubInfoChan chan *PubInfo

type ChannelMetaInfo struct {
	Name    string `json:"name"`
	Paused  bool   `json:"paused"`
	Skipped bool   `json:"skipped"`
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
	isOrdered       int32
	magicCode       int64
	committedOffset atomic.Value
	detailStats     *DetailStatsInfo
	needFixData     int32
	pubWaitingChan  PubInfoChan
	quitChan        chan struct{}
	pubLoopFunc     func(v *Topic)
	reqToEndCB      ReqToEndFunc
	wg              sync.WaitGroup

	delayedQueue atomic.Value
	isExt        int32
	saveMutex    sync.Mutex
}

func (t *Topic) setExt() {
	atomic.StoreInt32(&t.isExt, 1)
}

func (t *Topic) IsExt() bool {
	return atomic.LoadInt32(&t.isExt) == 1
}

func GetTopicFullName(topic string, part int) string {
	return topic + "-" + strconv.Itoa(part)
}

func NewTopic(topicName string, part int, opt *Options,
	deleteCallback func(*Topic), writeDisabled int32,
	notify func(v interface{}), loopFunc func(v *Topic),
	reqToEnd ReqToEndFunc) *Topic {
	return NewTopicWithExt(topicName, part, false, opt, deleteCallback, writeDisabled, notify, loopFunc, reqToEnd)
}

// Topic constructor
func NewTopicWithExt(topicName string, part int, ext bool, opt *Options,
	deleteCallback func(*Topic), writeDisabled int32,
	notify func(v interface{}), loopFunc func(v *Topic),
	reqToEnd ReqToEndFunc) *Topic {
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
		pubWaitingChan: make(PubInfoChan, 200),
		quitChan:       make(chan struct{}),
		pubLoopFunc:    loopFunc,
		reqToEndCB:     reqToEnd,
	}
	if ext {
		t.setExt()
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
	queue, err := NewDiskQueueWriter(backendName,
		t.dataPath,
		opt.MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(opt.MaxMsgSize)+minValidMsgLength,
		opt.SyncEvery)

	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init disk queue: %v ", t.fullName, err)
		if err == ErrNeedFixQueueStart {
			t.SetDataFixState(true)
		} else {
			t.MarkAsRemoved()
			return nil
		}
	}
	t.backend = queue.(*diskQueueWriter)

	t.UpdateCommittedOffset(t.backend.GetQueueWriteEnd())
	err = t.loadMagicCode()
	if err != nil {
		nsqLog.LogErrorf("topic %v failed to load magic code: %v", t.fullName, err)
		return nil
	}
	t.detailStats = NewDetailStatsInfo(t.TotalDataSize(), t.getHistoryStatsFileName())
	t.notifyCall(t)
	nsqLog.LogDebugf("new topic created: %v", t.tname)

	if t.pubLoopFunc != nil {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.pubLoopFunc(t)
		}()
	}
	t.LoadChannelMeta()
	return t
}

func (t *Topic) GetDelayedQueue() *DelayQueue {
	if t.IsOrdered() {
		return nil
	}

	if t.delayedQueue.Load() == nil {
		return nil
	}
	return t.delayedQueue.Load().(*DelayQueue)
}

func (t *Topic) GetOrCreateDelayedQueueNoLock(idGen MsgIDGenerator) (*DelayQueue, error) {
	if t.delayedQueue.Load() == nil {
		delayedQueue, err := NewDelayQueue(t.tname, t.partition, t.dataPath, t.option, idGen, t.IsExt())
		if err == nil {
			t.delayedQueue.Store(delayedQueue)
			t.channelLock.RLock()
			for _, ch := range t.channelMap {
				ch.SetDelayedQueue(delayedQueue)
			}
			t.channelLock.RUnlock()
		} else {
			return nil, err
		}
	}
	return t.delayedQueue.Load().(*DelayQueue), nil
}

func (t *Topic) GetWaitChan() PubInfoChan {
	return t.pubWaitingChan
}

func (t *Topic) QuitChan() <-chan struct{} {
	return t.quitChan
}

func (t *Topic) IsDataNeedFix() bool {
	return atomic.LoadInt32(&t.needFixData) == 1
}

func (t *Topic) SetDataFixState(needFix bool) {
	if needFix {
		atomic.StoreInt32(&t.needFixData, 1)
	} else {
		atomic.StoreInt32(&t.needFixData, 0)
	}
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
		nsqLog.Infof("remove the magic file %v failed:%v", fileName, err)
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

func (t *Topic) removeHistoryStat() {
	fileName := t.getHistoryStatsFileName()
	err := os.Remove(fileName)
	if err != nil {
		nsqLog.Infof("remove file %v failed:%v", fileName, err)
	}
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
	t.removeHistoryStat()
	t.RemoveChannelMeta()
	t.removeMagicCode()
	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().Delete()
	}
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
	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().SetTrace(enable)
	}
}

func (t *Topic) getChannelMetaFileName() string {
	return path.Join(t.dataPath, "channel_meta"+strconv.Itoa(t.partition))
}

func (t *Topic) LoadChannelMeta() error {
	fn := t.getChannelMetaFileName()
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			nsqLog.LogErrorf("failed to read channel metadata from %s - %s", fn, err)
		}
		return err
	}
	channels := make([]*ChannelMetaInfo, 0)
	err = json.Unmarshal(data, &channels)
	if err != nil {
		nsqLog.LogErrorf("failed to parse metadata - %s", err)
		return err
	}

	for _, ch := range channels {
		channelName := ch.Name
		if !protocol.IsValidChannelName(channelName) {
			nsqLog.LogWarningf("skipping creation of invalid channel %s", channelName)
			continue
		}
		channel := t.GetChannel(channelName)

		if ch.Paused {
			channel.Pause()
		}

		if ch.Skipped {
			channel.Skip()
		}
	}
	return nil
}

func (t *Topic) GetChannelMeta() []ChannelMetaInfo {
	t.channelLock.RLock()
	channels := make([]ChannelMetaInfo, 0, len(t.channelMap))
	for _, channel := range t.channelMap {
		channel.RLock()
		if !channel.ephemeral {
			meta := ChannelMetaInfo{
				Name:    channel.name,
				Paused:  channel.IsPaused(),
				Skipped: channel.IsSkipped(),
			}
			channels = append(channels, meta)
		}
		channel.RUnlock()
	}
	t.channelLock.RUnlock()
	return channels
}

func (t *Topic) SaveChannelMeta() error {
	fileName := t.getChannelMetaFileName()
	channels := make([]*ChannelMetaInfo, 0)
	t.channelLock.RLock()
	for _, channel := range t.channelMap {
		channel.RLock()
		if !channel.ephemeral {
			meta := &ChannelMetaInfo{
				Name:    channel.name,
				Paused:  channel.IsPaused(),
				Skipped: channel.IsSkipped(),
			}
			channels = append(channels, meta)
		}
		channel.RUnlock()
	}
	t.channelLock.RUnlock()
	d, err := json.Marshal(channels)
	if err != nil {
		return err
	}
	t.saveMutex.Lock()
	defer t.saveMutex.Unlock()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(d)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()
	err = util.AtomicRename(tmpFileName, fileName)
	if err != nil {
		return err
	}
	return nil
}

func (t *Topic) RemoveChannelMeta() {
	fileName := t.getChannelMetaFileName()
	err := os.Remove(fileName)
	if err != nil {
		nsqLog.Infof("remove file %v failed:%v", fileName, err)
	}
}

func (t *Topic) getHistoryStatsFileName() string {
	return path.Join(t.dataPath, t.fullName+HISTORY_STAT_FILE_NAME)
}

func (t *Topic) GetDetailStats() *DetailStatsInfo {
	return t.detailStats
}

func (t *Topic) SaveHistoryStats() error {
	if t.Exiting() {
		return ErrExiting
	}
	t.Lock()
	defer t.Unlock()
	return t.detailStats.SaveHistory(t.getHistoryStatsFileName())
}

func (t *Topic) LoadHistoryStats() error {
	return t.detailStats.LoadHistory(t.getHistoryStatsFileName())
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
		nsqLog.LogDebugf("committed is rollbacked: %v, %v", cur, offset)
	}
	t.committedOffset.Store(offset)
	syncEvery := atomic.LoadInt64(&t.dynamicConf.SyncEvery)
	if syncEvery == 1 ||
		offset.TotalMsgCnt()-atomic.LoadInt64(&t.lastSyncCnt) >= syncEvery {
		if !t.IsWriteDisabled() {
			t.flush(true)
		}
	} else {
		t.flushForChannels()
	}
}

func (t *Topic) GetDiskQueueSnapshot() *DiskQueueSnapshot {
	e := t.backend.GetQueueReadEnd()
	commit := t.GetCommitted()
	if commit != nil && e.Offset() > commit.Offset() {
		e = commit
	}
	start := t.backend.GetQueueReadStart()
	d := NewDiskQueueSnapshot(getBackendName(t.tname, t.partition), t.dataPath, e)
	d.SetQueueStart(start)
	return d
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

func (t *Topic) GetDynamicInfo() TopicDynamicConf {
	t.Lock()
	info := *t.dynamicConf
	t.Unlock()
	return info
}

func (t *Topic) IsOrdered() bool {
	return atomic.LoadInt32(&t.isOrdered) == 1
}

func (t *Topic) SetDynamicInfo(dynamicConf TopicDynamicConf, idGen MsgIDGenerator) {
	t.Lock()
	if idGen != nil {
		t.msgIDCursor = idGen
	}
	atomic.StoreInt64(&t.dynamicConf.SyncEvery, dynamicConf.SyncEvery)
	atomic.StoreInt32(&t.dynamicConf.AutoCommit, dynamicConf.AutoCommit)
	atomic.StoreInt32(&t.dynamicConf.RetentionDay, dynamicConf.RetentionDay)
	t.dynamicConf.OrderedMulti = dynamicConf.OrderedMulti
	if dynamicConf.OrderedMulti {
		atomic.StoreInt32(&t.isOrdered, 1)
	} else {
		atomic.StoreInt32(&t.isOrdered, 0)
	}
	if !dynamicConf.OrderedMulti && idGen != nil {
		dq, _ := t.GetOrCreateDelayedQueueNoLock(idGen)
		if dq != nil {
			atomic.StoreInt64(&dq.SyncEvery, dynamicConf.SyncEvery)
		}
	}
	t.dynamicConf.Ext = dynamicConf.Ext
	if dynamicConf.Ext {
		t.setExt()
	}
	nsqLog.Logf("topic dynamic configure changed to %v", dynamicConf)
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

		var ext int32
		if t.IsExt() {
			ext = 1
		} else {
			ext = 0
		}
		channel = NewChannel(t.GetTopicName(), t.GetTopicPart(), channelName, readEnd,
			t.option, deleteCallback, atomic.LoadInt32(&t.writeDisabled),
			t.notifyCall, t.reqToEndCB, ext)

		channel.UpdateQueueEnd(readEnd, false)
		channel.SetDelayedQueue(t.GetDelayedQueue())
		if t.IsWriteDisabled() {
			channel.DisableConsume(true)
		}
		t.channelMap[channelName] = channel
		nsqLog.Logf("TOPIC(%s): new channel(%s), end: %v", t.GetFullName(),
			channel.name, channel.GetChannelEnd())
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

func (t *Topic) CloseExistingChannel(channelName string, deleteData bool) error {
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
	if deleteData {
		channel.Delete()
	} else {
		channel.Close()
	}

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	return t.CloseExistingChannel(channelName, true)
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
	if m.ID > 0 {
		nsqLog.Logf("should not pass id in message while pub: %v", m.ID)
		return 0, 0, 0, nil, ErrInvalidMessageID
	}
	t.Lock()
	defer t.Unlock()
	if m.DelayedType >= MinDelayedType {
		return 0, 0, 0, nil, ErrMessageInvalidDelayedState
	}

	id, offset, writeBytes, dend, err := t.PutMessageNoLock(m)
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

	id, offset, writeBytes, dend, err := t.put(m, true)
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
	_, _, _, dend, err := t.put(m, false)
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
		_, _, _, dend, err = t.put(m, false)
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
	firstCnt := int64(0)
	var diskEnd diskQueueEndInfo
	batchBytes := int32(0)
	for _, m := range msgs {
		if m.ID > 0 {
			nsqLog.Logf("should not pass id in message while pub: %v", m.ID)
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
			return 0, 0, 0, 0, nil, ErrInvalidMessageID
		}
		id, offset, bytes, end, err := t.put(m, true)
		if err != nil {
			t.ResetBackendEndNoLock(wend.Offset(), wend.TotalMsgCnt())
			return firstMsgID, firstOffset, batchBytes, firstCnt, &diskEnd, err
		}
		diskEnd = end
		batchBytes += bytes
		if firstOffset == BackendOffset(-1) {
			firstOffset = offset
			firstMsgID = id
			firstCnt = diskEnd.TotalMsgCnt()
		}
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

func (t *Topic) put(m *Message, trace bool) (MessageID, BackendOffset, int32, diskQueueEndInfo, error) {
	if m.ID <= 0 {
		m.ID = t.nextMsgID()
	}
	offset, writeBytes, dend, err := writeMessageToBackend(t.IsExt(), &t.putBuffer, m, t.backend)
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

	if trace {
		if m.TraceID != 0 || atomic.LoadInt32(&t.EnableTrace) == 1 || nsqLog.Level() >= levellogger.LOG_DETAIL {
			nsqMsgTracer.TracePub(t.GetTopicName(), t.GetTopicPart(), "PUB", m.TraceID, m, offset, dend.TotalMsgCnt())
		}
	}
	// TODO: handle delayed type for dpub and transaction message
	// should remove from delayed queue after written on disk file
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
			oldEnd := channel.GetChannelEnd()
			err := channel.UpdateQueueEnd(e, forceReload)
			if err != nil {
				if err != ErrExiting {
					nsqLog.LogErrorf(
						"failed to update topic end to channel(%s) - %s",
						channel.name, err)
				}
			} else {
				if e.Offset() < oldEnd.Offset() {
					nsqLog.LogWarningf(
						"update topic new end is less than old channel(%s) - %v, %v",
						channel.name, oldEnd, e)
				}
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
	close(t.quitChan)
	t.wg.Wait()

	if deleted {
		t.channelLock.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.channelLock.Unlock()

		if t.GetDelayedQueue() != nil {
			t.GetDelayedQueue().Delete()
		}
		// empty the queue (deletes the backend files, too)
		t.Empty()
		t.removeHistoryStat()
		t.RemoveChannelMeta()
		t.removeMagicCode()
		return t.backend.Delete()
	}

	// write anything leftover to disk
	t.flush(true)
	nsqLog.Logf("[TRACE_DATA] exiting topic end: %v, cnt: %v", t.TotalDataSize(), t.TotalMessageCnt())
	t.SaveChannelMeta()
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

	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().Close()
	}
	return t.backend.Close()
}

func (t *Topic) IsWriteDisabled() bool {
	return atomic.LoadInt32(&t.writeDisabled) == 1
}

func (t *Topic) DisableForSlave() {
	atomic.StoreInt32(&t.writeDisabled, 1)
	nsqLog.Logf("[TRACE_DATA] while disable topic %v end: %v, cnt: %v, queue start: %v", t.GetFullName(),
		t.TotalDataSize(), t.TotalMessageCnt(), t.backend.GetQueueReadStart())
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
	if t.GetDelayedQueue() != nil {
		t.GetDelayedQueue().ForceFlush()
	}

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

// maybe should return the cleaned offset to allow commit log clean
func (t *Topic) TryCleanOldData(retentionSize int64, noRealClean bool, maxCleanOffset BackendOffset) (BackendQueueEnd, error) {
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
		return nil, nil
	}
	cleanStart := t.backend.GetQueueReadStart()
	nsqLog.Logf("clean topic %v data current start: %v, oldest confirmed %v, max clean end: %v",
		t.GetFullName(), cleanStart, oldestPos, maxCleanOffset)
	if cleanStart.Offset()+BackendOffset(retentionSize) >= oldestPos.Offset() {
		return nil, nil
	}

	if oldestPos.Offset() < maxCleanOffset || maxCleanOffset == BackendOffset(0) {
		maxCleanOffset = oldestPos.Offset()
	}
	snapReader := NewDiskQueueSnapshot(getBackendName(t.tname, t.partition), t.dataPath, oldestPos)
	snapReader.SetQueueStart(cleanStart)
	err := snapReader.SeekTo(cleanStart.Offset())
	if err != nil {
		nsqLog.Errorf("topic: %v failed to seek to %v: %v", t.GetFullName(), cleanStart, err)
		return nil, err
	}
	readInfo := snapReader.GetCurrentReadQueueOffset()
	data := snapReader.ReadOne()
	if data.Err != nil {
		return nil, data.Err
	}
	var cleanEndInfo BackendQueueOffset
	t.Lock()
	retentionDay := atomic.LoadInt32(&t.dynamicConf.RetentionDay)
	if retentionDay == 0 {
		retentionDay = int32(DEFAULT_RETENTION_DAYS)
	}
	cleanTime := time.Now().Add(-1 * time.Hour * 24 * time.Duration(retentionDay))
	t.Unlock()
	for {
		if retentionSize > 0 {
			// clean data ignore the retention day
			// only keep the retention size (start from the last consumed)
			if data.Offset > maxCleanOffset-BackendOffset(retentionSize) {
				break
			}
			cleanEndInfo = readInfo
		} else {
			msg, decodeErr := decodeMessage(data.Data, t.IsExt())
			if decodeErr != nil {
				nsqLog.LogErrorf("failed to decode message - %s - %v", decodeErr, data)
			} else {
				if msg.Timestamp >= cleanTime.UnixNano() {
					break
				}
				if data.Offset >= maxCleanOffset {
					break
				}
				cleanEndInfo = readInfo
			}
		}
		err = snapReader.SkipToNext()
		if err != nil {
			nsqLog.Logf("failed to skip - %s ", err)
			break
		}
		readInfo = snapReader.GetCurrentReadQueueOffset()
		data = snapReader.ReadOne()
		if data.Err != nil {
			nsqLog.LogErrorf("failed to read - %s ", data.Err)
			break
		}
	}

	nsqLog.Infof("clean topic %v data from %v under retention %v, %v",
		t.GetFullName(), cleanEndInfo, cleanTime, retentionSize)
	if cleanEndInfo == nil || cleanEndInfo.Offset()+BackendOffset(retentionSize) >= maxCleanOffset {
		if cleanEndInfo != nil {
			nsqLog.Warningf("clean topic %v data at position: %v could not exceed current oldest confirmed %v and max clean end: %v",
				t.GetFullName(), cleanEndInfo, oldestPos, maxCleanOffset)
		}
		return nil, nil
	}
	return t.backend.CleanOldDataByRetention(cleanEndInfo, noRealClean, maxCleanOffset)
}

func (t *Topic) ResetBackendWithQueueStartNoLock(queueStartOffset int64, queueStartCnt int64) error {
	if !t.IsWriteDisabled() {
		nsqLog.Warningf("reset the topic %v backend only allow while write disabled", t.GetFullName())
		return ErrOperationInvalidState
	}
	if queueStartOffset < 0 || queueStartCnt < 0 {
		return errors.New("queue start should not less than 0")
	}
	queueStart := t.backend.GetQueueWriteEnd().(*diskQueueEndInfo)
	queueStart.virtualEnd = BackendOffset(queueStartOffset)
	queueStart.totalMsgCnt = queueStartCnt
	nsqLog.Warningf("reset the topic %v backend with queue start: %v", t.GetFullName(), queueStart)
	err := t.backend.ResetWriteWithQueueStart(queueStart)
	if err != nil {
		return err
	}
	newEnd := t.backend.GetQueueReadEnd()
	t.UpdateCommittedOffset(newEnd)

	t.channelLock.Lock()
	for _, ch := range t.channelMap {
		nsqLog.Infof("channel stats: %v", ch.GetChannelDebugStats())
		ch.UpdateQueueEnd(newEnd, true)
		ch.ConfirmBackendQueueOnSlave(newEnd.Offset(), newEnd.TotalMsgCnt(), true)
	}
	t.channelLock.Unlock()
	return nil
}

func (t *Topic) GetDelayedQueueConsumedState() (RecentKeyList, map[int]uint64, map[string]uint64) {
	if t.IsOrdered() {
		return nil, nil, nil
	}
	dq := t.GetDelayedQueue()
	if dq == nil {
		return nil, nil, nil
	}
	chList := make([]string, 0)
	t.channelLock.RLock()
	for _, ch := range t.channelMap {
		chList = append(chList, ch.GetName())
	}
	t.channelLock.RUnlock()
	return dq.GetOldestConsumedState(chList)
}

func (t *Topic) UpdateDelayedQueueConsumedState(keyList RecentKeyList, cntList map[int]uint64, channelCntList map[string]uint64) error {
	if t.IsOrdered() {
		nsqLog.Infof("should never delayed queue in ordered topic: %v", t.GetFullName())
		return nil
	}
	dq := t.GetDelayedQueue()
	if dq == nil {
		nsqLog.Infof("no delayed queue while update delayed state on topic: %v", t.GetFullName())
		return nil
	}

	return dq.UpdateConsumedState(keyList, cntList, channelCntList)
}
