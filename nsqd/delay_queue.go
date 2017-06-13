package nsqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type KVStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	GetLatestBinLogOffset() (int64, error)
}

func writeDelayedMessageToBackend(buf *bytes.Buffer, msg *Message, bq *diskQueueWriter) (BackendOffset, int32, diskQueueEndInfo, error) {
	buf.Reset()
	_, err := msg.WriteDelayedTo(buf)
	if err != nil {
		return 0, 0, diskQueueEndInfo{}, err
	}
	return bq.PutV2(buf.Bytes())
}

type DelayQueue struct {
	sync.Mutex

	tname     string
	fullName  string
	partition int
	backend   *diskQueueWriter
	dataPath  string
	flushChan chan int
	exitFlag  int32

	msgIDCursor  MsgIDGenerator
	defaultIDSeq uint64

	option      *Options
	needFlush   int32
	lastSyncCnt int64
	putBuffer   bytes.Buffer
	quitChan    chan struct{}
	wg          sync.WaitGroup
	kvStore     KVStore
}

func NewDelayQueue(idGen MsgIDGenerator) *DelayQueue {
	return &DelayQueue{msgIDCursor: idGen}
}

func (q *DelayQueue) GetFullName() string {
	return q.fullName
}

func (q *DelayQueue) GetTopicName() string {
	return q.tname
}

func (q *DelayQueue) GetTopicPart() int {
	return q.partition
}

func (q *DelayQueue) SetMsgGenerator(idGen MsgIDGenerator) {
	q.Lock()
	q.msgIDCursor = idGen
	q.Unlock()
}

func (q *DelayQueue) GetMsgGenerator() MsgIDGenerator {
	q.Lock()
	cursor := q.msgIDCursor
	q.Unlock()
	return cursor
}

func (q *DelayQueue) SetDynamicInfo(idGen MsgIDGenerator) {
	q.Lock()
	if idGen != nil {
		q.msgIDCursor = idGen
	}
	q.Unlock()
}

func (q *DelayQueue) nextMsgID() MessageID {
	id := uint64(0)
	if q.msgIDCursor != nil {
		id = q.msgIDCursor.NextID()
	} else {
		id = atomic.AddUint64(&q.defaultIDSeq, 1)
	}
	return MessageID(id)
}

// PutMessage writes a Message to the queue
func (q *DelayQueue) PutDelayMessage(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	q.Lock()
	id, offset, writeBytes, dend, err := q.PutDelayMessageNoLock(m)
	q.Unlock()
	return id, offset, writeBytes, dend, err
}

func (q *DelayQueue) PutDelayMessageNoLock(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	if atomic.LoadInt32(&q.exitFlag) == 1 {
		return 0, 0, 0, nil, errors.New("exiting")
	}
	if m.ID > 0 {
		nsqLog.Logf("should pass id in message ")
		return 0, 0, 0, nil, ErrInvalidMessageID
	}
	if m.DelayedChannel == "" {
		return 0, 0, 0, nil, errors.New("invalid delayed message with no channel")
	}

	id, offset, writeBytes, dend, err := q.put(m, true)
	return id, offset, writeBytes, &dend, err
}

func (q *DelayQueue) PutDelayMessageOnReplica(m *Message, offset BackendOffset) (BackendQueueEnd, error) {
	if atomic.LoadInt32(&q.exitFlag) == 1 {
		return nil, ErrExiting
	}
	wend := q.backend.GetQueueWriteEnd()
	if wend.Offset() != offset {
		nsqLog.LogErrorf("topic %v: write offset mismatch: %v, %v", q.GetFullName(), offset, wend)
		return nil, ErrWriteOffsetMismatch
	}
	if m.DelayedChannel == "" {
		return nil, errors.New("invalid delayed message with no channel")
	}

	_, _, _, dend, err := q.put(m, false)
	if err != nil {
		return nil, err
	}
	return &dend, nil
}

func (q *DelayQueue) put(m *Message, trace bool) (MessageID, BackendOffset, int32, diskQueueEndInfo, error) {
	if m.ID <= 0 {
		m.ID = q.nextMsgID()
	}

	offset, writeBytes, dend, err := writeDelayedMessageToBackend(&q.putBuffer, m, q.backend)
	atomic.StoreInt32(&q.needFlush, 1)
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write delayed message to backend - %s",
			q.GetFullName(), err)
		return m.ID, offset, writeBytes, dend, err
	}

	var msgKey [16]byte
	binary.BigEndian.PutUint64(msgKey[:8], uint64(m.DelayedTs))
	binary.BigEndian.PutUint64(msgKey[8:16], uint64(m.ID))

	err = q.kvStore.Put(msgKey[:], q.putBuffer.Bytes())
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to write delayed message %v to kv store- %s",
			q.GetFullName(), m, err)
	}

	return m.ID, offset, writeBytes, dend, nil
}

func (q *DelayQueue) Delete() error {
	return q.exit(true)
}

func (q *DelayQueue) Close() error {
	return q.exit(false)
}

func (q *DelayQueue) exit(deleted bool) error {
	q.Lock()
	defer q.Unlock()
	if !atomic.CompareAndSwapInt32(&q.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	close(q.quitChan)
	q.wg.Wait()

	if deleted {
		// empty the queue (deletes the backend files, too)
		// TODO remove kv store
		return q.backend.Delete()
	}

	// write anything leftover to disk
	q.flush()
	return q.backend.Close()
}

func (q *DelayQueue) ForceFlush() {
	s := time.Now()
	q.flush()
	cost := time.Now().Sub(s)
	if cost > time.Second {
		nsqLog.Logf("topic(%s): flush cost: %v", q.GetFullName(), cost)
	}
}

func (q *DelayQueue) flush() error {
	ok := atomic.CompareAndSwapInt32(&q.needFlush, 1, 0)
	if !ok {
		return nil
	}
	atomic.StoreInt64(&q.lastSyncCnt, q.backend.GetQueueWriteEnd().TotalMsgCnt())
	err := q.backend.Flush()
	if err != nil {
		nsqLog.LogErrorf("failed flush: %v", err)
		return err
	}
	return err
}
