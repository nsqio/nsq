package nsqd

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/boltdb/bolt"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	syncedOffsetKey  = []byte("synced_offset")
	bucketDelayedMsg = []byte("delayed_message")
	bucketMeta       = []byte("meta")
)

func writeDelayedMessageToBackend(buf *bytes.Buffer, msg *Message, bq *diskQueueWriter) (BackendOffset, int32, diskQueueEndInfo, error) {
	buf.Reset()
	_, err := msg.WriteDelayedTo(buf)
	if err != nil {
		return 0, 0, diskQueueEndInfo{}, err
	}
	return bq.PutV2(buf.Bytes())
}

type DelayQueue struct {
	tname     string
	fullName  string
	partition int
	backend   *diskQueueWriter
	dataPath  string
	exitFlag  int32

	msgIDCursor  MsgIDGenerator
	defaultIDSeq uint64

	needFlush int32
	putBuffer bytes.Buffer
	kvStore   *bolt.DB
}

func NewDelayQueue(topicName string, part int, dataPath string, opt *Options,
	idGen MsgIDGenerator) (*DelayQueue, error) {
	q := &DelayQueue{
		tname:       topicName,
		partition:   part,
		putBuffer:   bytes.Buffer{},
		dataPath:    dataPath,
		msgIDCursor: idGen,
	}
	q.fullName = GetTopicFullName(q.tname, q.partition)
	backendName := getDelayQueueBackendName(q.tname, q.partition)
	// max delay message size need add the delay ts and channel name
	queue, err := NewDiskQueueWriter(backendName,
		q.dataPath,
		opt.MaxBytesPerFile,
		int32(minValidMsgLength),
		int32(opt.MaxMsgSize)+minValidMsgLength+8+255, 0)

	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init delayed disk queue: %v , %v ", q.fullName, err, backendName)
		return nil, err
	}
	q.backend = queue.(*diskQueueWriter)
	q.kvStore, err = bolt.Open(path.Join(q.dataPath, getDelayQueueDBName(q.tname, q.partition)), 0644, nil)
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init delayed db: %v , %v ", q.fullName, err, backendName)
		return nil, err
	}
	err = q.kvStore.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketDelayedMsg)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketMeta)
		return err
	})
	if err != nil {
		nsqLog.LogErrorf("topic(%v) failed to init delayed db: %v , %v ", q.fullName, err, backendName)
		return nil, err
	}

	return q, nil
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

func (q *DelayQueue) nextMsgID() MessageID {
	id := uint64(0)
	if q.msgIDCursor != nil {
		id = q.msgIDCursor.NextID()
	} else {
		id = atomic.AddUint64(&q.defaultIDSeq, 1)
	}
	return MessageID(id)
}

func (q *DelayQueue) PutDelayMessage(m *Message) (MessageID, BackendOffset, int32, BackendQueueEnd, error) {
	if atomic.LoadInt32(&q.exitFlag) == 1 {
		return 0, 0, 0, nil, errors.New("exiting")
	}
	if m.ID > 0 {
		nsqLog.Logf("should not pass id in message ")
		return 0, 0, 0, nil, ErrInvalidMessageID
	}
	if !IsValidDelayedMessage(m) {
		return 0, 0, 0, nil, errors.New("invalid delayed message")
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
	if !IsValidDelayedMessage(m) {
		return nil, errors.New("invalid delayed message")
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

	err = q.kvStore.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDelayedMsg)
		exists := b.Get(msgKey[:]) != nil
		err := b.Put(msgKey[:], q.putBuffer.Bytes())
		if err != nil {
			return err
		}
		b = tx.Bucket(bucketMeta)
		if !exists {
			cntKey := []byte("counter_" + strconv.Itoa(int(m.DelayedType)) + "_" + m.DelayedChannel)
			cnt := uint64(0)
			cntBytes := b.Get(cntKey)
			if cntBytes != nil {
				cnt = binary.BigEndian.Uint64(cntBytes)
			}
			cnt++
			binary.BigEndian.PutUint64(cntBytes[:8], cnt)
			err = b.Put(cntKey, cntBytes)
			if err != nil {
				return err
			}
		}
		return b.Put(syncedOffsetKey, []byte(strconv.Itoa(int(dend.Offset()))))
	})
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
	if !atomic.CompareAndSwapInt32(&q.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	q.kvStore.Close()
	if deleted {
		os.RemoveAll(path.Join(q.dataPath, getDelayQueueDBName(q.tname, q.partition)))
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
	err := q.backend.Flush()
	if err != nil {
		nsqLog.LogErrorf("failed flush: %v", err)
		return err
	}
	q.kvStore.Sync()
	return err
}

func (q *DelayQueue) PeekRecentTimeoutWithFilter(results []*Message, peekTs int64, filterType int,
	filterChannel string) (int, error) {
	db := q.kvStore
	idx := 0
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDelayedMsg)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			delayedTs := int64(binary.BigEndian.Uint64(k[:8]))
			if delayedTs > peekTs || idx >= len(results) {
				break
			}

			m, err := DecodeDelayedMessage(v)
			if err != nil {
				nsqLog.LogErrorf("failed to decode delayed message: %v, %v", m, err)
				continue
			}
			if filterType >= 0 && filterType != int(m.DelayedType) {
				continue
			}
			if len(filterChannel) > 0 && filterChannel != m.DelayedChannel {
				continue
			}
			results[idx] = m
			idx++
		}
		return nil
	})
	return idx, err
}

func (q *DelayQueue) PeekRecentTimeout(results []*Message) (int, error) {
	return q.PeekRecentTimeoutWithFilter(results, time.Now().UnixNano(), -1, "")
}

func (q *DelayQueue) PeekAll(results []*Message) (int, error) {
	return q.PeekRecentTimeoutWithFilter(results, time.Now().Add(time.Hour*24*365).UnixNano(), -1, "")
}

func (q *DelayQueue) GetSyncedOffset() (BackendOffset, error) {
	var synced BackendOffset
	err := q.kvStore.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		v := b.Get(syncedOffsetKey)
		offset, err := strconv.Atoi(string(v))
		if err != nil {
			return err
		}
		synced = BackendOffset(offset)
		return nil
	})
	if err != nil {
		nsqLog.LogErrorf("failed to get synced offset: %v", err)
	}
	return synced, err
}

func (q *DelayQueue) ConfirmedMessage(msgID MessageID, delayedTs int64) error {
	var msgKey [16]byte
	binary.BigEndian.PutUint64(msgKey[:8], uint64(delayedTs))
	binary.BigEndian.PutUint64(msgKey[8:16], uint64(msgID))

	err := q.kvStore.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketDelayedMsg)
		oldV := b.Get(msgKey[:])
		err := b.Delete(msgKey[:])
		if err != nil {
			return err
		}
		if oldV != nil {
			m, err := DecodeDelayedMessage(oldV)
			if err != nil {
				nsqLog.Infof("failed to decode delayed message: %v, %v", oldV, err)
				return err
			}
			cntKey := []byte("counter_" + strconv.Itoa(int(m.DelayedType)) + "_" + m.DelayedChannel)
			cnt := uint64(0)
			cntBytes := b.Get(cntKey)
			if cntBytes != nil {
				cnt = binary.BigEndian.Uint64(cntBytes)
			}
			if cnt > 0 {
				cnt--
				binary.BigEndian.PutUint64(cntBytes[:8], cnt)
				err = b.Put(cntKey, cntBytes)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		nsqLog.LogErrorf(
			"TOPIC(%s) : failed to delete delayed message %v-%v, %v",
			q.GetFullName(), msgID, delayedTs, err)
	}
	return err
}
