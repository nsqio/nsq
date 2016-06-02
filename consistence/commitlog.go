package consistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	DEFAULT_COMMIT_BUF_SIZE = 1024
	MAX_INCR_ID_BIT         = 50
)

var (
	ErrCommitLogWrongID         = errors.New("commit log id is wrong")
	ErrCommitLogWrongLastID     = errors.New("commit log last id should no less than log id")
	ErrCommitLogIDNotFound      = errors.New("commit log id is not found")
	ErrCommitLogOutofBound      = errors.New("commit log offset is out of bound")
	ErrCommitLogEOF             = errors.New("commit log end of file")
	ErrCommitLogOffsetInvalid   = errors.New("commit log offset is invalid")
	ErrCommitLogPartitionExceed = errors.New("commit log partition id is exceeded")
)

type CommitLogData struct {
	LogID int64
	// epoch for the topic leader
	Epoch EpochType
	// if single message, this should be the same with logid
	// for multiple messages, this would be the logid for the last message in batch
	LastMsgLogID int64
	MsgOffset    int64
	// size for batch messages
	MsgSize int32
	// the total message count for all from begin, not only this batch
	MsgCnt int64
}

var emptyLogData CommitLogData

func GetLogDataSize() int {
	return binary.Size(emptyLogData)
}

func GetPrevLogOffset(cur int64) int64 {
	return cur - int64(GetLogDataSize())
}

func GetNextLogOffset(cur int64) int64 {
	return cur + int64(GetLogDataSize())
}

type TopicCommitLogMgr struct {
	topic         string
	partition     int
	nLogID        int64
	pLogID        int64
	path          string
	committedLogs []CommitLogData
	bufSize       int
	appender      *os.File
	sync.Mutex
}

func GetTopicPartitionLogPath(basepath, t string, p int) string {
	fullpath := filepath.Join(basepath, GetTopicPartitionFileName(t, p, ".commit.log"))
	return fullpath
}

func InitTopicCommitLogMgr(t string, p int, basepath string, commitBufSize int) (*TopicCommitLogMgr, error) {
	if uint64(p) >= uint64(1)<<(63-MAX_INCR_ID_BIT) {
		return nil, ErrCommitLogPartitionExceed
	}
	fullpath := GetTopicPartitionLogPath(basepath, t, p)
	mgr := &TopicCommitLogMgr{
		topic:         t,
		partition:     p,
		nLogID:        0,
		pLogID:        0,
		path:          fullpath,
		bufSize:       commitBufSize,
		committedLogs: make([]CommitLogData, 0, commitBufSize),
	}
	// load check point index. read sizeof(CommitLogData) until EOF.
	var err error
	// note: using append mode can make sure write only to end of file
	// we can do random read without affecting the append behavior
	mgr.appender, err = os.OpenFile(mgr.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		coordLog.Infof("open topic commit log file error: %v", err)
		return nil, err
	}

	//load meta
	f, err := mgr.appender.Stat()
	if err != nil {
		coordLog.Infof("stat file error: %v", err)
		return nil, err
	}
	fsize := f.Size()
	// read latest logid and incr. combine the partition id at high.
	if fsize > 0 {
		num := fsize / int64(GetLogDataSize())
		roundOffset := (num - 1) * int64(GetLogDataSize())
		l, err := mgr.GetCommitLogFromOffset(roundOffset)
		if err != nil {
			coordLog.Infof("load file error: %v", err)
			return nil, err
		}
		mgr.pLogID = l.LogID
		mgr.nLogID = l.LastMsgLogID + 1
		if mgr.pLogID < int64(uint64(mgr.partition)<<MAX_INCR_ID_BIT) {
			coordLog.Infof("log id init less than expected: %v", mgr.pLogID)
			panic("init commit log id failed")
		} else if mgr.pLogID > int64(uint64(mgr.partition+1)<<MAX_INCR_ID_BIT+1) {
			coordLog.Infof("log id init large than expected: %v", mgr.pLogID)
			panic("init commit log id failed")
		}
		if mgr.nLogID < int64(uint64(mgr.partition)<<MAX_INCR_ID_BIT) {
			coordLog.Infof("log id init less than expected: %v", mgr.pLogID)
			panic("init commit log id failed")
		} else if mgr.nLogID > int64(uint64(mgr.partition+1)<<MAX_INCR_ID_BIT+1) {
			coordLog.Infof("log id init large than expected: %v", mgr.pLogID)
			panic("init commit log id failed")
		}
	} else {
		mgr.nLogID = int64(uint64(mgr.partition)<<MAX_INCR_ID_BIT + 1)
	}
	return mgr, nil
}

func (self *TopicCommitLogMgr) Delete() {
	self.Lock()
	self.flushCommitLogsNoLock()
	self.appender.Sync()
	self.appender.Close()
	err := os.Remove(self.path)
	if err != nil {
		coordLog.Warningf("failed to remove the commit log for topic: %v", self.path)
	}
	self.Unlock()
}

func (self *TopicCommitLogMgr) Close() {
	self.Lock()
	self.flushCommitLogsNoLock()
	self.appender.Sync()
	self.appender.Close()
	self.Unlock()
}

func (self *TopicCommitLogMgr) NextID() uint64 {
	id := atomic.AddInt64(&self.nLogID, 1)
	return uint64(id)
}

// reset the nextid
func (self *TopicCommitLogMgr) Reset(id uint64) {
}

func (self *TopicCommitLogMgr) TruncateToOffset(offset int64) (*CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	self.flushCommitLogsNoLock()
	err := self.appender.Truncate(offset)
	if err != nil {
		return nil, err
	}
	if offset == 0 {
		atomic.StoreInt64(&self.pLogID, 0)
		return nil, nil
	}
	b := bytes.NewBuffer(make([]byte, GetLogDataSize()))
	n, err := self.appender.ReadAt(b.Bytes(), offset-int64(GetLogDataSize()))
	if err != nil {
		return nil, err
	}
	if n != GetLogDataSize() {
		return nil, ErrCommitLogOffsetInvalid
	}
	var l CommitLogData
	err = binary.Read(b, binary.BigEndian, &l)
	if err != nil {
		return nil, err
	}

	atomic.StoreInt64(&self.pLogID, l.LogID)
	if l.LastMsgLogID+1 > self.nLogID {
		atomic.StoreInt64(&self.nLogID, l.LastMsgLogID+1)
	}
	return &l, nil
}

func (self *TopicCommitLogMgr) getCommitLogFromOffsetNoLock(offset int64) (*CommitLogData, error) {
	self.flushCommitLogsNoLock()
	f, err := self.appender.Stat()
	if err != nil {
		return nil, err
	}
	fsize := f.Size()
	if offset == fsize {
		return nil, ErrCommitLogEOF
	}

	if offset > fsize {
		return nil, ErrCommitLogOutofBound
	}
	if (offset % int64(GetLogDataSize())) != 0 {
		return nil, ErrCommitLogOffsetInvalid
	}
	b := bytes.NewBuffer(make([]byte, GetLogDataSize()))
	n, err := self.appender.ReadAt(b.Bytes(), offset)
	if err != nil {
		return nil, err
	}
	if n != GetLogDataSize() {
		return nil, ErrCommitLogOffsetInvalid
	}
	var l CommitLogData
	err = binary.Read(b, binary.BigEndian, &l)
	return &l, err
}

func (self *TopicCommitLogMgr) GetCommitLogFromOffset(offset int64) (*CommitLogData, error) {
	self.Lock()
	ret, err := self.getCommitLogFromOffsetNoLock(offset)
	self.Unlock()
	return ret, err
}

func (self *TopicCommitLogMgr) GetLastLogOffset() (int64, error) {
	self.Lock()
	defer self.Unlock()
	self.flushCommitLogsNoLock()
	f, err := self.appender.Stat()
	if err != nil {
		return 0, err
	}
	fsize := f.Size()
	if fsize == 0 {
		return 0, nil
	}
	num := fsize / int64(GetLogDataSize())
	roundOffset := (num - 1) * int64(GetLogDataSize())
	for {
		l, err := self.getCommitLogFromOffsetNoLock(roundOffset)
		if err != nil {
			return 0, err
		}
		if l.LogID == atomic.LoadInt64(&self.pLogID) {
			return roundOffset, nil
		} else if l.LogID < atomic.LoadInt64(&self.pLogID) {
			break
		}
		roundOffset -= int64(GetLogDataSize())
		if roundOffset < 0 {
			break
		}
	}
	return 0, ErrCommitLogIDNotFound
}

func (self *TopicCommitLogMgr) GetLastCommitLogID() int64 {
	return atomic.LoadInt64(&self.pLogID)
}

func (self *TopicCommitLogMgr) IsCommitted(id int64) bool {
	if atomic.LoadInt64(&self.pLogID) == id {
		return true
	}
	return false
}

func (self *TopicCommitLogMgr) AppendCommitLog(l *CommitLogData, slave bool) error {
	if l.LogID <= atomic.LoadInt64(&self.pLogID) {
		coordLog.Errorf("commit id %v less than prev id: %v", l, self.pLogID)
		return ErrCommitLogWrongID
	}
	if l.LastMsgLogID < l.LogID {
		coordLog.Errorf("commit id %v less than last msgid", l)
		return ErrCommitLogWrongLastID
	}
	self.Lock()
	defer self.Unlock()
	if slave {
		atomic.StoreInt64(&self.nLogID, l.LastMsgLogID+1)
	}
	if cap(self.committedLogs) == 0 {
		// no buffer, write to file directly.
		err := binary.Write(self.appender, binary.BigEndian, l)
		if err != nil {
			return err
		}
	} else {
		if len(self.committedLogs) >= cap(self.committedLogs) {
			self.flushCommitLogsNoLock()
		}
		self.committedLogs = append(self.committedLogs, *l)
	}
	atomic.StoreInt64(&self.pLogID, l.LogID)
	return nil
}

func (self *TopicCommitLogMgr) switchForMaster(master bool) {
	self.Lock()
	self.flushCommitLogsNoLock()
	if master {
		if cap(self.committedLogs) != self.bufSize {
			self.committedLogs = make([]CommitLogData, 0, self.bufSize)
		}
	} else {
		if cap(self.committedLogs) != self.bufSize*100 {
			self.committedLogs = make([]CommitLogData, 0, self.bufSize*100)
		}
	}
	self.Unlock()
}

func (self *TopicCommitLogMgr) flushCommitLogsNoLock() {
	// write buffered commit logs to file.
	for _, v := range self.committedLogs {
		err := binary.Write(self.appender, binary.BigEndian, v)
		if err != nil {
			panic(err)
		}
	}
	self.committedLogs = self.committedLogs[0:0]
}

func (self *TopicCommitLogMgr) FlushCommitLogs() {
	self.Lock()
	self.flushCommitLogsNoLock()
	self.Unlock()
}

func (self *TopicCommitLogMgr) GetCommitLogs(startOffset int64, num int) ([]CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	self.flushCommitLogsNoLock()
	f, err := self.appender.Stat()
	if err != nil {
		return nil, err
	}
	fsize := f.Size()
	if startOffset == fsize {
		return nil, nil
	}
	if startOffset > fsize-int64(GetLogDataSize()) {
		return nil, ErrCommitLogOutofBound
	}
	if (startOffset % int64(GetLogDataSize())) != 0 {
		return nil, ErrCommitLogOffsetInvalid
	}
	needRead := int64(num * GetLogDataSize())
	if startOffset+needRead > fsize {
		needRead = fsize - startOffset
	}
	b := bytes.NewBuffer(make([]byte, needRead))
	n, err := self.appender.ReadAt(b.Bytes(), startOffset)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	logList := make([]CommitLogData, 0, n/GetLogDataSize())
	var l CommitLogData
	for n > 0 {
		err := binary.Read(b, binary.BigEndian, &l)
		if err != nil {
			return nil, err
		}
		logList = append(logList, l)
		n -= GetLogDataSize()
	}
	return logList, err
}
