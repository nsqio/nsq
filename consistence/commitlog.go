package consistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

const (
	DEFAULT_COMMIT_BUF_SIZE = 1024
)

var (
	ErrCommitLogWrongID       = errors.New("commit log id is wrong")
	ErrCommitLogIDNotFound    = errors.New("commit log id is not found")
	ErrCommitLogOutofBound    = errors.New("commit log offset is out of bound")
	ErrCommitLogEOF           = errors.New("commit log end of file")
	ErrCommitLogOffsetInvalid = errors.New("commit log offset is invalid")
)

// message data file + check point file.
// message on memory and replica, flush to disk and write the check point file.
type CommitLogData struct {
	LogID int64
	// epoch for the topic leader
	Epoch     int32
	MsgOffset int64
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
	appender      *os.File
}

func GetTopicPartitionLogPath(basepath, t string, p int) string {
	fullpath := filepath.Join(basepath, GetTopicPartitionFileName(t, p, ".commit.log"))
	return fullpath
}

func InitTopicCommitLogMgr(t string, p int, basepath string, commitBufSize int) (*TopicCommitLogMgr, error) {
	fullpath := GetTopicPartitionLogPath(basepath, t, p)
	mgr := &TopicCommitLogMgr{
		topic:         t,
		partition:     p,
		nLogID:        0,
		pLogID:        0,
		path:          fullpath,
		committedLogs: make([]CommitLogData, 0, commitBufSize),
	}
	// load check point index. read sizeof(CommitLogData) until EOF.
	var err error
	// note: using append mode can make sure write only to end of file
	// we can do random read without affecting the append behavior
	mgr.appender, err = os.OpenFile(mgr.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
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
	if fsize > 0 {
		num := fsize / int64(GetLogDataSize())
		roundOffset := (num - 1) * int64(GetLogDataSize())
		l, err := mgr.GetCommmitLogFromOffset(roundOffset)
		if err != nil {
			coordLog.Infof("load file error: %v", err)
			return nil, err
		}
		mgr.pLogID = l.LogID
		mgr.nLogID = l.LogID + 1000
	} else {
		mgr.nLogID = 1
	}
	return mgr, nil
}

func (self *TopicCommitLogMgr) NextID() int64 {
	id := atomic.AddInt64(&self.nLogID, 1)
	return id
}

func (self *TopicCommitLogMgr) TruncateToOffset(offset int64) (*CommitLogData, error) {
	err := self.appender.Truncate(offset)
	if err != nil {
		return nil, err
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

	self.pLogID = l.LogID
	return &l, nil
}

func (self *TopicCommitLogMgr) GetCommmitLogFromOffset(offset int64) (*CommitLogData, error) {
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

func (self *TopicCommitLogMgr) GetLastLogOffset() (int64, error) {
	f, err := self.appender.Stat()
	if err != nil {
		return 0, err
	}
	fsize := f.Size()
	num := fsize / int64(GetLogDataSize())
	roundOffset := (num - 1) * int64(GetLogDataSize())
	for {
		l, err := self.GetCommmitLogFromOffset(roundOffset)
		if err != nil {
			return 0, err
		}
		if l.LogID == self.pLogID {
			return roundOffset, nil
		} else if l.LogID < self.pLogID {
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
	return self.pLogID
}

func (self *TopicCommitLogMgr) IsCommitted(id int64) bool {
	if self.pLogID == id {
		return true
	}
	return false
}

func (self *TopicCommitLogMgr) AppendCommitLog(l *CommitLogData, slave bool) error {
	if l.LogID <= self.pLogID {
		return ErrCommitLogWrongID
	}
	if slave {
		self.nLogID = l.LogID + 1
	}
	if cap(self.committedLogs) == 0 {
		// no buffer, write to file directly.
		err := binary.Write(self.appender, binary.BigEndian, l)
		if err != nil {
			return err
		}
	} else {
		if len(self.committedLogs) >= cap(self.committedLogs) {
			self.FlushCommitLogs()
		}
		self.committedLogs = append(self.committedLogs, *l)
	}
	self.pLogID = l.LogID
	return nil
}

func (self *TopicCommitLogMgr) FlushCommitLogs() {
	// write buffered commit logs to file.
	for _, v := range self.committedLogs {
		err := binary.Write(self.appender, binary.BigEndian, v)
		if err != nil {
			panic(err)
		}
	}
	self.committedLogs = self.committedLogs[0:0]
}

func (self *TopicCommitLogMgr) GetCommitLogs(startOffset int64, num int) ([]CommitLogData, error) {
	f, err := self.appender.Stat()
	if err != nil {
		return nil, err
	}
	fsize := f.Size()

	if startOffset > fsize-int64(GetLogDataSize()) {
		return nil, ErrCommitLogOutofBound
	}
	if (startOffset % int64(GetLogDataSize())) != 0 {
		return nil, ErrCommitLogOffsetInvalid
	}
	b := bytes.NewBuffer(make([]byte, GetLogDataSize()*num))
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

func (self *TopicCommitLogMgr) GetCommitLogsReverse(startIndex int64, num int) ([]CommitLogData, error) {
	ret := make([]CommitLogData, 0, num)
	for i := startIndex; i < int64(len(self.committedLogs)); i++ {
		ret = append(ret, self.committedLogs[len(self.committedLogs)-int(i)-1])
		if len(ret) >= num {
			return ret, nil
		}
	}
	dataSize := GetLogDataSize()
	// TODO: read from end of commit file.
	endOffset := 0
	readStart := endOffset - dataSize*(num-len(ret))
	if readStart < 0 {
		readStart = 0
	}
	buf := make([]byte, endOffset-readStart)
	// TODO: read file data to buf
	var tmp CommitLogData
	for i := 0; i < len(buf)-dataSize; i++ {
		err := binary.Read(bytes.NewReader(buf[i:i+dataSize]), binary.BigEndian, &tmp)
		if err != nil {
			return nil, err
		}
		ret = append(ret, tmp)
		i = i + dataSize
	}
	return ret, nil
}
