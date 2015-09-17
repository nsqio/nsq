package consistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/golang/glog"
	"os"
	"path/filepath"
	"sync"
)

const (
	DEFAULT_COMMIT_BUF_SIZE = 1024
)

var (
	ErrCommitLogId         = errors.New("commit log id is wrong")
	ErrCommitLogIdNotFound = errors.New("commit log id is not found")
)

// message data file + check point file.
// message on memory and replica, flush to disk and write the check point file.
type CommitLogData struct {
	LogId int64
	// epoch for the topic leader
	Epoch     int
	MsgFileId int
	MsgOffset int
}

var emptyLogData CommitLogData

func GetLogDataSize() int {
	return binary.Size(emptyLogData)
}

type TopicCommitLogMgr struct {
	sync.Mutex
	topic         string
	partition     int
	nLogId        int64
	pLogId        int64
	path          string
	checkPoints   map[int64]struct{}
	committedLogs []CommitLogData
	appender      *os.File
}

func InitTopicCommitLogMgr(t string, p int, basepath string, commitBufSize int) *TopicCommitLogMgr {
	fullpath := filepath.Join(basepath, "commit.log")
	mgr := &TopicCommitLogMgr{
		topic:         t,
		partition:     p,
		nLogId:        0,
		pLogId:        0,
		path:          fullpath,
		checkPoints:   make(map[int64]struct{}),
		committedLogs: make([]CommitLogData, 0, commitBufSize),
	}
	// load check point index. read sizeof(CommitLogData) until EOF.
	var err error
	mgr.appender, err = os.OpenFile(mgr.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		glog.Infof("open topic commit log file error: %v", err)
		return nil
	}

	return mgr
}

func (self *TopicCommitLogMgr) nextLogId() int64 {
	oldid := self.nLogId
	self.nLogId++
	return oldid
}

func (self *TopicCommitLogMgr) GetLastCommitLogId() int64 {
	return self.pLogId
}

func (self *TopicCommitLogMgr) isCommitted(id int64) bool {
	if self.pLogId == id {
		return true
	}
	if _, ok := self.checkPoints[id]; ok {
		return true
	}
	return false
}

func (self *TopicCommitLogMgr) appendCommitLog(l *CommitLogData, slave bool) error {
	if l.LogId < self.nLogId {
		return ErrCommitLogId
	}
	if slave {
		self.nLogId = l.LogId + 1
	} else {
		if l.LogId == self.nLogId {
			return ErrCommitLogId
		}
	}
	if cap(self.committedLogs) == 0 {
		// no buffer, write to check point file directly.
		err := binary.Write(self.appender, binary.BigEndian, l)
		if err != nil {
			return err
		}
	} else {
		if len(self.committedLogs) >= cap(self.committedLogs) {
			self.flushCommitLogs()
		}
		self.committedLogs = append(self.committedLogs, *l)
	}
	self.checkPoints[l.LogId] = struct{}{}
	self.pLogId = l.LogId
	return nil
}

func (self *TopicCommitLogMgr) flushCommitLogs() {
	// write commit logs to check point file.
	self.checkPoints = make(map[int64]struct{})

	for _, v := range self.committedLogs {
		err := binary.Write(self.appender, binary.BigEndian, v)
		if err != nil {
			panic(err)
		}
	}
	self.committedLogs = self.committedLogs[0:0]
}

func (self *TopicCommitLogMgr) getCommitLogsReverse(startIndex int64, num int) ([]CommitLogData, error) {
	ret := make([]CommitLogData, 0, num)
	for i := startIndex; i < int64(len(self.committedLogs)); i++ {
		ret = append(ret, self.committedLogs[len(self.committedLogs)-int(i)-1])
		if len(ret) >= num {
			return ret, nil
		}
	}
	dataSize := GetLogDataSize()
	// read from end of commit file.
	endOffset := 0
	readStart := endOffset - dataSize*(num-len(ret))
	if readStart < 0 {
		readStart = 0
	}
	buf := make([]byte, endOffset-readStart)
	// read file data to buf
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
