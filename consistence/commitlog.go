package consistence

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/absolute8511/nsq/internal/util"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	DEFAULT_COMMIT_BUF_SIZE = 400
	MAX_COMMIT_BUF_SIZE     = 1000
	MAX_INCR_ID_BIT         = 50
)

var (
	ErrCommitLogWrongID              = errors.New("commit log id is wrong")
	ErrCommitLogWrongLastID          = errors.New("commit log last id should no less than log id")
	ErrCommitLogIDNotFound           = errors.New("commit log id is not found")
	ErrCommitLogSearchNotFound       = errors.New("search commit log data not found")
	ErrCommitLogOutofBound           = errors.New("commit log offset is out of bound")
	ErrCommitLogEOF                  = errors.New("commit log end of file")
	ErrCommitLogOffsetInvalid        = errors.New("commit log offset is invalid")
	ErrCommitLogPartitionExceed      = errors.New("commit log partition id is exceeded")
	ErrCommitLogSearchFailed         = errors.New("commit log data search failed")
	ErrCommitLogSegmentSizeInvalid   = errors.New("commit log segment size is invalid")
	ErrCommitLogLessThanSegmentStart = errors.New("commit log read index is less than segment start")
	ErrCommitLogCleanKeepMin         = errors.New("commit log clean should keep some data")
)

var LOGROTATE_NUM = 2000000
var MIN_KEEP_LOG_ITEM = 1000

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
	// the message number for current commit
	MsgNum int32
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

func GetPartitionFromMsgID(id int64) int {
	// the max partition id will be less than 1024
	return int((uint64(id) & (uint64(1024-1) << MAX_INCR_ID_BIT)) >> MAX_INCR_ID_BIT)
}

func getCommitLogFile(path string, start int64, for_write bool) (*os.File, error) {
	name := getSegmentFilename(path, start)
	mode := os.O_RDONLY
	if for_write {
		mode = os.O_RDWR
	}
	tmpFile, err := os.OpenFile(name, mode, 0644)
	if err != nil {
		return nil, err
	}

	return tmpFile, nil
}

func getCommitLogCountFromFile(path string, start int64) (int64, error) {
	name := getSegmentFilename(path, start)
	f, err := os.Stat(name)
	if err != nil {
		return 0, err
	}
	fsize := f.Size()

	if (fsize % int64(GetLogDataSize())) != 0 {
		return 0, ErrCommitLogSegmentSizeInvalid
	}
	return fsize / int64(GetLogDataSize()), nil
}

func getCommitLogFromFile(file *os.File, offset int64) (*CommitLogData, error) {
	f, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fsize := f.Size()
	if offset == fsize {
		return nil, ErrCommitLogEOF
	}

	if offset > fsize-int64(GetLogDataSize()) {
		return nil, ErrCommitLogOutofBound
	}

	if (offset % int64(GetLogDataSize())) != 0 {
		return nil, ErrCommitLogOffsetInvalid
	}
	b := bytes.NewBuffer(make([]byte, GetLogDataSize()))
	n, err := file.ReadAt(b.Bytes(), offset)
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

func getLastCommitLogDataFromFile(file *os.File) (*CommitLogData, int64, error) {
	s, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	fsize := s.Size()
	if fsize == 0 {
		return nil, 0, ErrCommitLogEOF
	}
	if fsize < int64(GetLogDataSize()) {
		return nil, 0, ErrCommitLogOffsetInvalid
	}
	num := fsize / int64(GetLogDataSize())
	roundOffset := (num - 1) * int64(GetLogDataSize())
	l, err := getCommitLogFromFile(file, roundOffset)
	if err != nil {
		coordLog.Infof("load file error: %v", err)
		return nil, 0, err
	}
	return l, roundOffset, err
}

func getLastCommitLogData(path string, startIndex int64) (*CommitLogData, int64, error) {
	tmpFile, err := getCommitLogFile(path, startIndex, false)
	if err != nil {
		return nil, 0, err
	}
	defer tmpFile.Close()
	return getLastCommitLogDataFromFile(tmpFile)
}

func getCommitLogListFromFile(file *os.File, offset int64, num int) ([]CommitLogData, error) {
	f, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fsize := f.Size()
	if offset == fsize {
		return nil, ErrCommitLogEOF
	}

	if offset > fsize-int64(GetLogDataSize()) {
		return nil, ErrCommitLogOutofBound
	}

	if (offset % int64(GetLogDataSize())) != 0 {
		return nil, ErrCommitLogOffsetInvalid
	}

	needRead := int64(num * GetLogDataSize())
	readToEnd := false
	if offset+needRead > fsize {
		needRead = fsize - offset
		readToEnd = true
	}
	b := bytes.NewBuffer(make([]byte, needRead))
	n, err := file.ReadAt(b.Bytes(), offset)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
		if int64(n) != needRead {
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
	if readToEnd {
		err = ErrCommitLogEOF
	}
	return logList, err
}

func truncateFileToOffset(file *os.File, fileStartOffset int64, offset int64) (*CommitLogData, error) {
	if offset > 0 && offset < int64(GetLogDataSize()) {
		return nil, ErrCommitLogOffsetInvalid
	}
	if offset < fileStartOffset {
		return nil, ErrCommitLogLessThanSegmentStart
	}
	err := file.Truncate(offset)
	if err != nil {
		s, _ := file.Stat()
		coordLog.Infof("truncate file %v failed: %v, offset:%v, %v ", file, err, offset, s)
		return nil, err
	}
	if offset == fileStartOffset {
		return nil, ErrCommitLogEOF
	}
	b := bytes.NewBuffer(make([]byte, GetLogDataSize()))
	n, err := file.ReadAt(b.Bytes(), offset-int64(GetLogDataSize()))
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

	return &l, nil
}

func getSegmentFilename(path string, index int64) string {
	return path + "." + fmt.Sprintf("%08d", index)
}

type LogStartInfo struct {
	SegmentStartCount int64
	SegmentStartIndex int64
	// the start offset in the file number at start index
	// This can allow the commit log start at the middle of the segment file
	SegmentStartOffset int64
}

type TopicCommitLogMgr struct {
	topic         string
	partition     int
	nLogID        int64
	pLogID        int64
	path          string
	committedLogs []CommitLogData
	buffer        []byte
	bufSize       int
	appender      *os.File
	currentStart  int64
	currentCount  int32
	logStartInfo  LogStartInfo
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
	if commitBufSize > MAX_COMMIT_BUF_SIZE {
		commitBufSize = MAX_COMMIT_BUF_SIZE
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
		buffer:        make([]byte, 0, (commitBufSize+1)*GetLogDataSize()),
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

	err = mgr.loadCurrentStart()
	if err != nil {
		return nil, err
	}
	err = mgr.loadLogSegStartInfo()
	if err != nil {
		return nil, err
	}
	//load meta
	f, err := mgr.appender.Stat()
	if err != nil {
		coordLog.Infof("stat file error: %v", err)
		return nil, err
	}
	fsize := f.Size()
	mgr.currentCount = int32(fsize / int64(GetLogDataSize()))
	// read latest logid and incr. combine the partition id at high.
	if fsize > 0 {
		num := fsize / int64(GetLogDataSize())
		roundOffset := (num - 1) * int64(GetLogDataSize())
		l, err := mgr.GetCommitLogFromOffsetV2(mgr.currentStart, roundOffset)
		if err != nil {
			coordLog.Infof("load file error: %v", err)
			return nil, err
		}
		mgr.pLogID = l.LogID
		mgr.nLogID = l.LastMsgLogID + 1
		if mgr.pLogID < int64(uint64(mgr.partition)<<MAX_INCR_ID_BIT) {
			coordLog.Errorf("log id init less than expected: %v", mgr.pLogID)
			return nil, errors.New("init commit log id failed")
		} else if mgr.pLogID > int64(uint64(mgr.partition+1)<<MAX_INCR_ID_BIT+1) {
			coordLog.Errorf("log id init large than expected: %v", mgr.pLogID)
			return nil, errors.New("init commit log id failed")
		}
		if mgr.nLogID < int64(uint64(mgr.partition)<<MAX_INCR_ID_BIT) {
			coordLog.Errorf("log id init less than expected: %v", mgr.pLogID)
			return nil, errors.New("init commit log id failed")
		} else if mgr.nLogID > int64(uint64(mgr.partition+1)<<MAX_INCR_ID_BIT+1) {
			coordLog.Errorf("log id init large than expected: %v", mgr.pLogID)
			return nil, errors.New("init commit log id failed")
		}
	} else {
		if mgr.currentStart <= mgr.logStartInfo.SegmentStartIndex {
			coordLog.Infof("no last commit data : %v, log start: %v", mgr.currentStart, mgr.logStartInfo)
			mgr.nLogID = int64(uint64(mgr.partition)<<MAX_INCR_ID_BIT + 1)
			if mgr.logStartInfo.SegmentStartIndex > 0 {
				l, _, err := getLastCommitLogData(mgr.path, mgr.currentStart-1)
				if err != nil {
					coordLog.Infof("get last commit data from file %v error: %v", mgr.currentStart-1, err)
				} else {
					mgr.pLogID = l.LogID
					mgr.nLogID = l.LastMsgLogID + 1
				}
			}
		} else {
			l, _, err := getLastCommitLogData(mgr.path, mgr.currentStart-1)
			if err != nil {
				coordLog.Infof("get last commit data from file %v error: %v", mgr.currentStart-1, err)
				return nil, err
			}
			mgr.pLogID = l.LogID
			mgr.nLogID = l.LastMsgLogID + 1
		}
	}
	coordLog.Infof("%v commit log init with log start: %v, current: %v:%v, pid: %v", mgr.path,
		mgr.logStartInfo, mgr.currentStart, mgr.currentCount, mgr.pLogID)
	return mgr, nil
}

func (self *TopicCommitLogMgr) MoveTo(newBase string) error {
	self.Lock()
	defer self.Unlock()
	self.flushCommitLogsNoLock()
	self.appender.Sync()
	self.appender.Close()
	newPath := GetTopicPartitionLogPath(newBase, self.topic, self.partition)
	coordLog.Infof("rename the topic %v commit log to :%v", self.topic, newPath)
	err := util.AtomicRename(self.path, newPath)
	if err != nil {
		coordLog.Infof("rename the topic %v commit log failed :%v", self.topic, err)
	}
	for i := int64(0); i < self.currentStart; i++ {
		util.AtomicRename(getSegmentFilename(self.path, int64(i)), getSegmentFilename(newPath, int64(i)))
	}
	err = util.AtomicRename(self.path+".current", newPath+".current")
	if err != nil {
		coordLog.Infof("rename the topic %v commit log current failed:%v", self.topic, err)
	}
	util.AtomicRename(self.path+".start", newPath+".start")
	return nil
}

func (self *TopicCommitLogMgr) loadLogSegStartInfo() error {
	file, err := os.OpenFile(self.path+".start", os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			self.logStartInfo.SegmentStartCount = 0
			self.logStartInfo.SegmentStartIndex = 0
			self.logStartInfo.SegmentStartOffset = 0
			return nil
		}
		coordLog.Errorf("open commit log file error: %v", err)
		return err
	}
	defer file.Close()
	var data int64
	var data2 int64
	var data3 int64
	_, err = fmt.Fscanf(file, "%d\n%d\n%d\n", &data, &data2, &data3)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&self.logStartInfo.SegmentStartCount, data)
	atomic.StoreInt64(&self.logStartInfo.SegmentStartIndex, data2)
	atomic.StoreInt64(&self.logStartInfo.SegmentStartOffset, data3)
	return nil
}

func (self *TopicCommitLogMgr) saveLogSegStartInfo() error {
	tmpFileName := self.path + ".start.tmp"
	file, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		coordLog.Errorf("open commit log file for write error: %v", err)
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%d\n%d\n%d\n", atomic.LoadInt64(&self.logStartInfo.SegmentStartCount),
		atomic.LoadInt64(&self.logStartInfo.SegmentStartIndex),
		atomic.LoadInt64(&self.logStartInfo.SegmentStartOffset))
	if err != nil {
		return err
	}
	file.Sync()
	return util.AtomicRename(tmpFileName, self.path+".start")
}

func (self *TopicCommitLogMgr) loadCurrentStart() error {
	file, err := os.OpenFile(self.path+".current", os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			self.currentStart = 0
			return nil
		}
		coordLog.Errorf("open commit log current file error: %v", err)
		return err
	}
	defer file.Close()
	var data int64
	_, err = fmt.Fscanf(file, "%d\n", &data)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&self.currentStart, data)
	return nil
}

func (self *TopicCommitLogMgr) saveCurrentStart() error {
	tmpFileName := self.path + ".current.tmp"
	file, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		coordLog.Errorf("open commit log current file for write error: %v", err)
		return err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "%d\n", atomic.LoadInt64(&self.currentStart))
	if err != nil {
		return err
	}
	file.Sync()

	return util.AtomicRename(tmpFileName, self.path+".current")
}

func (self *TopicCommitLogMgr) ResetLogWithStart(newStart LogStartInfo) error {
	self.Lock()
	defer self.Unlock()

	self.flushCommitLogsNoLock()
	self.appender.Sync()
	self.appender.Close()
	err := os.Remove(self.path)
	if err != nil {
		coordLog.Warningf("failed to remove the commit log for topic: %v", self.path)
	}
	for i := int64(0); i < self.currentStart; i++ {
		fn := getSegmentFilename(self.path, int64(i))
		os.Remove(fn)
		coordLog.Infof("commit log removed: %v", fn)
	}
	os.Remove(self.path + ".current")
	os.Remove(self.path + ".start")
	self.logStartInfo = newStart
	self.logStartInfo.SegmentStartOffset = 0
	atomic.StoreInt64(&self.pLogID, 0)
	self.currentStart = newStart.SegmentStartIndex
	self.currentCount = 0
	self.appender, err = os.OpenFile(self.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		coordLog.Infof("open topic commit log file error: %v", err)
		return err
	}
	err = self.saveCurrentStart()
	if err != nil {
		return err
	}
	return self.saveLogSegStartInfo()
}

func (self *TopicCommitLogMgr) GetLogStartInfo() (*LogStartInfo, *CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	logStart := self.logStartInfo
	l, err := self.getCommitLogFromOffsetV2(logStart.SegmentStartIndex, logStart.SegmentStartOffset)
	if err != nil {
		coordLog.Infof("get log start first log info err: %v, current: %v:%v, log start: %v", err,
			self.currentStart, self.currentCount, logStart)
	}
	return &logStart, l, err
}

func (self *TopicCommitLogMgr) GetCurrentStart() int64 {
	self.Lock()
	tmp := atomic.LoadInt64(&self.currentStart)
	self.Unlock()
	return tmp
}

func (self *TopicCommitLogMgr) Delete() {
	self.Lock()
	self.flushCommitLogsNoLock()
	self.appender.Sync()
	self.appender.Close()
	err := os.Remove(self.path)
	if err != nil && !os.IsNotExist(err) {
		coordLog.Warningf("failed to remove the commit log for topic: %v", self.path)
	}
	for i := int64(0); i < self.currentStart; i++ {
		os.Remove(getSegmentFilename(self.path, int64(i)))
	}
	os.Remove(self.path + ".current")
	os.Remove(self.path + ".start")
	self.Unlock()
}

func (self *TopicCommitLogMgr) Close() {
	self.Lock()
	self.flushCommitLogsNoLock()
	self.appender.Sync()
	self.appender.Close()
	self.saveCurrentStart()
	self.saveLogSegStartInfo()
	self.Unlock()
}

func (self *TopicCommitLogMgr) Reopen() error {
	var err error
	self.appender, err = os.OpenFile(self.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		coordLog.Infof("open topic commit log file error: %v", err)
		return err
	}

	atomic.StoreInt64(&self.pLogID, 0)
	err = self.loadCurrentStart()
	if err != nil {
		coordLog.Infof("open topic commit log error: %v", err)
	}
	err = self.loadLogSegStartInfo()
	if err != nil {
		coordLog.Infof("open topic commit log error: %v", err)
	}
	//load meta
	f, err := self.appender.Stat()
	if err != nil {
		coordLog.Infof("stat file error: %v", err)
		return err
	}
	fsize := f.Size()
	self.currentCount = int32(fsize / int64(GetLogDataSize()))
	// read latest logid and incr. combine the partition id at high.
	if fsize > 0 {
		num := fsize / int64(GetLogDataSize())
		roundOffset := (num - 1) * int64(GetLogDataSize())
		l, err := self.GetCommitLogFromOffsetV2(self.currentStart, roundOffset)
		if err != nil {
			coordLog.Infof("load file error: %v", err)
			return err
		}
		self.pLogID = l.LogID
		self.nLogID = l.LastMsgLogID + 1
		if self.pLogID < int64(uint64(self.partition)<<MAX_INCR_ID_BIT) {
			coordLog.Errorf("log id init less than expected: %v", self.pLogID)
			return errors.New("init commit log id failed")
		} else if self.pLogID > int64(uint64(self.partition+1)<<MAX_INCR_ID_BIT+1) {
			coordLog.Errorf("log id init large than expected: %v", self.pLogID)
			return errors.New("init commit log id failed")
		}
		if self.nLogID < int64(uint64(self.partition)<<MAX_INCR_ID_BIT) {
			coordLog.Errorf("log id init less than expected: %v", self.pLogID)
			return errors.New("init commit log id failed")
		} else if self.nLogID > int64(uint64(self.partition+1)<<MAX_INCR_ID_BIT+1) {
			coordLog.Errorf("log id init large than expected: %v", self.pLogID)
			return errors.New("init commit log id failed")
		}
	} else {
		if self.currentStart <= self.logStartInfo.SegmentStartIndex {
			coordLog.Infof("no last commit data : %v, log start: %v", self.currentStart, self.logStartInfo)
			self.nLogID = int64(uint64(self.partition)<<MAX_INCR_ID_BIT + 1)
			if self.logStartInfo.SegmentStartIndex > 0 {
				l, _, err := getLastCommitLogData(self.path, self.currentStart-1)
				if err != nil {
					coordLog.Infof("get last commit data from file %v error: %v", self.currentStart-1, err)
				} else {
					self.pLogID = l.LogID
					self.nLogID = l.LastMsgLogID + 1
				}
			}
		} else {
			l, _, err := getLastCommitLogData(self.path, self.currentStart-1)
			if err != nil {
				coordLog.Infof("get last commit data from file %v error: %v", self.currentStart-1, err)
				return err
			}
			self.pLogID = l.LogID
			self.nLogID = l.LastMsgLogID + 1
		}
	}
	return nil

}

func (self *TopicCommitLogMgr) NextID() uint64 {
	id := atomic.AddInt64(&self.nLogID, 1)
	return uint64(id)
}

// reset the nextid
func (self *TopicCommitLogMgr) Reset(id uint64) {
}

func (self *TopicCommitLogMgr) GetCurrentEnd() (int64, int64) {
	self.Lock()
	defer self.Unlock()
	return self.currentStart, int64(self.currentCount) * int64(GetLogDataSize())
}

func (self *TopicCommitLogMgr) CleanOldData(fileIndex int64, fileOffset int64) error {
	self.Lock()
	defer self.Unlock()
	if fileIndex > self.currentStart {
		return ErrCommitLogCleanKeepMin
	}
	// keep some log items
	if fileIndex == self.currentStart {
		if fileOffset/int64(GetLogDataSize())+int64(MIN_KEEP_LOG_ITEM) >= int64(self.currentCount) {
			return ErrCommitLogCleanKeepMin
		}
	} else if fileIndex == self.currentStart-1 {
		fName := getSegmentFilename(self.path, fileIndex)
		stat, err := os.Stat(fName)
		if err != nil {
			return err
		}

		leftSize := stat.Size() - fileOffset
		if leftSize < 0 {
			return ErrCommitLogOffsetInvalid
		}
		if leftSize/int64(GetLogDataSize())+int64(self.currentCount) <= int64(MIN_KEEP_LOG_ITEM) {
			return ErrCommitLogCleanKeepMin
		}
	}
	oldStartInfo := self.logStartInfo
	cleanStart := self.logStartInfo.SegmentStartIndex
	cleanStartOffset := self.logStartInfo.SegmentStartOffset
	for i := int64(0); i < cleanStart; i++ {
		if i < fileIndex-1 {
			fName := getSegmentFilename(self.path, int64(i))
			err := os.Remove(fName)
			if err != nil {
				if !os.IsNotExist(err) {
					coordLog.Warningf("clean commit segment %v failed: %v", fName, err)
				}
			} else {
				coordLog.Infof("clean commit segment %v ", fName)
			}
		}
	}
	// clean the commit segment before the specific file index
	for i := cleanStart; i < fileIndex; i++ {
		fName := getSegmentFilename(self.path, int64(i))
		stat, err := os.Stat(fName)
		if err != nil {
			coordLog.Warningf("commit segment %v stat failed: %v", fName, stat)
			self.saveLogSegStartInfo()
			return err
		}
		if cleanStartOffset > stat.Size() {
			coordLog.Warningf("commit clean offset %v more than segment size : %v", cleanStartOffset, stat)
			self.saveLogSegStartInfo()
			return ErrCommitLogOffsetInvalid
		}
		if (stat.Size()-cleanStartOffset)%int64(GetLogDataSize()) != 0 {
			coordLog.Warningf("commit segment size invalid: %v", stat)
			self.saveLogSegStartInfo()
			return ErrCommitLogOffsetInvalid
		}

		atomic.AddInt64(&self.logStartInfo.SegmentStartCount, (stat.Size()-cleanStartOffset)/int64(GetLogDataSize()))
		cleanStartOffset = 0
		atomic.AddInt64(&self.logStartInfo.SegmentStartIndex, 1)
		atomic.StoreInt64(&self.logStartInfo.SegmentStartOffset, cleanStartOffset)
		// keep the previous file to read the last commit log
		if int64(i) < fileIndex-1 {
			err = os.Remove(fName)
			if err != nil {
				coordLog.Warningf("clean commit segment %v failed: %v", fName, err)
			} else {
				coordLog.Infof("clean commit segment %v ", fName)
			}
		}
	}
	if fileOffset < cleanStartOffset {
		coordLog.Warningf("commit clean offset %v less than segment start: %v", fileOffset, cleanStartOffset)
		return ErrCommitLogOffsetInvalid
	}
	diffCnt := (fileOffset - cleanStartOffset) / int64(GetLogDataSize())
	self.logStartInfo.SegmentStartCount += diffCnt
	atomic.StoreInt64(&self.logStartInfo.SegmentStartOffset, fileOffset)

	if self.logStartInfo.SegmentStartIndex < self.currentStart {
		fName := getSegmentFilename(self.path, self.logStartInfo.SegmentStartIndex)
		stat, err := os.Stat(fName)
		if err != nil {
			return err
		}
		if fileOffset > stat.Size() {
			coordLog.Warningf("commit clean offset %v more than segment size : %v", fileOffset, stat)
			return ErrCommitLogOffsetInvalid
		}
		if fileOffset == stat.Size() {
			atomic.AddInt64(&self.logStartInfo.SegmentStartIndex, 1)
			atomic.StoreInt64(&self.logStartInfo.SegmentStartOffset, 0)
			// keep the previous file to read the last commit log
			// os.Remove(fName)
		}
	}

	self.saveLogSegStartInfo()
	coordLog.Infof("commit segment start clean from %v to : %v (%v:%v)",
		oldStartInfo, self.logStartInfo, fileIndex, fileOffset)
	return nil
}

func (self *TopicCommitLogMgr) TruncateToOffsetV2(startIndex int64, offset int64) (*CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	self.flushCommitLogsNoLock()
	if startIndex > self.currentStart {
		return nil, ErrCommitLogOffsetInvalid
	}
	if startIndex < self.logStartInfo.SegmentStartIndex ||
		(startIndex == self.logStartInfo.SegmentStartIndex && offset < self.logStartInfo.SegmentStartOffset) {
		coordLog.Errorf("truncate file exceed the begin of the segment start: %v, %v", startIndex, self.logStartInfo)
		return nil, ErrCommitLogLessThanSegmentStart
	}

	startOffset := int64(0)
	if startIndex == self.logStartInfo.SegmentStartIndex {
		startOffset = self.logStartInfo.SegmentStartOffset
	}
	coordLog.Infof("truncate commit log from: %v, %v", self.currentStart, self.currentCount)
	if startIndex < self.currentStart {
		oldStart := self.currentStart
		self.currentStart = startIndex
		self.currentCount = int32(offset / int64(GetLogDataSize()))
		self.saveCurrentStart()

		self.appender.Sync()
		self.appender.Close()
		os.Remove(self.path)
		for i := startIndex + 1; i < oldStart; i++ {
			os.Remove(getSegmentFilename(self.path, int64(i)))
		}

		err := util.AtomicRename(getSegmentFilename(self.path, startIndex), self.path)
		if err != nil {
			coordLog.Infof("rename file failed: %v, %v", startIndex, err)
			return nil, err
		}
		self.appender, err = os.OpenFile(self.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			coordLog.Errorf("open topic %v commit log file error: %v", self.path, err)
			return nil, err
		}
	}
	return self.truncateToOffset(startOffset, offset)
}

func (self *TopicCommitLogMgr) truncateToOffset(startOffset int64, offset int64) (*CommitLogData, error) {
	l, err := truncateFileToOffset(self.appender, startOffset, offset)
	self.currentCount = int32(offset / int64(GetLogDataSize()))
	coordLog.Infof("truncate commit log to: %v, %v", self.currentStart, self.currentCount)
	if err != nil {
		if err == ErrCommitLogEOF {
			if self.currentStart <= self.logStartInfo.SegmentStartIndex {
				atomic.StoreInt64(&self.pLogID, 0)
				return nil, err
			} else {
				l, _, err = getLastCommitLogData(self.path, self.currentStart-1)
				if err != nil {
					return nil, err
				}
			}
		} else {
			coordLog.Errorf("truncate topic %v commit log error: %v", self.topic, err)
			return nil, err
		}
	}
	atomic.StoreInt64(&self.pLogID, l.LogID)
	if l.LastMsgLogID+1 > atomic.LoadInt64(&self.nLogID) {
		atomic.StoreInt64(&self.nLogID, l.LastMsgLogID+1)
	}
	return l, nil
}

func (self *TopicCommitLogMgr) ConvertToCountIndex(start int64, offset int64) (int64, error) {
	self.Lock()
	defer self.Unlock()
	if start > self.currentStart {
		return 0, ErrCommitLogOutofBound
	}
	if offset%int64(GetLogDataSize()) != 0 {
		return 0, ErrCommitLogOffsetInvalid
	}
	countIndex := self.logStartInfo.SegmentStartCount
	startOffset := self.logStartInfo.SegmentStartOffset
	if start < self.logStartInfo.SegmentStartIndex ||
		(start == self.logStartInfo.SegmentStartIndex && offset < startOffset) {
		return 0, ErrCommitLogLessThanSegmentStart
	}
	for i := self.logStartInfo.SegmentStartIndex; i < start; i++ {
		cnt, err := getCommitLogCountFromFile(self.path, i)
		if err != nil {
			coordLog.Warningf("get count from segment %v failed: %v", i, err)
			return 0, err
		}
		countIndex += cnt - startOffset/int64(GetLogDataSize())
		startOffset = 0
	}
	if offset < startOffset {
		return 0, ErrCommitLogOffsetInvalid
	}
	countIndex += (offset - startOffset) / int64(GetLogDataSize())
	return countIndex, nil
}

func (self *TopicCommitLogMgr) ConvertToOffsetIndex(countIndex int64) (int64, int64, error) {
	self.Lock()
	defer self.Unlock()
	if countIndex < self.logStartInfo.SegmentStartCount {
		return 0, 0, ErrCommitLogLessThanSegmentStart
	}
	segIndex := self.logStartInfo.SegmentStartIndex
	offset := self.logStartInfo.SegmentStartOffset
	countIndex -= self.logStartInfo.SegmentStartCount
	for countIndex > 0 && segIndex < self.currentStart {
		cnt, err := getCommitLogCountFromFile(self.path, segIndex)
		if err != nil {
			coordLog.Warningf("get count from segment %v failed: %v", segIndex, err)
			return 0, 0, err
		}
		cnt -= offset / int64(GetLogDataSize())
		if countIndex < cnt {
			offset += countIndex * int64(GetLogDataSize())
			return segIndex, offset, nil
		}
		countIndex -= cnt
		segIndex++
		offset = int64(0)
	}
	if segIndex == self.currentStart && countIndex > 0 {
		if countIndex > int64(self.currentCount)-offset/int64(GetLogDataSize()) {
			coordLog.Warningf("count out of bound: %v, %v:%v", countIndex, self.currentStart, self.currentCount)
			return 0, 0, ErrCommitLogOutofBound
		}
		self.flushCommitLogsNoLock()
		offset += countIndex * int64(GetLogDataSize())
	}
	return segIndex, offset, nil
}

func (self *TopicCommitLogMgr) getCommitLogFromOffsetV2(start int64, offset int64) (*CommitLogData, error) {
	if start > self.currentStart {
		return nil, ErrCommitLogOutofBound
	} else if start < self.logStartInfo.SegmentStartIndex {
		return nil, ErrCommitLogLessThanSegmentStart
	} else if start == self.logStartInfo.SegmentStartIndex && offset < self.logStartInfo.SegmentStartOffset {
		return nil, ErrCommitLogLessThanSegmentStart
	} else {
		if start == self.currentStart {
			self.flushCommitLogsNoLock()
			return getCommitLogFromFile(self.appender, offset)
		}
		logs, err := self.getCommitLogsV2(start, offset, 1)
		if err != nil {
			return nil, err
		}
		if len(logs) == 0 {
			return nil, ErrCommitLogEOF
		}
		l := logs[0]
		return &l, nil
	}
}

func (self *TopicCommitLogMgr) GetCommitLogFromOffsetV2(start int64, offset int64) (*CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	return self.getCommitLogFromOffsetV2(start, offset)
}

func (self *TopicCommitLogMgr) GetLastCommitLogDataOnSegment(index int64) (int64, *CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	if self.currentStart == index {
		self.flushCommitLogsNoLock()
		l, readOffset, err := getLastCommitLogDataFromFile(self.appender)
		return readOffset, l, err
	} else if index > self.currentStart {
		return 0, nil, ErrCommitLogOutofBound
	} else if index < self.logStartInfo.SegmentStartIndex {
		return 0, nil, ErrCommitLogLessThanSegmentStart
	} else {
		l, readOffset, err := getLastCommitLogData(self.path, index)
		return readOffset, l, err
	}
}

// this will get the log data of last commit
func (self *TopicCommitLogMgr) GetLastCommitLogOffsetV2() (int64, int64, *CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	self.flushCommitLogsNoLock()
	f, err := self.appender.Stat()
	if err != nil {
		return self.currentStart, 0, nil, err
	}
	fsize := f.Size()
	if fsize == 0 {
		if self.currentStart == self.logStartInfo.SegmentStartIndex {
			return self.currentStart, 0, nil, ErrCommitLogEOF
		} else {
			// it may happen : a new commit segment without any commit log,
			// so we get the last log id from previous segment.
			l, readOffset, err := getLastCommitLogData(self.path, self.currentStart-1)
			if err != nil {
				return self.currentStart, 0, nil, err
			}
			if l.LogID == atomic.LoadInt64(&self.pLogID) {
				return self.currentStart - 1, readOffset, l, nil
			}
		}
		return self.currentStart, 0, nil, ErrCommitLogIDNotFound
	}

	num := fsize / int64(GetLogDataSize())
	roundOffset := (num - 1) * int64(GetLogDataSize())
	for {
		l, err := getCommitLogFromFile(self.appender, roundOffset)
		if err != nil {
			return self.currentStart, 0, nil, err
		}
		if l.LogID == atomic.LoadInt64(&self.pLogID) {
			return self.currentStart, roundOffset, l, nil
		} else if l.LogID < atomic.LoadInt64(&self.pLogID) {
			break
		}
		roundOffset -= int64(GetLogDataSize())
		if roundOffset < 0 {
			break
		}
	}
	return self.currentStart, 0, nil, ErrCommitLogIDNotFound
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
		coordLog.Errorf("commit id %v less than prev id: %v, %v", l, atomic.LoadInt64(&self.pLogID), atomic.LoadInt64(&self.nLogID))
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
	if self.currentCount >= int32(LOGROTATE_NUM) {
		self.flushCommitLogsNoLock()
		self.appender.Sync()
		self.appender.Close()
		newName := getSegmentFilename(self.path, self.currentStart)
		err := util.AtomicRename(self.path, newName)
		if err != nil {
			coordLog.Errorf("rotate file %v to %v failed: %v", self.path, newName, err)
			return err
		}
		coordLog.Infof("rotate file %v to %v", self.path, newName)
		self.appender, err = os.OpenFile(self.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			coordLog.Errorf("open topic %v commit log file error: %v", self.path, err)
			return err
		}
		atomic.AddInt64(&self.currentStart, 1)
		self.currentCount = 0
		err = self.saveCurrentStart()
		if err != nil {
			return err
		}
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
	self.currentCount++
	atomic.StoreInt64(&self.pLogID, l.LogID)
	return nil
}

func (self *TopicCommitLogMgr) updateBufferSize(bs int) {
	if bs != 0 {
		if bs > MAX_COMMIT_BUF_SIZE {
			bs = MAX_COMMIT_BUF_SIZE
		} else if bs < 0 {
			bs = DEFAULT_COMMIT_BUF_SIZE
		}
	}
	self.Lock()
	self.bufSize = bs
	self.Unlock()
}

func (self *TopicCommitLogMgr) switchForMaster(master bool) {
	self.Lock()
	self.flushCommitLogsNoLock()
	if master {
		if cap(self.committedLogs) != self.bufSize {
			self.committedLogs = make([]CommitLogData, 0, self.bufSize)
		}
	} else {
		slaveBuf := self.bufSize
		if self.bufSize < DEFAULT_COMMIT_BUF_SIZE/4 {
			slaveBuf = slaveBuf * 2
		}
		if cap(self.committedLogs) != slaveBuf {
			self.committedLogs = make([]CommitLogData, 0, slaveBuf)
		}
	}
	self.Unlock()
}

func (self *TopicCommitLogMgr) flushCommitLogsNoLock() {
	if len(self.committedLogs) == 0 {
		return
	}
	// write buffered commit logs to file.
	tmpBuf := bytes.NewBuffer(self.buffer[:0])
	for _, v := range self.committedLogs {
		err := binary.Write(tmpBuf, binary.BigEndian, v)
		if err != nil {
			panic(err)
		}
	}
	_, err := tmpBuf.WriteTo(self.appender)
	if err != nil {
		panic(err)
	}
	self.committedLogs = self.committedLogs[0:0]
}

func (self *TopicCommitLogMgr) FlushCommitLogs() {
	self.Lock()
	self.flushCommitLogsNoLock()
	self.Unlock()
}

func (self *TopicCommitLogMgr) getCommitLogsV2(startIndex int64, startOffset int64, num int) ([]CommitLogData, error) {
	if startIndex < self.logStartInfo.SegmentStartIndex {
		return nil, ErrCommitLogLessThanSegmentStart
	} else if startIndex == self.logStartInfo.SegmentStartIndex && startOffset < self.logStartInfo.SegmentStartOffset {
		return nil, ErrCommitLogLessThanSegmentStart
	} else if startIndex > self.currentStart {
		return nil, ErrCommitLogOutofBound
	}
	self.flushCommitLogsNoLock()
	if startIndex == self.currentStart {
		return getCommitLogListFromFile(self.appender, startOffset, num)
	}

	var totalLogs []CommitLogData
	readIndex := startIndex
	readOffset := startOffset
	for {
		var loglist []CommitLogData
		var err error
		if readIndex < self.currentStart {
			tmpFile, fileErr := getCommitLogFile(self.path, readIndex, false)
			if fileErr != nil {
				coordLog.Warningf("read logs from %v error : %v", readIndex, fileErr)
				return totalLogs, fileErr
			}
			loglist, err = getCommitLogListFromFile(tmpFile, readOffset, num-len(totalLogs))
			tmpFile.Close()
		} else {
			loglist, err = getCommitLogListFromFile(self.appender, readOffset, num-len(totalLogs))
		}
		if err == ErrCommitLogEOF {
			//coordLog.Debugf("read %v to end: %v logs", readIndex, len(loglist))
			readIndex++
			readOffset = 0
		} else {
			if err != nil {
				coordLog.Infof("read %v:%v logs failed: %v", readIndex, readOffset, err)
				return totalLogs, err
			}
			readOffset = readOffset + int64(len(loglist))*int64(GetLogDataSize())
		}
		totalLogs = append(totalLogs, loglist...)
		if readIndex > self.currentStart {
			if err == ErrCommitLogEOF {
				return totalLogs, err
			}
			break
		} else if len(totalLogs) == num {
			break
		}
	}
	return totalLogs, nil
}

func (self *TopicCommitLogMgr) GetCommitLogsV2(startIndex int64, startOffset int64, num int) ([]CommitLogData, error) {
	self.Lock()
	defer self.Unlock()
	return self.getCommitLogsV2(startIndex, startOffset, num)
}

type ICommitLogComparator interface {
	SearchEndBoundary() int64
	LessThanLeftBoundary(l *CommitLogData) bool
	GreatThanRightBoundary(l *CommitLogData) bool
}

func (self *TopicCommitLogMgr) SearchLogDataByComparator(comp ICommitLogComparator) (int64, int64, *CommitLogData, error) {
	logStart, _, _ := self.GetLogStartInfo()
	searchOffset := logStart.SegmentStartOffset
	searchLogIndexStart := logStart.SegmentStartIndex
	searchLogIndexEnd := comp.SearchEndBoundary()
	if searchLogIndexEnd > self.currentStart {
		searchLogIndexEnd = self.currentStart
	}
	// find the start index of log
	for searchLogIndexStart < searchLogIndexEnd {
		searchIndex := searchLogIndexStart + (searchLogIndexEnd-searchLogIndexStart)/2
		_, segEndLog, err := self.GetLastCommitLogDataOnSegment(searchIndex)
		if err != nil {
			coordLog.Infof("read last log data failed: %v, offset: %v", err, searchIndex)
			if os.IsNotExist(err) {
				searchLogIndexStart = searchIndex + 1
				continue
			}
			return 0, 0, nil, err
		}
		searchBeginOffset := int64(0)
		if searchIndex == logStart.SegmentStartIndex {
			searchBeginOffset = logStart.SegmentStartOffset
		}
		segStartLog, err := self.GetCommitLogFromOffsetV2(searchIndex, searchBeginOffset)
		if err != nil {
			coordLog.Infof("log file searching start failed: %v:%v, %v", searchIndex, searchBeginOffset, logStart)
			return 0, 0, nil, err
		}
		if coordLog.Level() >= 4 {
			coordLog.Debugf("log file index searching: %v:%v, %v", searchLogIndexStart, searchLogIndexEnd, searchIndex)
		}
		if comp.GreatThanRightBoundary(segEndLog) {
			searchLogIndexStart = searchIndex + 1
		} else if comp.LessThanLeftBoundary(segStartLog) {
			searchLogIndexEnd = searchIndex - 1
		} else {
			searchLogIndexStart = searchIndex
			break
		}
	}
	if searchLogIndexStart > searchLogIndexEnd {
		return searchLogIndexEnd, 0, nil, ErrCommitLogSearchNotFound
	}
	searchCntStart := int64(0)
	if searchLogIndexStart == self.logStartInfo.SegmentStartIndex {
		searchCntStart = self.logStartInfo.SegmentStartOffset / int64(GetLogDataSize())
	}
	roffset, _, err := self.GetLastCommitLogDataOnSegment(searchLogIndexStart)
	if err != nil {
		coordLog.Infof("get last log data on segment:%v, failed:%v\n", searchLogIndexStart, err)
		return 0, 0, nil, err
	}
	searchCntEnd := roffset / int64(GetLogDataSize())
	var cur *CommitLogData
	for {
		if searchCntStart > searchCntEnd {
			break
		}
		searchCntPos := searchCntStart + (searchCntEnd-searchCntStart)/2
		searchOffset = searchCntPos * int64(GetLogDataSize())
		if coordLog.Level() >= 4 {
			coordLog.Debugf("log searching: %v:%v", searchLogIndexStart, searchOffset)
		}
		cur, err = self.GetCommitLogFromOffsetV2(searchLogIndexStart, searchOffset)
		if err != nil {
			coordLog.Infof("get log data from:%v:%v, failed:%v\n", searchLogIndexStart, searchOffset, err)
			if err == ErrCommitLogLessThanSegmentStart {
				searchCntStart = searchCntPos + 1
				continue
			}
			return 0, 0, nil, err
		}
		if comp.LessThanLeftBoundary(cur) {
			searchCntEnd = searchCntPos - 1
		} else if comp.GreatThanRightBoundary(cur) {
			searchCntStart = searchCntPos + 1
		} else {
			break
		}
	}
	return searchLogIndexStart, searchOffset, cur, nil
}

type CntComparator int64

func (self CntComparator) SearchEndBoundary() int64 {
	return int64(self)/int64(LOGROTATE_NUM) + 1
}

func (self CntComparator) LessThanLeftBoundary(l *CommitLogData) bool {
	if l.MsgCnt > int64(self) {
		return true
	}
	return false
}

func (self CntComparator) GreatThanRightBoundary(l *CommitLogData) bool {
	if l.MsgCnt+int64(l.MsgNum-1) < int64(self) {
		return true
	}
	return false
}

type MsgIDComparator int64

func (self MsgIDComparator) SearchEndBoundary() int64 {
	return int64(self)/int64(LOGROTATE_NUM) + 1
}

func (self MsgIDComparator) LessThanLeftBoundary(l *CommitLogData) bool {
	if l.LogID > int64(self) {
		return true
	}
	return false
}

func (self MsgIDComparator) GreatThanRightBoundary(l *CommitLogData) bool {
	if l.LastMsgLogID < int64(self) {
		return true
	}
	return false
}

type MsgOffsetComparator int64

func (self MsgOffsetComparator) SearchEndBoundary() int64 {
	return int64(self)/int64(LOGROTATE_NUM) + 1
}

func (self MsgOffsetComparator) LessThanLeftBoundary(l *CommitLogData) bool {
	if l.MsgOffset > int64(self) {
		return true
	}
	return false
}

func (self MsgOffsetComparator) GreatThanRightBoundary(l *CommitLogData) bool {
	// if the offset is equal to the right boundary, it means it should be next log (great than current log)
	if l.MsgOffset+int64(l.MsgSize) <= int64(self) {
		return true
	}
	return false
}

func (self *TopicCommitLogMgr) SearchLogDataByMsgCnt(searchCnt int64) (int64, int64, *CommitLogData, error) {
	return self.SearchLogDataByComparator(CntComparator(searchCnt))
}

func (self *TopicCommitLogMgr) SearchLogDataByMsgID(searchMsgID int64) (int64, int64, *CommitLogData, error) {
	return self.SearchLogDataByComparator(MsgIDComparator(searchMsgID))
}

func (self *TopicCommitLogMgr) SearchLogDataByMsgOffset(searchMsgOffset int64) (int64, int64, *CommitLogData, error) {
	return self.SearchLogDataByComparator(MsgOffsetComparator(searchMsgOffset))
}
