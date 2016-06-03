package consistence

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"
)

func newTestLogger(tbl test.TbLog) levellogger.Logger {
	return &test.TestLogger{tbl, 0}
}

func TestCommitLogWrite(t *testing.T) {
	logName := "test_log" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)

	test.Nil(t, err)
	test.Equal(t, logMgr.pLogID, int64(0))
	test.Equal(t, logMgr.nLogID > logMgr.pLogID, true)
	test.Equal(t, logMgr.GetLastCommitLogID(), int64(0))

	num := 100
	msgRawSize := 10
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		test.Equal(t, int64(logMgr.NextID()) > logData.LastMsgLogID, true)
	}
	lastOffset, err := logMgr.GetLastLogOffset()
	test.Nil(t, err)
	test.Equal(t, int64((num-1)*GetLogDataSize()), lastOffset)
	lastLog, err := logMgr.GetCommitLogFromOffset(lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	alllogs, err := logMgr.GetCommitLogs(0, num*2)
	test.Nil(t, err)
	test.Equal(t, len(alllogs), num)
	var prevLog CommitLogData
	for i := 0; i < num; i++ {
		logs, err := logMgr.GetCommitLogs(int64(i*GetLogDataSize()), 1)
		test.Nil(t, err)
		test.Equal(t, len(logs), 1)
		if prevLog.LogID > 0 {
			test.Equal(t, prevLog.LogID < logs[0].LogID, true)
			test.Equal(t, prevLog.MsgOffset+int64(msgRawSize), logs[0].MsgOffset)
			test.Equal(t, prevLog.MsgCnt+1, logs[0].MsgCnt)
			test.Equal(t, prevLog.MsgNum, 1)
		}
		prevLog = logs[0]
	}
	for i := num; i < num*2; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		test.Equal(t, int64(logMgr.NextID()) > logData.LastMsgLogID, true)
	}
	lastOffset, err = logMgr.GetLastLogOffset()
	test.Nil(t, err)
	lastLog, err = logMgr.GetCommitLogFromOffset(lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	for i := num; i < num*2; i++ {
		logs, err := logMgr.GetCommitLogs(int64(i*GetLogDataSize()), 1)
		test.Nil(t, err)
		test.Equal(t, len(logs), 1)
		if prevLog.LogID > 0 {
			test.Equal(t, prevLog.LogID < logs[0].LogID, true)
			test.Equal(t, prevLog.MsgOffset+int64(msgRawSize), logs[0].MsgOffset)
			test.Equal(t, prevLog.MsgCnt+int64(1), logs[0].MsgCnt)
			test.Equal(t, prevLog.MsgNum, 1)
		}
		prevLog = logs[0]
	}
}

func TestCommitLogTruncate(t *testing.T) {
	logName := "test_log" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)

	test.Nil(t, err)
	test.Equal(t, logMgr.pLogID, int64(0))
	test.Equal(t, logMgr.nLogID > logMgr.pLogID, true)
	test.Equal(t, logMgr.GetLastCommitLogID(), int64(0))

	num := 100
	msgRawSize := 10
	truncateOffset := int64(0)
	truncateId := int64(0)
	truncateAtCnt := 50
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		if i < truncateAtCnt {
			truncateOffset += int64(GetLogDataSize())
			truncateId = logData.LogID
		}
	}
	test.Equal(t, int64(truncateAtCnt*GetLogDataSize()), truncateOffset)
	prevLog, err := logMgr.TruncateToOffset(truncateOffset)
	test.Nil(t, err)
	test.Equal(t, truncateId, logMgr.pLogID)
	test.Equal(t, truncateId, prevLog.LogID)
	test.Equal(t, int64(truncateAtCnt-1), prevLog.MsgCnt)
	prevLog, err = logMgr.TruncateToOffset(0)
	test.Nil(t, err)
	test.Equal(t, int64(0), logMgr.pLogID)
	test.Nil(t, prevLog)
}

func TestCommitLogForBatchWrite(t *testing.T) {
	logName := "test_log" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	test.Nil(t, err)

	batchNum := 10
	msgRawSize := 10
	var logData CommitLogData
	logData.Epoch = 1
	logData.LogID = int64(logMgr.NextID())
	logData.MsgOffset = int64(0)
	logData.MsgCnt = int64(1)
	endID := int64(0)
	endID += int64(batchNum)
	for j := 0; j < batchNum; j++ {
		logData.MsgSize += int32(msgRawSize)
	}
	logData.LastMsgLogID = endID
	logData.MsgNum = int32(batchNum)
	// append as slave
	err = logMgr.AppendCommitLog(&logData, true)
	test.Nil(t, err)
	test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
	nextID := logMgr.NextID()
	test.Equal(t, int64(nextID) > endID, true)
	test.Equal(t, logMgr.nLogID > logData.LastMsgLogID, true)
	lastOffset, err := logMgr.GetLastLogOffset()
	test.Nil(t, err)
	lastLog, err := logMgr.GetCommitLogFromOffset(lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	test.Equal(t, lastLog.MsgNum, int32(batchNum))
}
