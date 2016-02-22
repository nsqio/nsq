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
	coordLog.logger = newTestLogger(t)
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
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
	}
	logMgr.FlushCommitLogs()
	lastOffset, err := logMgr.GetLastLogOffset()
	test.Nil(t, err)
	lastLog, err := logMgr.GetCommmitLogFromOffset(lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	var prevLog CommitLogData
	for i := 0; i < num; i++ {
		logs, err := logMgr.GetCommitLogs(int64(i*GetLogDataSize()), 1)
		test.Nil(t, err)
		test.Equal(t, len(logs), 1)
		if prevLog.LogID > 0 {
			test.Equal(t, prevLog.LogID+1, logs[0].LogID)
			test.Equal(t, prevLog.MsgOffset+int64(msgRawSize), logs[0].MsgOffset)
		}
		prevLog = logs[0]
	}
	for i := num; i < num*2; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
	}
	logMgr.FlushCommitLogs()
	lastOffset, err = logMgr.GetLastLogOffset()
	test.Nil(t, err)
	lastLog, err = logMgr.GetCommmitLogFromOffset(lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	for i := num; i < num*2; i++ {
		logs, err := logMgr.GetCommitLogs(int64(i*GetLogDataSize()), 1)
		test.Nil(t, err)
		test.Equal(t, len(logs), 1)
		if prevLog.LogID > 0 {
			test.Equal(t, prevLog.LogID+1, logs[0].LogID)
			test.Equal(t, prevLog.MsgOffset+int64(msgRawSize), logs[0].MsgOffset)
		}
		prevLog = logs[0]
	}
}
