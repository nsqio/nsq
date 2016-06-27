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
		test.Equal(t, int64(logMgr.nLogID) >= logData.LastMsgLogID, true)
	}
	startIndex, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((num-1)*GetLogDataSize()), lastOffset)
	lastLog, err := logMgr.GetCommitLogFromOffsetV2(startIndex, lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	alllogs, err := logMgr.GetCommitLogsV2(startIndex, 0, num)
	test.Nil(t, err)
	alllogs, err = logMgr.GetCommitLogsV2(startIndex, 0, num*2)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, len(alllogs), num)
	var prevLog CommitLogData
	for i := 0; i < num; i++ {
		logs, err := logMgr.GetCommitLogsV2(startIndex, int64(i*GetLogDataSize()), 1)
		test.Nil(t, err)
		test.Equal(t, len(logs), 1)
		if prevLog.LogID > 0 {
			test.Equal(t, prevLog.LogID < logs[0].LogID, true)
			test.Equal(t, prevLog.MsgOffset+int64(msgRawSize), logs[0].MsgOffset)
			test.Equal(t, prevLog.MsgCnt+1, logs[0].MsgCnt)
			test.Equal(t, prevLog.MsgNum, int32(1))
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
		test.Equal(t, int64(logMgr.nLogID) >= logData.LastMsgLogID, true)
	}
	startIndex, lastOffset, _, err = logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	lastLog, err = logMgr.GetCommitLogFromOffsetV2(startIndex, lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	for i := num; i < num*2; i++ {
		logs, err := logMgr.GetCommitLogsV2(startIndex, int64(i*GetLogDataSize()), 1)
		test.Nil(t, err)
		test.Equal(t, len(logs), 1)
		if prevLog.LogID > 0 {
			test.Equal(t, prevLog.LogID < logs[0].LogID, true)
			test.Equal(t, prevLog.MsgOffset+int64(msgRawSize), logs[0].MsgOffset)
			test.Equal(t, prevLog.MsgCnt+int64(1), logs[0].MsgCnt)
			test.Equal(t, prevLog.MsgNum, int32(1))
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
	prevLog, err := logMgr.TruncateToOffsetV2(0, truncateOffset)
	test.Nil(t, err)
	test.Equal(t, truncateId, logMgr.pLogID)
	test.Equal(t, truncateId, prevLog.LogID)
	test.Equal(t, int64(truncateAtCnt-1), prevLog.MsgCnt)
	prevLog, err = logMgr.TruncateToOffsetV2(0, 0)
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
	lastIndex, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	lastLog, err := logMgr.GetCommitLogFromOffsetV2(lastIndex, lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	test.Equal(t, lastLog.MsgNum, int32(batchNum))
}

func TestCommitLogTruncateMultiSegments(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()

	logName := "test_log_truncate" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
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
	}
	currentLogIndex := logMgr.GetCurrentStart()
	prevLog, err := logMgr.TruncateToOffsetV2(currentLogIndex, 0)
	test.Nil(t, err)
	test.Equal(t, currentLogIndex, logMgr.GetCurrentStart())
	endIndex, endOffset := logMgr.GetCurrentEnd()
	test.Equal(t, int64(0), endOffset)
	test.Equal(t, currentLogIndex, endIndex)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(num-LOGROTATE_NUM+1), prevLog.LogID)
	test.Equal(t, int64(num-LOGROTATE_NUM-1), prevLog.MsgCnt)
	currentLogIndex = logMgr.GetCurrentStart()
	prevLog, err = logMgr.TruncateToOffsetV2(currentLogIndex-1, int64(LOGROTATE_NUM)*int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, currentLogIndex-1, logMgr.GetCurrentStart())
	endIndex, endOffset = logMgr.GetCurrentEnd()
	test.Equal(t, int64(LOGROTATE_NUM)*int64(GetLogDataSize()), endOffset)
	test.Equal(t, currentLogIndex-1, endIndex)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(num-LOGROTATE_NUM+1), prevLog.LogID)
	test.Equal(t, int64(num-LOGROTATE_NUM-1), prevLog.MsgCnt)

	currentLogIndex = logMgr.GetCurrentStart()
	prevLog, err = logMgr.TruncateToOffsetV2(currentLogIndex-1, int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, currentLogIndex-1, logMgr.GetCurrentStart())
	endIndex, endOffset = logMgr.GetCurrentEnd()
	test.Equal(t, int32(1), logMgr.currentCount)
	test.Equal(t, int64(GetLogDataSize())*int64(logMgr.currentCount), endOffset)
	test.Equal(t, currentLogIndex-1, endIndex)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount+1), prevLog.LogID)
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount-1), prevLog.MsgCnt)
	logs, err := logMgr.GetCommitLogsV2(logMgr.currentStart, 0, 1)
	test.Nil(t, err)
	test.Equal(t, len(logs), 1)
	logs, err = logMgr.GetCommitLogsV2(logMgr.currentStart, 0, 2)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, len(logs), 1)

	currentLogIndex = logMgr.GetCurrentStart()
	prevLog, err = logMgr.TruncateToOffsetV2(currentLogIndex-1, 0)
	test.Nil(t, err)
	test.Equal(t, currentLogIndex-1, logMgr.GetCurrentStart())
	endIndex, endOffset = logMgr.GetCurrentEnd()
	test.Equal(t, int32(0), logMgr.currentCount)
	test.Equal(t, int64(logMgr.currentCount)*int64(GetLogDataSize()), endOffset)
	test.Equal(t, currentLogIndex-1, endIndex)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount+1), prevLog.LogID)
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount-1), prevLog.MsgCnt)

}

func TestCommitLogRotate(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()
	logName := "test_log_rotate" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(4)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)

	test.Nil(t, err)
	test.Equal(t, logMgr.pLogID, int64(0))
	test.Equal(t, logMgr.nLogID > logMgr.pLogID, true)
	test.Equal(t, logMgr.GetLastCommitLogID(), int64(0))

	_, _, _, err = logMgr.GetLastCommitLogOffsetV2()
	test.Equal(t, ErrCommitLogEOF, err)

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
		t.Logf("index:%v, %v, %v", i, logMgr.GetCurrentStart(), logMgr.currentCount)
		test.Equal(t, logMgr.GetCurrentStart(), int64(i/LOGROTATE_NUM))
		test.Equal(t, int64(logMgr.currentCount), int64(i)-int64(i/LOGROTATE_NUM)*int64(LOGROTATE_NUM)+1)
		logIndex, logOffset, lastLogData, err := logMgr.GetLastCommitLogOffsetV2()
		test.Equal(t, nil, err)
		test.Equal(t, int64(i/LOGROTATE_NUM), logIndex)
		test.Equal(t, int64(logMgr.currentCount-1)*int64(GetLogDataSize()), logOffset)
		test.Equal(t, int64(i), lastLogData.MsgCnt)
	}
	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())

	lastLog, err := logMgr.GetCommitLogFromOffsetV2(currentStart, lastOffset)
	test.Nil(t, err)
	test.Equal(t, lastLog.LogID, logMgr.GetLastCommitLogID())
	alllogs, err := logMgr.GetCommitLogsV2(currentStart, 0, LOGROTATE_NUM)
	test.Nil(t, err)
	test.Equal(t, len(alllogs), LOGROTATE_NUM)
	alllogs, err = logMgr.GetCommitLogsV2(currentStart, 0, LOGROTATE_NUM*2)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, len(alllogs), LOGROTATE_NUM)
	// test reopen
	oldLogID := logMgr.pLogID
	oldNLogID := logMgr.nLogID
	logMgr.Close()
	logMgr, err = InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	test.Nil(t, err)
	test.Equal(t, logMgr.pLogID, oldLogID)
	test.Equal(t, logMgr.nLogID, oldNLogID+1)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())

	var prevLog CommitLogData
	for i := 0; i < LOGROTATE_NUM; i++ {
		logs, err := logMgr.GetCommitLogsV2(0, int64(i*GetLogDataSize()), 1)
		test.Nil(t, err)
		test.Equal(t, len(logs), 1)
		if prevLog.LogID > 0 {
			test.Equal(t, prevLog.LogID < logs[0].LogID, true)
			test.Equal(t, prevLog.MsgOffset+int64(msgRawSize), logs[0].MsgOffset)
			test.Equal(t, prevLog.MsgCnt+1, logs[0].MsgCnt)
			test.Equal(t, prevLog.MsgNum, int32(1))
		}
		prevLog = logs[0]
	}
	for cnt := 0; cnt < LOGROTATE_NUM; cnt++ {
		l, err := logMgr.GetCommitLogFromOffsetV2(logMgr.currentStart, int64(cnt*GetLogDataSize()))
		test.Nil(t, err)
		test.Equal(t, l.LogID, logMgr.currentStart*int64(LOGROTATE_NUM)+int64(cnt)+2)
		test.Equal(t, l.MsgCnt, logMgr.currentStart*int64(LOGROTATE_NUM)+int64(cnt))
	}
	for i := int64(0); i < logMgr.currentStart; i++ {
		for cnt := 0; cnt < LOGROTATE_NUM; cnt++ {
			l, err := logMgr.GetCommitLogFromOffsetV2(i, int64(cnt*GetLogDataSize()))
			test.Nil(t, err)
			test.Equal(t, i*int64(LOGROTATE_NUM)+int64(cnt), l.MsgCnt)
		}
	}

	logs, err := logMgr.GetCommitLogsV2(logMgr.currentStart, 0, LOGROTATE_NUM+1)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, len(logs), LOGROTATE_NUM)

	logs, err = logMgr.GetCommitLogsV2(0, 0, LOGROTATE_NUM-1)
	test.Equal(t, len(logs), LOGROTATE_NUM-1)
	test.Nil(t, err)
	logs, err = logMgr.GetCommitLogsV2(0, 0, LOGROTATE_NUM)
	test.Nil(t, err)
	test.Equal(t, len(logs), LOGROTATE_NUM)
	logs, err = logMgr.GetCommitLogsV2(0, 0, LOGROTATE_NUM+1)
	test.Nil(t, err)
	test.Equal(t, len(logs), LOGROTATE_NUM+1)
	logs, err = logMgr.GetCommitLogsV2(0, 0, num-1)
	test.Nil(t, err)
	test.Equal(t, len(logs), num-1)
	logs, err = logMgr.GetCommitLogsV2(0, 0, num)
	test.Nil(t, err)
	test.Equal(t, len(logs), num)
	logs, err = logMgr.GetCommitLogsV2(0, 0, num+1)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, len(logs), num)
	logs, err = logMgr.GetCommitLogsV2(0, 0, num*2)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, len(logs), num)
	logs, err = logMgr.GetCommitLogsV2(1, int64(GetLogDataSize()), LOGROTATE_NUM)
	test.Nil(t, err)
	test.Equal(t, len(logs), LOGROTATE_NUM)
}

func TestCommitLogSearch(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()
	logName := "test_log_search" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(4)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)

	test.Nil(t, err)

	// single pub
	num := 100
	msgRawSize := 10
	for i := 0; i < num/2; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgSize = int32(msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		test.Equal(t, logMgr.GetCurrentStart(), int64(i/LOGROTATE_NUM))
		test.Equal(t, int64(logMgr.currentCount), int64(i)-int64(i/LOGROTATE_NUM)*int64(LOGROTATE_NUM)+1)
	}
	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	// singe and multi pub
	lastCnt := num / 2
	for i := num / 2; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.MsgCnt = int64(lastCnt + 1)
		logData.MsgOffset = int64(lastCnt * msgRawSize)
		if i%3 == 0 {
			logData.LastMsgLogID = int64(logMgr.NextID())
			logData.MsgSize = int32(msgRawSize * 2)
			logData.MsgNum = 2
			lastCnt += 2
		} else {
			logData.LastMsgLogID = logData.LogID
			logData.MsgSize = int32(msgRawSize)
			logData.MsgNum = 1
			lastCnt += 1
		}
		logData.Epoch = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		test.Equal(t, logMgr.GetCurrentStart(), int64(i/LOGROTATE_NUM))
		test.Equal(t, int64(logMgr.currentCount), int64(i)-int64(i/LOGROTATE_NUM)*int64(LOGROTATE_NUM)+1)
	}

	t.Logf("total %v messages", lastCnt)
	for i := 1; i < lastCnt; i++ {
		logIndex, offset, l, err := logMgr.SearchLogDataByMsgCnt(int64(i))
		test.Nil(t, err)
		t.Logf("searched: %v:%v, %v", logIndex, offset, l)
		if i < num/2 {
			test.Equal(t, int64(i), l.MsgCnt)
			test.Equal(t, int64((i-1)/LOGROTATE_NUM), logIndex)
			test.Equal(t, l.MsgOffset, int64((i-1)*msgRawSize))
		} else {
			// multi pub will cause the msg count different.
			test.Equal(t, l.MsgCnt <= int64(i), true)
			test.Equal(t, l.MsgCnt+int64(l.MsgNum-1) >= int64(i), true)
		}
	}
	lastCommitID := logMgr.GetLastCommitLogID()
	for i := int64(2); i < lastCommitID; i++ {
		_, _, l, err := logMgr.SearchLogDataByMsgID(int64(i))
		test.Nil(t, err)
		if i < int64(num/2) {
			test.Equal(t, int64(i), l.LogID)
		} else {
			test.Equal(t, l.LogID <= int64(i), true)
			test.Equal(t, l.LastMsgLogID >= int64(i), true)
		}
	}

	searchOffset := int64(0)
	_, _, lastLog, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	for i := 1; i < lastCnt; i++ {
		_, _, l, err := logMgr.SearchLogDataByMsgOffset(searchOffset)
		test.Nil(t, err)
		if i < num/2 {
			test.Equal(t, searchOffset, l.MsgOffset)
		} else {
			test.Equal(t, l.MsgOffset <= searchOffset, true)
			test.Equal(t, l.MsgOffset+int64(l.MsgSize) >= searchOffset, true)
		}
		searchOffset = l.MsgOffset + int64(l.MsgSize)
		if searchOffset > lastLog.MsgOffset {
			break
		}
	}
}
