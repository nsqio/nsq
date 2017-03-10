package consistence

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

func newTestLogger(tbl test.TbLog) levellogger.Logger {
	return &test.TestLogger{tbl, 0}
}

func TestCommitLogGetPartitionIDFromMsgID(t *testing.T) {
	for pid := 0; pid < 1024; pid++ {
		msgID := int64(uint64(pid)<<MAX_INCR_ID_BIT + 1)
		test.Equal(t, pid, GetPartitionFromMsgID(msgID))
		cnt := 0
		for cnt < 10000 {
			msgID += int64(rand.Intn(100))
			test.Equal(t, pid, GetPartitionFromMsgID(msgID))
			cnt++
		}
		msgID = int64(uint64(pid)<<MAX_INCR_ID_BIT + uint64(1)<<MAX_INCR_ID_BIT - 1)
		test.Equal(t, pid, GetPartitionFromMsgID(msgID))
	}
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
	cntIndex, err := logMgr.ConvertToCountIndex(0, 0)
	test.Nil(t, err)
	test.Equal(t, int64(0), cntIndex)

	num := 100
	msgRawSize := 10
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		test.Equal(t, int64(logMgr.nLogID) >= logData.LastMsgLogID, true)
	}
	startIndex, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((num-1)*GetLogDataSize()), lastOffset)
	cntIndex, err = logMgr.ConvertToCountIndex(startIndex, lastOffset)
	test.Nil(t, err)
	test.Equal(t, int64(num-1), cntIndex)

	convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, startIndex, convertedIndex)
	test.Equal(t, lastOffset, convertedOffset)
	cntIndex, err = logMgr.ConvertToCountIndex(startIndex, lastOffset+int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, int64(num), cntIndex)
	convertedIndex, convertedOffset, err = logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, startIndex, convertedIndex)
	test.Equal(t, lastOffset+int64(GetLogDataSize()), convertedOffset)

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

		cntIndex, err = logMgr.ConvertToCountIndex(startIndex, int64(i*GetLogDataSize()))
		test.Nil(t, err)
		test.Equal(t, int64(i), cntIndex)
		convertedIndex, convertedOffset, err = logMgr.ConvertToOffsetIndex(cntIndex)
		test.Nil(t, err)
		test.Equal(t, startIndex, convertedIndex)
		test.Equal(t, int64(i*GetLogDataSize()), convertedOffset)
	}
	for i := num; i < num*2; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
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
	currentStart := logMgr.currentStart
	currentCount := logMgr.currentCount
	logMgr.Close()
	logMgr, err = InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	test.Equal(t, currentStart, logMgr.currentStart)
	test.Equal(t, currentCount, logMgr.currentCount)
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
		logData.MsgCnt = int64(i + 1)
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
	test.Equal(t, int64(truncateAtCnt), prevLog.MsgCnt)
	prevLog, err = logMgr.TruncateToOffsetV2(0, 0)
	test.Equal(t, ErrCommitLogEOF, err)
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

	cntIndex, err := logMgr.ConvertToCountIndex(lastIndex, lastOffset+int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, int64(1), cntIndex)
	convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, lastIndex, convertedIndex)
	test.Equal(t, lastOffset+int64(GetLogDataSize()), convertedOffset)

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
		logData.MsgCnt = int64(i + 1)
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
	test.Equal(t, int64(num-LOGROTATE_NUM), prevLog.MsgCnt)
	currentLogIndex = logMgr.GetCurrentStart()
	prevLog, err = logMgr.TruncateToOffsetV2(currentLogIndex-1, int64(LOGROTATE_NUM)*int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, currentLogIndex-1, logMgr.GetCurrentStart())
	endIndex, endOffset = logMgr.GetCurrentEnd()
	test.Equal(t, int64(LOGROTATE_NUM)*int64(GetLogDataSize()), endOffset)
	test.Equal(t, currentLogIndex-1, endIndex)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(num-LOGROTATE_NUM+1), prevLog.LogID)
	test.Equal(t, int64(num-LOGROTATE_NUM), prevLog.MsgCnt)

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
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount), prevLog.MsgCnt)
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
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount), prevLog.MsgCnt)

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
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		//t.Logf("index:%v, %v, %v", i, logMgr.GetCurrentStart(), logMgr.currentCount)
		test.Equal(t, logMgr.GetCurrentStart(), int64(i/LOGROTATE_NUM))
		test.Equal(t, int64(logMgr.currentCount), int64(i)-int64(i/LOGROTATE_NUM)*int64(LOGROTATE_NUM)+1)
		logIndex, logOffset, lastLogData, err := logMgr.GetLastCommitLogOffsetV2()
		test.Equal(t, nil, err)
		test.Equal(t, int64(i/LOGROTATE_NUM), logIndex)
		test.Equal(t, int64(logMgr.currentCount-1)*int64(GetLogDataSize()), logOffset)
		test.Equal(t, int64(i+1), lastLogData.MsgCnt)

		cntIndex, err := logMgr.ConvertToCountIndex(logMgr.GetCurrentStart(), logOffset)
		test.Nil(t, err)
		test.Equal(t, int64(i), cntIndex)
		convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
		test.Nil(t, err)
		test.Equal(t, logMgr.GetCurrentStart(), convertedIndex)
		test.Equal(t, logOffset, convertedOffset)

	}
	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())

	cntIndex, err := logMgr.ConvertToCountIndex(currentStart, lastOffset+int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, int64(num), cntIndex)
	convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, currentStart, convertedIndex)
	test.Equal(t, lastOffset+int64(GetLogDataSize()), convertedOffset)

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
		test.Equal(t, l.MsgCnt, logMgr.currentStart*int64(LOGROTATE_NUM)+int64(cnt+1))
	}
	for i := int64(0); i < logMgr.currentStart; i++ {
		for cnt := 0; cnt < LOGROTATE_NUM; cnt++ {
			l, err := logMgr.GetCommitLogFromOffsetV2(i, int64(cnt*GetLogDataSize()))
			test.Nil(t, err)
			test.Equal(t, i*int64(LOGROTATE_NUM)+int64(cnt+1), l.MsgCnt)

			cntIndex, err := logMgr.ConvertToCountIndex(i, int64(cnt*GetLogDataSize()))
			test.Nil(t, err)
			test.Equal(t, i*int64(LOGROTATE_NUM)+int64(cnt), cntIndex)
			convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
			test.Nil(t, err)
			test.Equal(t, i, convertedIndex)
			test.Equal(t, int64(cnt*GetLogDataSize()), convertedOffset)

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

func TestCommitLogRotateForReopen(t *testing.T) {
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
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		// test reopen during append
		if logMgr.currentCount == int32(LOGROTATE_NUM/2) {
			logMgr.Close()
			logMgr, err = InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
			test.Nil(t, err)
		}
	}
	logMgr.FlushCommitLogs()
	for i := int64(0); i < logMgr.currentStart; i++ {
		s, err := os.Stat(getSegmentFilename(logMgr.path, i))
		test.Nil(t, err)
		test.Equal(t, int64(GetLogDataSize()*LOGROTATE_NUM), s.Size())
	}
	stat, err := os.Stat(logMgr.path)
	test.Nil(t, err)
	test.Equal(t, true, stat.Size() <= int64(GetLogDataSize()*LOGROTATE_NUM))
	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
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
	coordLog.SetLevel(3)
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
		logIndex, _, l, err := logMgr.SearchLogDataByMsgCnt(int64(i))
		test.Nil(t, err)
		//t.Logf("searched: %v:%v, %v", logIndex, offset, l)
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

	logMgr.CleanOldData(1, int64(GetLogDataSize()))
	logStart, startLog, _ := logMgr.GetLogStartInfo()
	t.Logf("search in the cleaned commit log : %v", logStart)
	for i := 1; i < lastCnt; i++ {
		logIndex, offset, l, err := logMgr.SearchLogDataByMsgCnt(int64(i))
		t.Logf("searched: %v, %v", logIndex, i)
		if int64(i) < logStart.SegmentStartCount+1 {
			if err == nil {
				test.NotEqual(t, int64(i), l.MsgCnt)
				test.Equal(t, true, logIndex > logStart.SegmentStartIndex || offset >= logStart.SegmentStartOffset)
			} else {
				test.Equal(t, ErrCommitLogSearchNotFound, err)
			}
			continue
		}
		test.Nil(t, err)
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
	lastCommitID = logMgr.GetLastCommitLogID()
	for i := int64(2); i < lastCommitID; i++ {
		logIndex, offset, l, err := logMgr.SearchLogDataByMsgID(int64(i))
		if int64(i) < startLog.LogID {
			if err == nil {
				test.NotEqual(t, int64(i), l.LogID)
				test.Equal(t, true, logIndex > logStart.SegmentStartIndex || offset >= logStart.SegmentStartOffset)
			} else {
				test.Equal(t, ErrCommitLogSearchNotFound, err)
			}
			continue
		}
		test.Nil(t, err)
		if i < int64(num/2) {
			test.Equal(t, int64(i), l.LogID)
		} else {
			test.Equal(t, l.LogID <= int64(i), true)
			test.Equal(t, l.LastMsgLogID >= int64(i), true)
		}
	}

	searchOffset = int64(0)
	_, _, lastLog, err = logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	for i := 1; i < lastCnt; i++ {
		logIndex, offset, l, err := logMgr.SearchLogDataByMsgOffset(searchOffset)
		if searchOffset < startLog.MsgOffset {
			if err == nil {
				test.NotEqual(t, searchOffset, l.MsgOffset)
				test.Equal(t, true, logIndex > logStart.SegmentStartIndex || offset >= logStart.SegmentStartOffset)
			} else {
				test.Equal(t, ErrCommitLogSearchNotFound, err)
			}
			continue
		}
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

func TestCommitLogCleanOld(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()
	logName := "test_log_clean_old" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(4)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)

	test.Nil(t, err)

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
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
	}

	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	logMgr.FlushCommitLogs()

	cntIndex, err := logMgr.ConvertToCountIndex(currentStart, lastOffset+int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, int64(num), cntIndex)
	convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, currentStart, convertedIndex)
	test.Equal(t, lastOffset+int64(GetLogDataSize()), convertedOffset)

	_, _, err = logMgr.GetLogStartInfo()
	test.Nil(t, err)
	for i := int64(0); i < currentStart-1; i++ {
		err = logMgr.CleanOldData(i, 0)
		test.Nil(t, err)
		realStart, firstLog, err := logMgr.GetLogStartInfo()
		test.Nil(t, err)
		test.Equal(t, i, realStart.SegmentStartIndex)
		test.Equal(t, int64(0), realStart.SegmentStartOffset)
		test.Equal(t, i*int64(LOGROTATE_NUM), realStart.SegmentStartCount)
		test.Equal(t, i*int64(LOGROTATE_NUM)*int64(msgRawSize), firstLog.MsgOffset)
		for j := int64(0); j <= i; j++ {
			tmpFileName := getSegmentFilename(logMgr.path, j)
			_, err = os.Stat(tmpFileName)
			if j < i-1 {
				t.Logf("check file : %v", j)
				test.NotNil(t, err)
				test.Equal(t, true, os.IsNotExist(err))
			} else {
				test.Nil(t, err)
			}
		}
		test.Equal(t, i, logMgr.logStartInfo.SegmentStartIndex)
		test.Equal(t, i*int64(LOGROTATE_NUM), logMgr.logStartInfo.SegmentStartCount)
		for j := int64(0); j < currentStart-1; j++ {
			for k := 0; k < LOGROTATE_NUM; k++ {
				if j < i {
					_, err := logMgr.GetCommitLogFromOffsetV2(j, int64(k*GetLogDataSize()))
					test.NotNil(t, err)
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
					_, err = logMgr.ConvertToCountIndex(j, int64(k*GetLogDataSize()))
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
					_, _, err = logMgr.ConvertToOffsetIndex(j*int64(LOGROTATE_NUM) + int64(k))
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
				} else {
					l, err := logMgr.GetCommitLogFromOffsetV2(j, int64(k*GetLogDataSize()))
					test.Nil(t, err)
					test.Equal(t, j*int64(LOGROTATE_NUM)+int64(k+1), l.MsgCnt)

					for num := 1; num < 100-int(j)*LOGROTATE_NUM-k; num++ {
						logs, err := logMgr.GetCommitLogsV2(j, int64(k*GetLogDataSize()), num)
						test.Nil(t, err)
						test.Equal(t, len(logs), num)
					}

					cntIndex, err := logMgr.ConvertToCountIndex(j, int64(k*GetLogDataSize()))
					test.Nil(t, err)
					test.Equal(t, j*int64(LOGROTATE_NUM)+int64(k), cntIndex)
					convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
					test.Nil(t, err)
					test.Equal(t, j, convertedIndex)
					test.Equal(t, int64(k*GetLogDataSize()), convertedOffset)
				}
			}
		}
	}
}

func TestCommitLogCleanOldKeepMinItems(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	oldMinItem := MIN_KEEP_LOG_ITEM
	LOGROTATE_NUM = 10
	MIN_KEEP_LOG_ITEM = LOGROTATE_NUM / 2
	defer func() {
		LOGROTATE_NUM = oldRotate
		MIN_KEEP_LOG_ITEM = oldMinItem
	}()
	logName := "test_log_clean_old" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(4)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)

	test.Nil(t, err)

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
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)

		fileNum, msgCurCnt := logMgr.GetCurrentEnd()
		if fileNum > 0 && msgCurCnt < int64(MIN_KEEP_LOG_ITEM) {
			err = logMgr.CleanOldData(fileNum-1, int64(LOGROTATE_NUM)*int64(GetLogDataSize()))
			test.Equal(t, ErrCommitLogCleanKeepMin, err)
		}
	}

	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	logMgr.FlushCommitLogs()
	t.Logf("last log %v, %v\n", currentStart, lastOffset)

	// should keep at least some log items
	err = logMgr.CleanOldData(currentStart, lastOffset-int64(MIN_KEEP_LOG_ITEM-1)*int64(GetLogDataSize()))
	test.Equal(t, ErrCommitLogCleanKeepMin, err)
	tmpFileName := getSegmentFilename(logMgr.path, 0)
	_, err = os.Stat(tmpFileName)
	test.Nil(t, err)
}

func TestCommitLogCleanOldAtMiddleOfSeg(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	oldMinItem := MIN_KEEP_LOG_ITEM
	LOGROTATE_NUM = 10
	MIN_KEEP_LOG_ITEM = LOGROTATE_NUM / 2
	defer func() {
		LOGROTATE_NUM = oldRotate
		MIN_KEEP_LOG_ITEM = oldMinItem
	}()
	logName := "test_log_clean_old" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(4)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	test.Nil(t, err)

	num := 100
	msgRawSize := 10
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
	}

	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	logMgr.FlushCommitLogs()

	for i := int64(0); i < currentStart; i++ {
		// clean at first offset
		err = logMgr.CleanOldData(i, int64(GetLogDataSize()))
		if err == ErrCommitLogCleanKeepMin {
			continue
		}
		test.Nil(t, err)
		realStart, firstLog, err := logMgr.GetLogStartInfo()
		test.Nil(t, err)
		test.Equal(t, i, realStart.SegmentStartIndex)
		test.Equal(t, int64(GetLogDataSize()), realStart.SegmentStartOffset)
		test.Equal(t, i*int64(LOGROTATE_NUM)+1, realStart.SegmentStartCount)
		test.Equal(t, i*int64(LOGROTATE_NUM)*int64(msgRawSize)+int64(msgRawSize), firstLog.MsgOffset)
		for j := int64(0); j < i; j++ {
			tmpFileName := getSegmentFilename(logMgr.path, j)
			_, err = os.Stat(tmpFileName)
			if j < i-1 {
				test.NotNil(t, err)
				test.Equal(t, true, os.IsNotExist(err))
			} else {
				test.Nil(t, err)
			}
		}
		for j := int64(0); j < currentStart; j++ {
			for k := 0; k < LOGROTATE_NUM; k++ {
				if j < i || (j == i && k < 1) {
					_, err := logMgr.GetCommitLogFromOffsetV2(j, int64(k*GetLogDataSize()))
					test.NotNil(t, err)
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
					_, err = logMgr.ConvertToCountIndex(j, int64(k*GetLogDataSize()))
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
					_, _, err = logMgr.ConvertToOffsetIndex(j*int64(LOGROTATE_NUM) + int64(k))
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
				} else {
					l, err := logMgr.GetCommitLogFromOffsetV2(j, int64(k*GetLogDataSize()))
					test.Nil(t, err)
					test.Equal(t, j*int64(LOGROTATE_NUM)+int64(k+1), l.MsgCnt)

					for num := 1; num < 100-int(j)*LOGROTATE_NUM-k; num++ {
						logs, err := logMgr.GetCommitLogsV2(j, int64(k*GetLogDataSize()), num)
						test.Nil(t, err)
						test.Equal(t, len(logs), num)
					}

					cntIndex, err := logMgr.ConvertToCountIndex(j, int64(k*GetLogDataSize()))
					test.Nil(t, err)
					test.Equal(t, j*int64(LOGROTATE_NUM)+int64(k), cntIndex)
					convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
					test.Nil(t, err)
					test.Equal(t, j, convertedIndex)
					test.Equal(t, int64(k*GetLogDataSize()), convertedOffset)
				}
			}
		}

		err = logMgr.CleanOldData(i, int64(GetLogDataSize())*int64(LOGROTATE_NUM))
		if err == ErrCommitLogCleanKeepMin {
			continue
		}
		test.Nil(t, err)
		realStart, firstLog, err = logMgr.GetLogStartInfo()
		test.Nil(t, err)
		test.Equal(t, i+1, realStart.SegmentStartIndex)
		test.Equal(t, int64(0), realStart.SegmentStartOffset)
		test.Equal(t, i*int64(LOGROTATE_NUM)+int64(LOGROTATE_NUM), realStart.SegmentStartCount)
		test.Equal(t, i*int64(LOGROTATE_NUM)*int64(msgRawSize)+int64(msgRawSize*LOGROTATE_NUM), firstLog.MsgOffset)
		for j := int64(0); j < i; j++ {
			tmpFileName := getSegmentFilename(logMgr.path, j)
			_, err = os.Stat(tmpFileName)
			if j < i-1 {
				test.NotNil(t, err)
				test.Equal(t, true, os.IsNotExist(err))
			} else {
				test.Nil(t, err)
			}
		}
		for j := int64(0); j < currentStart; j++ {
			for k := 0; k < LOGROTATE_NUM; k++ {
				if j < i || (j == i && k < LOGROTATE_NUM) {
					_, err := logMgr.GetCommitLogFromOffsetV2(j, int64(k*GetLogDataSize()))
					test.NotNil(t, err)
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
					_, err = logMgr.ConvertToCountIndex(j, int64(k*GetLogDataSize()))
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
					_, _, err = logMgr.ConvertToOffsetIndex(j*int64(LOGROTATE_NUM) + int64(k))
					test.Equal(t, ErrCommitLogLessThanSegmentStart, err)
				} else {
					l, err := logMgr.GetCommitLogFromOffsetV2(j, int64(k*GetLogDataSize()))
					test.Nil(t, err)
					test.Equal(t, j*int64(LOGROTATE_NUM)+int64(k+1), l.MsgCnt)

					for num := 1; num < 100-int(j)*LOGROTATE_NUM-k; num++ {
						logs, err := logMgr.GetCommitLogsV2(j, int64(k*GetLogDataSize()), num)
						test.Nil(t, err)
						test.Equal(t, len(logs), num)
					}

					cntIndex, err := logMgr.ConvertToCountIndex(j, int64(k*GetLogDataSize()))
					test.Nil(t, err)
					test.Equal(t, j*int64(LOGROTATE_NUM)+int64(k), cntIndex)
					convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
					test.Nil(t, err)
					l1, _ := logMgr.GetCommitLogFromOffsetV2(j, int64(k*GetLogDataSize()))
					l2, _ := logMgr.GetCommitLogFromOffsetV2(convertedIndex, convertedOffset)
					test.Equal(t, l1, l2)
				}
			}
		}
	}
}

func TestCommitLogTruncateAndCleanOld(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	oldMinItem := MIN_KEEP_LOG_ITEM
	LOGROTATE_NUM = 10
	MIN_KEEP_LOG_ITEM = LOGROTATE_NUM / 2
	defer func() {
		LOGROTATE_NUM = oldRotate
		MIN_KEEP_LOG_ITEM = oldMinItem
	}()
	logName := "test_log_clean_old" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(4)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	test.Nil(t, err)

	num := 100
	msgRawSize := 10
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
	}

	currentLogIndex, _, _, err := logMgr.GetLastCommitLogOffsetV2()

	err = logMgr.CleanOldData(currentLogIndex/2, int64(GetLogDataSize()))
	test.Nil(t, err)

	prevLog, err := logMgr.TruncateToOffsetV2(currentLogIndex, 0)
	test.Nil(t, err)
	test.Equal(t, currentLogIndex, logMgr.GetCurrentStart())
	endIndex, endOffset := logMgr.GetCurrentEnd()
	test.Equal(t, int64(0), endOffset)
	test.Equal(t, currentLogIndex, endIndex)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(num-LOGROTATE_NUM+1), prevLog.LogID)
	test.Equal(t, int64(num-LOGROTATE_NUM), prevLog.MsgCnt)
	currentLogIndex = logMgr.GetCurrentStart()
	prevLog, err = logMgr.TruncateToOffsetV2(currentLogIndex-1, int64(LOGROTATE_NUM)*int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, currentLogIndex-1, logMgr.GetCurrentStart())
	endIndex, endOffset = logMgr.GetCurrentEnd()
	test.Equal(t, int64(LOGROTATE_NUM)*int64(GetLogDataSize()), endOffset)
	test.Equal(t, currentLogIndex-1, endIndex)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(num-LOGROTATE_NUM+1), prevLog.LogID)
	test.Equal(t, int64(num-LOGROTATE_NUM), prevLog.MsgCnt)

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
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount), prevLog.MsgCnt)
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
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount), prevLog.MsgCnt)

	logStart, _, _ := logMgr.GetLogStartInfo()
	prevLog, err = logMgr.TruncateToOffsetV2(logStart.SegmentStartIndex, logStart.SegmentStartOffset+int64(GetLogDataSize()))
	test.Nil(t, err)
	cntIndex, _ := logMgr.ConvertToCountIndex(logStart.SegmentStartIndex, logStart.SegmentStartOffset+int64(GetLogDataSize()))
	test.Equal(t, logStart.SegmentStartCount+int64(1), cntIndex)
	test.Equal(t, int32(2), logMgr.currentCount)
	test.Equal(t, prevLog.LogID, logMgr.pLogID)
	test.Equal(t, int64(LOGROTATE_NUM)*int64(logMgr.currentStart)+int64(logMgr.currentCount+1), prevLog.LogID)

	prevLog, err = logMgr.TruncateToOffsetV2(logStart.SegmentStartIndex, logStart.SegmentStartOffset)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, int32(1), logMgr.currentCount)
	test.Equal(t, int64(0), logMgr.GetLastCommitLogID())
	prevLog, err = logMgr.TruncateToOffsetV2(logStart.SegmentStartIndex, logStart.SegmentStartOffset-int64(GetLogDataSize()))
	test.NotNil(t, err)
}

func TestCommitLogResetWithStart(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()
	logName := "test_log_reset_start" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(3)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	test.Nil(t, err)

	num := 100
	msgRawSize := 10
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
	}

	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	logMgr.FlushCommitLogs()

	var newStart LogStartInfo
	newStart.SegmentStartIndex = currentStart * 2
	newStart.SegmentStartCount = int64(num * 2)
	// reset with new start
	err = logMgr.ResetLogWithStart(newStart)
	test.Nil(t, err)
	realStart, _, err := logMgr.GetLogStartInfo()
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, newStart, *realStart)
	test.Equal(t, newStart.SegmentStartIndex, logMgr.GetCurrentStart())
	fileNum, offset := logMgr.GetCurrentEnd()
	test.Equal(t, newStart.SegmentStartIndex, fileNum)
	test.Equal(t, int64(0), offset)
	logs, err := logMgr.GetCommitLogsV2(newStart.SegmentStartIndex, 0, 1)
	test.NotNil(t, err)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, 0, len(logs))
	_, err = logMgr.GetCommitLogFromOffsetV2(newStart.SegmentStartIndex, 0)
	test.NotNil(t, err)
	test.Equal(t, ErrCommitLogEOF, err)

	fileNum, offset, _, err = logMgr.GetLastCommitLogOffsetV2()
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, newStart.SegmentStartIndex, fileNum)
	test.Equal(t, int64(0), offset)

	cntIndex, err := logMgr.ConvertToCountIndex(fileNum, offset)
	test.Nil(t, err)
	test.Equal(t, realStart.SegmentStartCount, cntIndex)
	convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, fileNum, convertedIndex)
	test.Equal(t, offset, convertedOffset)

	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
	}
	currentStart, lastOffset, _, err = logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	test.Equal(t, newStart.SegmentStartIndex+int64(num/LOGROTATE_NUM)-1, logMgr.GetCurrentStart())
	cntIndex, err = logMgr.ConvertToCountIndex(currentStart, lastOffset)
	test.Nil(t, err)
	test.Equal(t, newStart.SegmentStartCount+int64(num-1), cntIndex)

	logMgr.FlushCommitLogs()
	// reset with old start
	err = logMgr.ResetLogWithStart(newStart)
	test.Nil(t, err)

	realStart, _, err = logMgr.GetLogStartInfo()
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, newStart, *realStart)
	test.Equal(t, newStart.SegmentStartIndex, logMgr.GetCurrentStart())
	fileNum, offset = logMgr.GetCurrentEnd()
	test.Equal(t, newStart.SegmentStartIndex, fileNum)
	test.Equal(t, int64(0), offset)
	logs, err = logMgr.GetCommitLogsV2(newStart.SegmentStartIndex, 0, 1)
	test.NotNil(t, err)
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, 0, len(logs))
	_, err = logMgr.GetCommitLogFromOffsetV2(newStart.SegmentStartIndex, 0)
	test.NotNil(t, err)
	test.Equal(t, ErrCommitLogEOF, err)

	fileNum, offset, _, err = logMgr.GetLastCommitLogOffsetV2()
	test.Equal(t, ErrCommitLogEOF, err)
	test.Equal(t, newStart.SegmentStartIndex, fileNum)
	test.Equal(t, int64(0), offset)
	cntIndex, err = logMgr.ConvertToCountIndex(fileNum, offset)
	test.Nil(t, err)
	test.Equal(t, realStart.SegmentStartCount, cntIndex)
	convertedIndex, convertedOffset, err = logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, fileNum, convertedIndex)
	test.Equal(t, offset, convertedOffset)

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
	}
	currentStart, lastOffset, _, err = logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	test.Equal(t, newStart.SegmentStartIndex+int64(num/LOGROTATE_NUM)-1, logMgr.GetCurrentStart())
	cntIndex, _ = logMgr.ConvertToCountIndex(currentStart, lastOffset)
	test.Equal(t, newStart.SegmentStartCount+int64(num-1), cntIndex)

	cntIndex, err = logMgr.ConvertToCountIndex(currentStart, lastOffset+int64(GetLogDataSize()))
	test.Nil(t, err)
	convertedIndex, convertedOffset, err = logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, currentStart, convertedIndex)
	test.Equal(t, lastOffset+int64(GetLogDataSize()), convertedOffset)
}

func TestCommitLogReopen(t *testing.T) {
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

	num := 100
	msgRawSize := 10
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
		test.Equal(t, logMgr.IsCommitted(logData.LogID), true)
		//t.Logf("index:%v, %v, %v", i, logMgr.GetCurrentStart(), logMgr.currentCount)
		test.Equal(t, logMgr.GetCurrentStart(), int64(i/LOGROTATE_NUM))
		test.Equal(t, int64(logMgr.currentCount), int64(i)-int64(i/LOGROTATE_NUM)*int64(LOGROTATE_NUM)+1)
		logIndex, logOffset, lastLogData, err := logMgr.GetLastCommitLogOffsetV2()
		test.Equal(t, nil, err)
		test.Equal(t, int64(i/LOGROTATE_NUM), logIndex)
		test.Equal(t, int64(logMgr.currentCount-1)*int64(GetLogDataSize()), logOffset)
		test.Equal(t, int64(i+1), lastLogData.MsgCnt)
	}
	logMgr.Close()
	err = logMgr.Reopen()
	test.Nil(t, err)
	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())

	cntIndex, err := logMgr.ConvertToCountIndex(currentStart, lastOffset+int64(GetLogDataSize()))
	test.Nil(t, err)
	test.Equal(t, int64(num), cntIndex)
	convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
	test.Nil(t, err)
	test.Equal(t, currentStart, convertedIndex)
	test.Equal(t, lastOffset+int64(GetLogDataSize()), convertedOffset)

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
	test.Equal(t, true, logMgr.nLogID >= oldNLogID)
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
		test.Equal(t, l.MsgCnt, logMgr.currentStart*int64(LOGROTATE_NUM)+int64(cnt+1))
	}
	for i := int64(0); i < logMgr.currentStart; i++ {
		for cnt := 0; cnt < LOGROTATE_NUM; cnt++ {
			l, err := logMgr.GetCommitLogFromOffsetV2(i, int64(cnt*GetLogDataSize()))
			test.Nil(t, err)
			test.Equal(t, i*int64(LOGROTATE_NUM)+int64(cnt+1), l.MsgCnt)

			cntIndex, err := logMgr.ConvertToCountIndex(i, int64(cnt*GetLogDataSize()))
			test.Nil(t, err)
			test.Equal(t, i*int64(LOGROTATE_NUM)+int64(cnt), cntIndex)
			convertedIndex, convertedOffset, err := logMgr.ConvertToOffsetIndex(cntIndex)
			test.Nil(t, err)
			test.Equal(t, i, convertedIndex)
			test.Equal(t, int64(cnt*GetLogDataSize()), convertedOffset)

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

	logMgr.Close()
	logMgr.Delete()
	err = logMgr.Reopen()
	test.Nil(t, err)
	test.Equal(t, logMgr.pLogID, int64(0))
	test.Equal(t, int64(0), logMgr.currentStart)
	test.Equal(t, int32(0), logMgr.currentCount)
	test.Equal(t, int64(0), logMgr.logStartInfo.SegmentStartIndex)
	test.Equal(t, int64(0), logMgr.logStartInfo.SegmentStartOffset)
	test.Equal(t, int64(0), logMgr.logStartInfo.SegmentStartCount)
	test.Equal(t, logMgr.nLogID > logMgr.pLogID, true)
	test.Equal(t, logMgr.GetLastCommitLogID(), int64(0))
	cntIndex, err = logMgr.ConvertToCountIndex(0, 0)
	test.Nil(t, err)
	test.Equal(t, int64(0), cntIndex)
	logMgr2, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	test.Equal(t, logMgr.nLogID, logMgr2.nLogID)
	test.Equal(t, logMgr.pLogID, logMgr2.pLogID)
	test.Equal(t, logMgr.currentStart, logMgr2.currentStart)
	test.Equal(t, logMgr.currentCount, logMgr2.currentCount)
	test.Equal(t, logMgr.logStartInfo, logMgr2.logStartInfo)
	test.Equal(t, logMgr.GetLastCommitLogID(), logMgr2.GetLastCommitLogID())
}

func TestCommitLogMoveToAndDelete(t *testing.T) {
	oldRotate := LOGROTATE_NUM
	LOGROTATE_NUM = 10
	defer func() {
		LOGROTATE_NUM = oldRotate
	}()
	logName := "test_log_moveto" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	coordLog.Logger = newTestLogger(t)
	coordLog.SetLevel(4)
	logMgr, err := InitTopicCommitLogMgr(logName, 0, tmpDir, 4)

	num := 100
	msgRawSize := 10
	for i := 0; i < num; i++ {
		var logData CommitLogData
		logData.LogID = int64(logMgr.NextID())
		logData.LastMsgLogID = logData.LogID
		logData.Epoch = 1
		logData.MsgOffset = int64(i * msgRawSize)
		logData.MsgCnt = int64(i + 1)
		logData.MsgNum = 1
		err = logMgr.AppendCommitLog(&logData, false)
		test.Nil(t, err)
	}
	currentStart, lastOffset, _, err := logMgr.GetLastCommitLogOffsetV2()
	test.Nil(t, err)
	test.Equal(t, int64((LOGROTATE_NUM-1)*GetLogDataSize()), lastOffset)
	test.Equal(t, currentStart, logMgr.GetCurrentStart())
	logMgr.Close()
	logMgr, _ = InitTopicCommitLogMgr(logName, 0, tmpDir, 4)
	_, err = os.Stat(logMgr.path + ".current")
	test.Nil(t, err)
	_, err = os.Stat(logMgr.path + ".start")
	test.Nil(t, err)

	newMoveToPath := GetTopicPartitionBasePath(tmpDir, logName+"-removed", 0)
	newPath := GetTopicPartitionLogPath(newMoveToPath, logMgr.topic, logMgr.partition)

	os.MkdirAll(newMoveToPath, 755)
	err = logMgr.MoveTo(newMoveToPath)
	test.Nil(t, err)
	defer os.Remove(newMoveToPath)

	t.Log(newMoveToPath)
	_, err = os.Stat(logMgr.path)
	test.NotNil(t, err)
	_, err = os.Stat(logMgr.path + ".current")
	test.NotNil(t, err)
	_, err = os.Stat(logMgr.path + ".start")
	test.NotNil(t, err)
	_, err = os.Stat(newPath)
	test.Nil(t, err)
	_, err = os.Stat(newPath + ".current")
	test.Nil(t, err)
	_, err = os.Stat(newPath + ".start")
	test.Nil(t, err)
	logMgr.Delete()
	_, err = os.Stat(newPath)
	test.Nil(t, err)
	_, err = os.Stat(newPath + ".current")
	test.Nil(t, err)
	_, err = os.Stat(newPath + ".start")
	test.Nil(t, err)

}
