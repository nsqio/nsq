package nsqd

import (
	"fmt"
	"github.com/absolute8511/nsq/internal/test"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestDiskQueueReaderResetConfirmed(t *testing.T) {

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	test.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	queue, _ := NewDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	test.NotNil(t, dqWriter)

	msg := []byte("test")
	msgNum := 1000
	for i := 0; i < msgNum; i++ {
		dqWriter.Put(msg)
	}
	dqWriter.Flush()
	end := dqWriter.GetQueueWriteEnd()
	test.Nil(t, err)

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, nil, true)
	dqReader.UpdateQueueEnd(end, false)
	msgOut, _ := dqReader.TryReadOne()
	equal(t, msgOut.Data, msg)
	test.Equal(t, msgOut.Offset+BackendOffset(msgOut.MovedSize), dqReader.(*diskQueueReader).readQueueInfo.Offset())
	test.Equal(t, msgOut.CurCnt, dqReader.(*diskQueueReader).readQueueInfo.TotalMsgCnt())
	defer dqReader.Close()
	oldConfirm := dqReader.GetQueueConfirmed()

	curConfirmed, err := dqReader.ResetReadToConfirmed()
	test.Nil(t, err)
	test.Equal(t, oldConfirm, curConfirmed)
	test.Equal(t, oldConfirm.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)
	msgOut2, _ := dqReader.TryReadOne()
	test.Equal(t, msgOut, msgOut2)
	err = dqReader.ConfirmRead(msgOut.Offset+BackendOffset(msgOut.MovedSize), msgOut.CurCnt)
	test.Nil(t, err)

	oldConfirm = dqReader.GetQueueConfirmed()
	test.Equal(t, msgOut.Offset+BackendOffset(msgOut.MovedSize), oldConfirm.Offset())
	curConfirmed, err = dqReader.ResetReadToConfirmed()
	test.Nil(t, err)
	test.Equal(t, oldConfirm, curConfirmed)
	test.Equal(t, oldConfirm.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)

	var confirmMsg ReadResult
	for i := 0; i < msgNum/2; i++ {
		msgOut, _ = dqReader.TryReadOne()
		equal(t, msgOut.Data, msg)
		if i == msgNum/4 {
			confirmMsg = msgOut
		}
	}
	curRead := dqReader.(*diskQueueReader).readQueueInfo
	test.Equal(t, true, curRead.Offset() > dqReader.GetQueueConfirmed().Offset())
	err = dqReader.ConfirmRead(confirmMsg.Offset+BackendOffset(confirmMsg.MovedSize), confirmMsg.CurCnt)
	test.Nil(t, err)
	test.Equal(t, true, curRead.Offset() > dqReader.GetQueueConfirmed().Offset())

	curConfirmed, err = dqReader.ResetReadToConfirmed()
	test.Nil(t, err)
	test.Equal(t, curConfirmed, dqReader.GetQueueConfirmed())
	test.Equal(t, curConfirmed.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)
	msgOut2, _ = dqReader.TryReadOne()
	test.Equal(t, confirmMsg.CurCnt+1, msgOut2.CurCnt)
	test.Equal(t, confirmMsg.Offset+confirmMsg.MovedSize, msgOut2.Offset)
}

func TestDiskQueueReaderResetRead(t *testing.T) {
	// backward, forward

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	test.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	queue, _ := NewDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	test.NotNil(t, dqWriter)

	msg := []byte("test")
	msgNum := 4000
	for i := 0; i < msgNum; i++ {
		dqWriter.Put(msg)
	}
	dqWriter.Flush()
	end := dqWriter.GetQueueWriteEnd()
	test.Nil(t, err)

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, nil, true)
	dqReader.UpdateQueueEnd(end, false)
	firstReadMsg, _ := dqReader.TryReadOne()
	equal(t, firstReadMsg.Data, msg)
	test.Equal(t, firstReadMsg.Offset+BackendOffset(firstReadMsg.MovedSize), dqReader.(*diskQueueReader).readQueueInfo.Offset())
	test.Equal(t, firstReadMsg.CurCnt, dqReader.(*diskQueueReader).readQueueInfo.TotalMsgCnt())
	firstFilePos := dqReader.(*diskQueueReader).readQueueInfo.EndOffset
	defer dqReader.Close()
	// reset to current read position
	oldReadInfo := dqReader.(*diskQueueReader).readQueueInfo
	newReaderInfo, err := dqReader.(*diskQueueReader).ResetReadToOffset(oldReadInfo.Offset(), oldReadInfo.TotalMsgCnt())
	test.Nil(t, err)
	test.Equal(t, &oldReadInfo, newReaderInfo.(*diskQueueEndInfo))
	confirmed := dqReader.GetQueueConfirmed()
	test.Equal(t, newReaderInfo.(*diskQueueEndInfo), confirmed.(*diskQueueEndInfo))

	// should fail on exceed the end
	_, err = dqReader.(*diskQueueReader).ResetReadToOffset(end.Offset()+1, end.TotalMsgCnt()+1)
	test.NotNil(t, err)

	// reset to current confirmed position
	msgOut, _ := dqReader.TryReadOne()
	test.Equal(t, newReaderInfo.Offset(), msgOut.Offset)
	test.Equal(t, newReaderInfo.TotalMsgCnt()+1, msgOut.CurCnt)
	dqReader.TryReadOne()
	newReaderInfo, err = dqReader.(*diskQueueReader).ResetReadToOffset(confirmed.Offset(), confirmed.TotalMsgCnt())
	test.Nil(t, err)
	test.Equal(t, confirmed.(*diskQueueEndInfo), newReaderInfo.(*diskQueueEndInfo))
	test.Equal(t, confirmed.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)
	msgOut, hasData := dqReader.TryReadOne()
	test.Equal(t, true, hasData)
	test.Equal(t, confirmed.Offset(), msgOut.Offset)
	test.Equal(t, confirmed.TotalMsgCnt()+1, msgOut.CurCnt)

	// reset to end
	newReaderInfo2, err := dqReader.(*diskQueueReader).ResetReadToOffset(end.Offset(), end.TotalMsgCnt())
	test.Nil(t, err)
	test.Equal(t, newReaderInfo2, dqReader.GetQueueReadEnd())
	test.Equal(t, newReaderInfo2, dqReader.GetQueueConfirmed())

	// reset backward
	newReaderInfo, err = dqReader.(*diskQueueReader).ResetReadToOffset(newReaderInfo.Offset(), newReaderInfo.TotalMsgCnt())
	test.Nil(t, err)
	test.Equal(t, newReaderInfo, dqReader.GetQueueConfirmed())

	var resetReadMsg ReadResult
	for i := 0; i < msgNum/2; i++ {
		msgOut, _ := dqReader.TryReadOne()
		equal(t, msgOut.Data, msg)
		if i == msgNum/4 {
			resetReadMsg = msgOut
		}
	}
	newReaderInfo, err = dqReader.(*diskQueueReader).ResetReadToOffset(resetReadMsg.Offset+resetReadMsg.MovedSize, resetReadMsg.CurCnt)
	test.Nil(t, err)
	test.Equal(t, newReaderInfo, dqReader.GetQueueConfirmed())
	test.Equal(t, newReaderInfo.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)
	msgOut, hasData = dqReader.TryReadOne()
	test.Equal(t, true, hasData)
	test.Nil(t, msgOut.Err)
	test.Equal(t, resetReadMsg.Offset+resetReadMsg.MovedSize, msgOut.Offset)
	test.Equal(t, resetReadMsg.CurCnt+1, msgOut.CurCnt)

	// backward reset
	newReaderInfo, err = dqReader.(*diskQueueReader).ResetReadToOffset(firstReadMsg.Offset+firstReadMsg.MovedSize, firstReadMsg.CurCnt)
	test.Nil(t, err)
	test.Equal(t, firstFilePos, newReaderInfo.(*diskQueueEndInfo).EndOffset)
	test.Equal(t, newReaderInfo, dqReader.GetQueueConfirmed())
	msgOut, hasData = dqReader.TryReadOne()
	test.Equal(t, true, hasData)
	test.Nil(t, msgOut.Err)
	test.Equal(t, firstReadMsg.Offset+firstReadMsg.MovedSize, msgOut.Offset)
	test.Equal(t, firstReadMsg.CurCnt+1, msgOut.CurCnt)
}

func TestDiskQueueReaderSkip(t *testing.T) {
	// skip offset, skip next, skip end
	// skip to read, skip to confirmed

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	test.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	queue, _ := NewDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	test.NotNil(t, dqWriter)

	msg := []byte("test")
	msgNum := 2000
	for i := 0; i < msgNum; i++ {
		dqWriter.Put(msg)
	}
	dqWriter.Flush()
	end := dqWriter.GetQueueWriteEnd()
	test.Nil(t, err)

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, nil, true)
	dqReader.UpdateQueueEnd(end, false)
	firstReadMsg, _ := dqReader.TryReadOne()
	equal(t, firstReadMsg.Data, msg)
	test.Equal(t, firstReadMsg.Offset+BackendOffset(firstReadMsg.MovedSize), dqReader.(*diskQueueReader).readQueueInfo.Offset())
	test.Equal(t, firstReadMsg.CurCnt, dqReader.(*diskQueueReader).readQueueInfo.TotalMsgCnt())
	defer dqReader.Close()
	// skip to current read position
	oldReadInfo := dqReader.(*diskQueueReader).readQueueInfo
	newReaderInfo, err := dqReader.SkipReadToOffset(oldReadInfo.Offset(), oldReadInfo.TotalMsgCnt())
	test.Nil(t, err)
	test.Equal(t, &oldReadInfo, newReaderInfo.(*diskQueueEndInfo))
	confirmed := dqReader.GetQueueConfirmed()
	test.Equal(t, newReaderInfo.(*diskQueueEndInfo), confirmed.(*diskQueueEndInfo))

	// should fail on exceed the end
	_, err = dqReader.(*diskQueueReader).SkipReadToOffset(end.Offset()+1, end.TotalMsgCnt()+1)
	test.NotNil(t, err)

	// skip to current confirmed position
	msgOut, _ := dqReader.TryReadOne()
	test.Equal(t, newReaderInfo.Offset(), msgOut.Offset)
	test.Equal(t, newReaderInfo.TotalMsgCnt()+1, msgOut.CurCnt)
	dqReader.TryReadOne()
	newReaderInfo, err = dqReader.SkipReadToOffset(confirmed.Offset(), confirmed.TotalMsgCnt())
	test.Nil(t, err)
	test.Equal(t, confirmed.(*diskQueueEndInfo), newReaderInfo.(*diskQueueEndInfo))
	test.Equal(t, confirmed.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)
	msgOut, hasData := dqReader.TryReadOne()
	test.Equal(t, true, hasData)
	test.Equal(t, confirmed.Offset(), msgOut.Offset)
	test.Equal(t, confirmed.TotalMsgCnt()+1, msgOut.CurCnt)

	curFileNum := dqReader.(*diskQueueReader).confirmedQueueInfo.EndOffset.FileNum
	// skip to next file
	newReaderInfo2, err := dqReader.(*diskQueueReader).SkipToNext()
	test.Nil(t, err)
	test.Equal(t, newReaderInfo2.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)
	test.Equal(t, newReaderInfo2, dqReader.GetQueueConfirmed())
	test.Equal(t, newReaderInfo2.(*diskQueueEndInfo).EndOffset.FileNum, curFileNum+1)

	// skip backward should fail
	newReaderInfo, err = dqReader.SkipReadToOffset(newReaderInfo.Offset(), newReaderInfo.TotalMsgCnt())
	test.NotNil(t, err)

	var resetReadMsg ReadResult
	for i := 0; i < msgNum/2; i++ {
		msgOut, _ := dqReader.TryReadOne()
		equal(t, msgOut.Data, msg)
		if i == msgNum/4 {
			resetReadMsg = msgOut
		}
	}
	newReaderInfo, err = dqReader.SkipReadToOffset(resetReadMsg.Offset+resetReadMsg.MovedSize, resetReadMsg.CurCnt)
	test.Nil(t, err)
	test.Equal(t, newReaderInfo, dqReader.GetQueueConfirmed())
	test.Equal(t, newReaderInfo.(*diskQueueEndInfo), &dqReader.(*diskQueueReader).readQueueInfo)
	msgOut, hasData = dqReader.TryReadOne()
	test.Equal(t, true, hasData)
	test.Nil(t, msgOut.Err)
	test.Equal(t, resetReadMsg.Offset+resetReadMsg.MovedSize, msgOut.Offset)
	test.Equal(t, resetReadMsg.CurCnt+1, msgOut.CurCnt)
	curEnd := dqReader.GetQueueReadEnd()
	newReaderInfo, err = dqReader.SkipReadToEnd()
	test.Nil(t, err)
	test.Equal(t, newReaderInfo, curEnd)
	test.Equal(t, &dqReader.(*diskQueueReader).readQueueInfo, curEnd.(*diskQueueEndInfo))
}

func TestDiskQueueReaderUpdateEnd(t *testing.T) {
	// init empty with end
	// init old reader with end
	// reader position should read from meta file

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	test.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	queue, _ := NewDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	test.NotNil(t, dqWriter)

	msg := []byte("test")
	msgNum := 2000
	var midEnd BackendQueueEnd
	for i := 0; i < msgNum; i++ {
		dqWriter.Put(msg)
		if i == msgNum/2 {
			midEnd = dqWriter.GetQueueWriteEnd()
		}
	}
	dqWriter.Flush()
	end := dqWriter.GetQueueWriteEnd()
	test.Nil(t, err)

	dqReaderWithEnd := newDiskQueueReader(dqName, dqName+"-meta1", tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, end, true)
	test.Equal(t, dqReaderWithEnd.GetQueueReadEnd(), end)
	test.Equal(t, dqReaderWithEnd.GetQueueConfirmed(), end)
	_, hasData := dqReaderWithEnd.TryReadOne()
	equal(t, hasData, false)
	dqReaderWithEnd.Close()
	dqReaderWithEnd = newDiskQueueReader(dqName, dqName+"-meta1", tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, midEnd, true)
	test.Equal(t, dqReaderWithEnd.GetQueueReadEnd(), end)
	test.Equal(t, dqReaderWithEnd.GetQueueConfirmed(), end)
	_, hasData = dqReaderWithEnd.TryReadOne()
	equal(t, hasData, false)

	dqReaderWithEnd.Close()
	dqReaderWithEnd = newDiskQueueReader(dqName, dqName+"-meta2", tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, midEnd, true)
	test.Equal(t, dqReaderWithEnd.GetQueueReadEnd(), midEnd)
	test.Equal(t, dqReaderWithEnd.GetQueueConfirmed(), midEnd)
	_, hasData = dqReaderWithEnd.TryReadOne()
	equal(t, hasData, false)

	dqReaderWithEnd.Close()
	dqReaderWithEnd = newDiskQueueReader(dqName, dqName+"-meta2", tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, end, true)
	test.Equal(t, dqReaderWithEnd.GetQueueReadEnd(), midEnd)
	test.Equal(t, dqReaderWithEnd.GetQueueConfirmed(), midEnd)
	_, hasData = dqReaderWithEnd.TryReadOne()
	equal(t, hasData, false)
	dqReaderWithEnd.UpdateQueueEnd(end, false)
	dqReaderWithEnd.Close()
	dqReaderWithEnd = newDiskQueueReader(dqName, dqName+"-meta2", tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, end, true)
	test.Equal(t, dqReaderWithEnd.GetQueueReadEnd(), end)
	test.Equal(t, dqReaderWithEnd.GetQueueConfirmed(), midEnd)
	_, hasData = dqReaderWithEnd.TryReadOne()
	equal(t, hasData, true)

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, nil, true)
	dqReader.UpdateQueueEnd(end, false)
	defer dqReader.Close()
	test.Equal(t, dqReader.GetQueueReadEnd(), end)
	test.Equal(t, dqReader.GetQueueConfirmed().Offset(), BackendOffset(0))
	_, hasData = dqReader.TryReadOne()
	equal(t, hasData, true)
}

func TestDiskQueueSnapshotReader(t *testing.T) {
	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	test.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	queue, _ := NewDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	test.NotNil(t, dqWriter)

	msg := []byte("test")
	msgNum := 2000
	var midEnd BackendQueueEnd
	var midEnd2 BackendQueueEnd
	for i := 0; i < msgNum; i++ {
		dqWriter.Put(msg)
		if i == msgNum/2 {
			midEnd = dqWriter.GetQueueWriteEnd()
		}
		if i == msgNum/4 {
			midEnd2 = dqWriter.GetQueueWriteEnd()
		}
	}
	dqWriter.Flush()
	end := dqWriter.GetQueueWriteEnd()
	test.Nil(t, err)

	dqReader := NewDiskQueueSnapshot(dqName, tmpDir, end)
	defer dqReader.Close()

	queueStart := dqReader.queueStart
	test.Equal(t, BackendOffset(0), queueStart.Offset())
	dqReader.SetQueueStart(midEnd2)
	test.Equal(t, midEnd2.Offset(), dqReader.readPos.Offset())
	result := dqReader.ReadOne()
	test.Nil(t, result.Err)
	test.Equal(t, midEnd2.Offset(), result.Offset)
	err = dqReader.SeekTo(midEnd.Offset())
	test.Nil(t, err)
	test.Equal(t, midEnd.Offset(), dqReader.readPos.virtualEnd)
	result = dqReader.ReadOne()
	test.Nil(t, result.Err)
	test.Equal(t, midEnd.Offset(), result.Offset)
	data, err := dqReader.ReadRaw(100)
	test.Nil(t, err)
	test.Equal(t, 100, len(data))
	// remove some begin of queue, and test queue start
}
