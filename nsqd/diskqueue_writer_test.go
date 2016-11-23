package nsqd

import (
	"bytes"
	"fmt"
	_ "github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/test"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"strings"
)

func TestDiskQueueWriter(t *testing.T) {
	l := newTestLogger(t)
	nsqLog.Logger = l

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	queue, _ := newDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	nequal(t, dqWriter, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(0))

	msg := []byte("test")
	dqWriter.Put(msg)
	dqWriter.Flush()
	end := dqWriter.GetQueueWriteEnd()
	equal(t, err, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(1))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, int64(0))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, dqWriter.diskWriteEnd.EndOffset.FileNum)
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, int64(len(msg)+4))
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, dqWriter.diskWriteEnd.EndOffset.Pos)

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, nil, true)
	dqReader.UpdateQueueEnd(end, false)
	msgOut, _ := dqReader.TryReadOne()
	equal(t, msgOut.Data, msg)
	dqReader.Close()
}

func TestDiskQueueWriterCleanOffsetmeta(t *testing.T) {
	l := newTestLogger(t)
	nsqLog.Logger = l

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	queue, _ := newDiskQueueWriter(dqName, tmpDir, 3, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	nequal(t, dqWriter, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(0))

	msg := []byte("test")
	for i :=0; i<200; i++{
		dqWriter.Put(msg)
	}
	dqWriter.Flush()
	end := dqWriter.GetQueueWriteEnd()
	fmt.Printf("queueWriteEnd: %d\n", end.Offset())
	fmt.Printf("queueStart: %d\n", dqWriter.diskQueueStart.virtualEnd)
	equal(t, err, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(200))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, int64(200))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, dqWriter.diskWriteEnd.EndOffset.FileNum)
	dqWriter.CleanOldDataByRetention(end, false, 0)
	files,err := ioutil.ReadDir(tmpDir)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		fileName := file.Name()
		if strings.Contains(fileName, ".offsetmate.dat") {
			num, _ := strconv.Atoi(strings.Split(fileName, ".")[2])
			assert(t, num >= 100, "Offset metadata which not larger than 100 is not clean.")
		}
	}
}

func TestDiskQueueWriterRoll(t *testing.T) {
	l := newTestLogger(t)
	nsqLog.Logger = l
	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	ml := int64(len(msg))
	dq, _ := newDiskQueueWriter(dqName, tmpDir, 9*(ml+4), int32(ml), 1<<10, 1)
	dqObj := dq.(*diskQueueWriter)
	defer dq.Close()
	nequal(t, dq, nil)
	nequal(t, dqObj, nil)
	equal(t, dq.(*diskQueueWriter).diskWriteEnd.TotalMsgCnt(), int64(0))

	for i := 0; i < 10; i++ {
		_, _, _, err := dq.Put(msg)
		equal(t, err, nil)
		equal(t, dqObj.diskWriteEnd.TotalMsgCnt(), int64(i+1))
	}
	dq.Flush()

	equal(t, dqObj.diskWriteEnd.EndOffset.FileNum, int64(1))
	equal(t, dqObj.diskWriteEnd.EndOffset.Pos, int64(ml+4))
	equal(t, int64(dqObj.diskWriteEnd.Offset()), 10*(ml+4))
}

func TestDiskQueueWriterRollbackAndResetWrite(t *testing.T) {
	l := newTestLogger(t)
	nsqLog.Logger = l
	nsqLog.SetLevel(3)
	dqName := "test_disk_queue_" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 226)
	ml := int64(len(msg))
	dq, _ := newDiskQueueWriter(dqName, tmpDir, 1024*1024*100, 0, 1024, 1)
	dqObj := dq.(*diskQueueWriter)
	defer dq.Close()
	nequal(t, dq, nil)
	nequal(t, dqObj, nil)
	equal(t, dq.(*diskQueueWriter).diskWriteEnd.TotalMsgCnt(), int64(0))

	for i := 0; i < 920000; i++ {
		_, _, _, err = dq.Put(msg)
		equal(t, err, nil)
	}
	dq.Flush()

	equal(t, dqObj.diskWriteEnd.EndOffset.FileNum, int64(2))
	f1, err := os.Stat(dqObj.fileName(0))
	equal(t, f1.Size(), (ml+4)*455903)
	f2, err := os.Stat(dqObj.fileName(1))
	equal(t, f2.Size(), (ml+4)*455903)
	f3, err := os.Stat(dqObj.fileName(2))
	equal(t, err, nil)
	equal(t, int64(dqObj.diskWriteEnd.Offset()), f1.Size()+f2.Size()+f3.Size())

	dq.RollbackWrite(dqObj.diskWriteEnd.Offset()-BackendOffset(ml+4), 1)
	_, _, _, err = dq.Put(msg)
	dq.Flush()
	equal(t, err, nil)
	f3, err = os.Stat(dqObj.fileName(2))
	equal(t, int64(dqObj.diskWriteEnd.Offset()), f1.Size()+f2.Size()+f3.Size())
	dq.ResetWriteEnd(BackendOffset(f1.Size()), 455903)
	equal(t, int64(dqObj.diskWriteEnd.Offset()), f1.Size())
	equal(t, int64(dqObj.diskWriteEnd.EndOffset.FileNum), int64(1))
	equal(t, int64(dqObj.diskWriteEnd.EndOffset.Pos), int64(0))
	for i := 455903; i < 920000; i++ {
		_, _, _, err = dq.Put(msg)
		equal(t, err, nil)
	}
	dq.Flush()
	equal(t, dqObj.diskWriteEnd.EndOffset.FileNum, int64(2))
	f1, err = os.Stat(dqObj.fileName(0))
	equal(t, f1.Size(), (ml+4)*455903)
	f2, err = os.Stat(dqObj.fileName(1))
	equal(t, f2.Size(), (ml+4)*455903)
	f3, err = os.Stat(dqObj.fileName(2))
	equal(t, err, nil)
	equal(t, int64(dqObj.diskWriteEnd.Offset()), f1.Size()+f2.Size()+f3.Size())
}

func TestDiskQueueWriterEmpty(t *testing.T) {
	l := newTestLogger(t)
	nsqLog.Logger = l
	nsqLog.SetLevel(4)
	dqName := "test_disk_queue_empty" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	msgRawSize := 4 + len(msg)
	maxBytesPerFile := 100
	dq, _ := newDiskQueueWriter(dqName, tmpDir, int64(maxBytesPerFile), 0, 1<<10, 1)
	dqReader := newDiskQueueReader(dqName, dqName, tmpDir,
		int64(maxBytesPerFile), 0, 1<<10, 1, 2*time.Second, nil, true)
	dqObj := dq.(*diskQueueWriter)
	defer dq.Close()

	nequal(t, dq, nil)
	nequal(t, dqObj, nil)

	t.Logf("test begin ... 000\n")
	for i := 0; i < 100; i++ {
		_, _, _, err = dq.Put(msg)
		equal(t, err, nil)
		equal(t, dqObj.diskWriteEnd.TotalMsgCnt(), int64(i+1))
	}
	dq.Flush()
	end2 := dq.GetQueueReadEnd().(*diskQueueEndInfo)
	equal(t, int64(end2.Offset()), int64(100*msgRawSize))
	equal(t, int64(end2.EndOffset.FileNum), int64(100/(maxBytesPerFile/msgRawSize+1)))

	equal(t, int64(end2.EndOffset.Pos), int64(msgRawSize)*(100-
		end2.EndOffset.FileNum*int64(maxBytesPerFile/msgRawSize+1)))

	dqReader.UpdateQueueEnd(dq.GetQueueReadEnd(), false)

	var rr ReadResult
	for i := 0; i < 3; i++ {
		rr, _ = dqReader.TryReadOne()
	}

	dqReader.ConfirmRead(BackendOffset(rr.Offset+rr.MovedSize), rr.CurCnt)
	time.Sleep(time.Second)
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(3*msgRawSize))
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.EndOffset.Pos,
		int64(3*msgRawSize))
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.EndOffset.FileNum, int64(0))

	t.Logf("test begin here...0\n")
	for {
		if dqReader.Depth() == int64(97) {
			equal(t, dqReader.DepthSize(), int64(97*msgRawSize))
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	equal(t, dqReader.Depth(), int64(97))

	_, err = dqReader.(*diskQueueReader).SkipReadToEnd()
	equal(t, err, nil)
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset,
		dqReader.(*diskQueueReader).queueEndInfo.EndOffset)
	equal(t, dqReader.(*diskQueueReader).queueEndInfo.Offset(),
		dqReader.(*diskQueueReader).readQueueInfo.Offset())

	dqReader.ConfirmRead(BackendOffset(-1), 0)
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.Offset(), dqReader.(*diskQueueReader).confirmedQueueInfo.Offset())
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset, dqReader.(*diskQueueReader).confirmedQueueInfo.EndOffset)
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset,
		dqReader.(*diskQueueReader).queueEndInfo.EndOffset)
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(100*msgRawSize))

	numFiles := dqObj.diskWriteEnd.EndOffset.FileNum
	dq.Empty()
	end := dq.GetQueueReadEnd().(*diskQueueEndInfo)
	dqReader.UpdateQueueEnd(end, false)
	dqReader.(*diskQueueReader).SkipReadToEnd()
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset,
		dqReader.(*diskQueueReader).queueEndInfo.EndOffset)
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo, dqReader.(*diskQueueReader).queueEndInfo)
	dqReader.Close()

	for i := int64(0); i <= numFiles; i++ {
		assertFileNotExist(t, dqObj.fileName(i))
	}
	equal(t, dqObj.diskWriteEnd.TotalMsgCnt(), int64(100))

	dqReader = newDiskQueueReader(dqName, dqName, tmpDir, int64(maxBytesPerFile), 0, 1<<10, 1, 2*time.Second, nil, true)

	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(100*msgRawSize))
	end = dq.GetQueueReadEnd().(*diskQueueEndInfo)
	equal(t, int64(end.Offset()), int64(100*msgRawSize))
	dqReader.UpdateQueueEnd(end, true)
	for i := 0; i < 100; i++ {
		_, _, _, err := dq.Put(msg)
		equal(t, err, nil)
		equal(t, dq.(*diskQueueWriter).diskWriteEnd.TotalMsgCnt(), int64(i+101))
	}
	dq.Flush()
	end = dq.GetQueueReadEnd().(*diskQueueEndInfo)
	time.Sleep(time.Second)
	equal(t, int64(end.Offset()), int64(200*msgRawSize))
	dqReader.UpdateQueueEnd(end, false)
	t.Logf("queue confirmed :%v\n", dqReader.(*diskQueueReader).confirmedQueueInfo)
	equal(t,
		dqReader.(*diskQueueReader).queueEndInfo.Offset(), end.Offset())
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(100*msgRawSize))
	equal(t,
		dqReader.(*diskQueueReader).queueEndInfo.EndOffset.GreatThan(&dqReader.(*diskQueueReader).readQueueInfo.EndOffset),
		true)

	for i := 0; i < 100; i++ {
		dqReader.TryReadOne()
	}
	dqReader.ConfirmRead(BackendOffset(-1), 0)
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.Offset(), dqReader.(*diskQueueReader).confirmedQueueInfo.Offset())
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset, dqReader.(*diskQueueReader).confirmedQueueInfo.EndOffset)
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset,
		dqReader.(*diskQueueReader).queueEndInfo.EndOffset)
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(200*msgRawSize))

	for {
		if dqReader.Depth() == int64(0) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	equal(t, dqReader.Depth(), int64(0))
	equal(t, dqReader.DepthSize(), int64(0))
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset.FileNum, dqObj.diskWriteEnd.EndOffset.FileNum)
	equal(t, dqReader.(*diskQueueReader).readQueueInfo.EndOffset.Pos, dqObj.diskWriteEnd.EndOffset.Pos)
	dqReader.Close()
}

func TestDiskQueueWriterCorruption(t *testing.T) {
	l := newTestLogger(t)
	nsqLog.Logger = l
	dqName := "test_disk_queue_corruption" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	// require a non-zero message length for the corrupt (len 0) test below
	dq, _ := newDiskQueueWriter(dqName, tmpDir, 1000, 10, 1<<10, 1)
	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1000, 10, 1<<10, 5, 2*time.Second, nil, true)
	defer dqReader.Close()
	defer dq.Close()

	msg := make([]byte, 123) // 127 bytes per message, 8 (1016 bytes) messages per file
	var e BackendQueueEnd
	for i := 0; i < 25; i++ {
		dq.Put(msg)
	}
	dq.Flush()
	e = dq.GetQueueReadEnd()
	dqReader.UpdateQueueEnd(e, false)

	equal(t, dq.(*diskQueueWriter).diskWriteEnd.TotalMsgCnt(), int64(25))

	// corrupt the 2nd file
	dqFn := dq.(*diskQueueWriter).fileName(1)
	os.Truncate(dqFn, 500) // 3 valid messages, 5 corrupted

	for i := 0; i < 19; i++ { // 1 message leftover in 4th file
		m, _ := dqReader.TryReadOne()
		equal(t, m.Data, msg)
		equal(t, m.Err, nil)
	}

	// corrupt the 4th (current) file
	dqFn = dq.(*diskQueueWriter).fileName(3)
	os.Truncate(dqFn, 100)

	dq.Put(msg) // in 5th file
	dq.Flush()
	e = dq.GetQueueReadEnd()
	dqReader.UpdateQueueEnd(e, true)
	readResult, _ := dqReader.TryReadOne()
	equal(t, readResult.Data, msg)

	// write a corrupt (len 0) message at the 5th (current) file
	dq.(*diskQueueWriter).writeFile.Write([]byte{0, 0, 0, 0})

	// force a new 6th file - put into 5th, then readOne errors, then put into 6th
	dq.Put(msg)
	dq.Put(msg)
	dq.Flush()
	e = dq.GetQueueReadEnd()
	dqReader.UpdateQueueEnd(e, true)
	readResult, _ = dqReader.TryReadOne()

	equal(t, readResult.Data, msg)
}

func TestDiskQueueWriterRollbackAndResetEnd(t *testing.T) {
	//rollback and reset write end across file test
	l := newTestLogger(t)
	nsqLog.Logger = l

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	queue, _ := newDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	dqWriter := queue.(*diskQueueWriter)
	defer dqWriter.Close()
	nequal(t, dqWriter, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(0))

	msg := []byte("test")
	totalCnt := 1000
	for cnt := 0; cnt < totalCnt; cnt++ {
		dqWriter.Put(msg)
		dqWriter.Flush()
		end := dqWriter.GetQueueWriteEnd()
		equal(t, err, nil)
		equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(cnt+1))
		equal(t, end.(*diskQueueEndInfo).Offset(), BackendOffset((cnt+1)*(len(msg)+4)))
		equal(t, end.(*diskQueueEndInfo).TotalMsgCnt(), int64(cnt+1))

		equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, int64(end.Offset()/1024))
		equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, dqWriter.diskWriteEnd.EndOffset.FileNum)
		leftPos := int64(end.Offset()) - 1024*end.(*diskQueueEndInfo).EndOffset.FileNum
		equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, leftPos)
		equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, dqWriter.diskWriteEnd.EndOffset.Pos)
	}

	oldEnd := dqWriter.GetQueueWriteEnd()
	err = dqWriter.RollbackWrite(oldEnd.Offset()-BackendOffset(len(msg)+4), 1)
	equal(t, err, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(totalCnt-1))
	end := dqWriter.GetQueueWriteEnd()
	equal(t, end.Offset(), oldEnd.Offset()-BackendOffset(len(msg)+4))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, oldEnd.(*diskQueueEndInfo).EndOffset.FileNum)
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, dqWriter.diskWriteEnd.EndOffset.FileNum)
	leftPos := int64(end.Offset()) - 1024*dqWriter.diskWriteEnd.EndOffset.FileNum
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, leftPos)
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, dqWriter.diskWriteEnd.EndOffset.Pos)

	resetOffset := int64((len(msg) + 4) * totalCnt / 2)
	err = dqWriter.ResetWriteEnd(BackendOffset(resetOffset), int64(totalCnt/2))
	equal(t, err, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(totalCnt/2))
	end = dqWriter.GetQueueWriteEnd()
	equal(t, end.Offset(), BackendOffset(resetOffset))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, int64(resetOffset/1024))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, dqWriter.diskWriteEnd.EndOffset.FileNum)
	leftPos = resetOffset - int64(dqWriter.diskWriteEnd.EndOffset.FileNum*1024)
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, leftPos)
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, dqWriter.diskWriteEnd.EndOffset.Pos)
	err = dqWriter.ResetWriteEnd(0, 0)
	end = dqWriter.GetQueueWriteEnd()
	equal(t, end.Offset(), BackendOffset(0))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, int64(0))
	equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, dqWriter.diskWriteEnd.EndOffset.FileNum)
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, int64(0))
	equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, dqWriter.diskWriteEnd.EndOffset.Pos)
	for cnt := 0; cnt < totalCnt; cnt++ {
		dqWriter.Put(msg)
		dqWriter.Flush()
		end := dqWriter.GetQueueWriteEnd()
		equal(t, err, nil)
		equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(cnt+1))
		equal(t, end.(*diskQueueEndInfo).Offset(), BackendOffset((cnt+1)*(len(msg)+4)))
		equal(t, end.(*diskQueueEndInfo).TotalMsgCnt(), int64(cnt+1))

		equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, int64(end.Offset()/1024))
		equal(t, end.(*diskQueueEndInfo).EndOffset.FileNum, dqWriter.diskWriteEnd.EndOffset.FileNum)
		leftPos := int64(end.Offset()) - 1024*end.(*diskQueueEndInfo).EndOffset.FileNum
		equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, leftPos)
		equal(t, end.(*diskQueueEndInfo).EndOffset.Pos, dqWriter.diskWriteEnd.EndOffset.Pos)
	}

}

func TestDiskQueueWriterRollbackToQueueStart(t *testing.T) {
	//TODO:
}

func TestDiskQueueWriterInitWithQueueStart(t *testing.T) {
	l := newTestLogger(t)
	nsqLog.Logger = l

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	fileMaxSize := 1024
	queue, err := newDiskQueueWriter(dqName, tmpDir, int64(fileMaxSize), 4, 1<<10, 1)
	test.Nil(t, err)
	dqWriter := queue.(*diskQueueWriter)
	nequal(t, dqWriter, nil)
	equal(t, dqWriter.diskWriteEnd.TotalMsgCnt(), int64(0))
	// test init start with empty

	msg := []byte("test")
	cntInFile := fileMaxSize / (len(msg) + 4)
	totalCnt := 1000
	for cnt := 0; cnt < totalCnt; cnt++ {
		dqWriter.Put(msg)
	}
	dqWriter.Flush()
	oldStart := dqWriter.diskQueueStart
	test.Equal(t, int64(0), oldStart.EndOffset.FileNum)
	dqWriter.Close()

	queue, err = newDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	test.Nil(t, err)
	dqWriter = queue.(*diskQueueWriter)
	test.Equal(t, oldStart, dqWriter.diskQueueStart)

	oldStart.EndOffset.FileNum++
	newStart, err := dqWriter.CleanOldDataByRetention(&oldStart, false, 0)
	test.Equal(t, nil, err)
	test.Equal(t, oldStart.EndOffset, newStart.(*diskQueueEndInfo).EndOffset)
	test.Equal(t, oldStart.EndOffset.FileNum*int64(fileMaxSize), int64(newStart.Offset()))
	test.Equal(t, oldStart.EndOffset.FileNum*int64(cntInFile), newStart.TotalMsgCnt())
	dqWriter.Close()

	queue, err = newDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	test.Nil(t, err)
	dqWriter = queue.(*diskQueueWriter)
	test.Equal(t, newStart, dqWriter.GetQueueReadStart())
	dqWriter.cleanOldData()
	test.Equal(t, dqWriter.GetQueueReadStart(), dqWriter.GetQueueWriteEnd())
	newStart = dqWriter.GetQueueReadStart()
	dqWriter.Close()
	queue, err = newDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	test.Nil(t, err)
	dqWriter = queue.(*diskQueueWriter)
	test.Equal(t, newStart, dqWriter.GetQueueReadStart())

	newStart.(*diskQueueEndInfo).virtualEnd += BackendOffset(len(msg) + 4)
	newStart.(*diskQueueEndInfo).totalMsgCnt++
	dqWriter.ResetWriteWithQueueStart(newStart)
	test.Equal(t, dqWriter.GetQueueReadStart(), dqWriter.GetQueueWriteEnd())
	test.Equal(t, newStart.Offset(), dqWriter.GetQueueReadStart().Offset())
	test.Equal(t, newStart.TotalMsgCnt(), dqWriter.GetQueueReadStart().TotalMsgCnt())
	newStart = dqWriter.GetQueueReadStart()
	dqWriter.Close()
	queue, err = newDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1)
	test.Nil(t, err)
	dqWriter = queue.(*diskQueueWriter)

	test.Equal(t, newStart, dqWriter.GetQueueReadStart())
	dqWriter.Close()
}

func TestDiskQueueWriterTorture(t *testing.T) {
	var wg sync.WaitGroup

	l := newTestLogger(t)
	nsqLog.Logger = l
	dqName := "test_disk_queue_torture" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq, _ := newDiskQueueWriter(dqName, tmpDir, 262144, 0, 1<<10, 1)
	nequal(t, dq, nil)

	msg := []byte("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
	msgRawSize := 4 + len(msg)

	numWriters := 4
	numReaders := 4
	readExitChan := make(chan int)
	writeExitChan := make(chan int)

	var incDepth int64
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case <-writeExitChan:
					return
				default:
					_, _, _, err := dq.Put(msg)
					if err == nil {
						atomic.AddInt64(&incDepth, int64(msgRawSize))
					} else {
						t.Logf("put error %v", err)
					}
				}
			}
		}()
	}

	time.Sleep(1 * time.Second)
	var e BackendQueueEnd

	t.Logf("closing writeExitChan")
	close(writeExitChan)
	wg.Wait()
	depth := atomic.LoadInt64(&incDepth)

	dq.Flush()
	e = dq.GetQueueReadEnd()
	t.Logf("diskqueue end : %v", e)
	t.Logf("restarting diskqueue")
	dq.Close()

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 262144, 0, 1<<10, 1, 2*time.Second, nil, true)
	defer dqReader.Close()
	dqReader.UpdateQueueEnd(e, false)
	time.Sleep(time.Second * 1)
	equal(t, dqReader.Depth(), depth/int64(msgRawSize))
	equal(t, dqReader.DepthSize(), depth)
	equal(t, dqReader.(*diskQueueReader).queueEndInfo.Offset(), BackendOffset(depth))
	equal(t, dqReader.(*diskQueueReader).confirmedQueueInfo.Offset(),
		BackendOffset(0))

	var read int64
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(time.Microsecond)
				m, hasData := dqReader.TryReadOne()
				if hasData {
					equal(t, msg, m.Data)
					equal(t, nil, m.Err)
					newr := atomic.AddInt64(&read, int64(4+len(m.Data)))
					if newr >= depth {
						dqReader.ConfirmRead(BackendOffset(newr), m.CurCnt)
					}
				}
				select {
				case <-readExitChan:
					return
				default:
				}
			}
		}()
	}

	t.Logf("waiting for depth 0")
	for {
		if atomic.LoadInt64(&read) == depth {
			break
		}
		time.Sleep(time.Second)
	}
	equal(t, atomic.LoadInt64(&read), depth)
	equal(t, dqReader.(*diskQueueReader).GetQueueConfirmed().Offset(),
		BackendOffset(depth))
	equal(t, dqReader.Depth(), int64(0))
	equal(t, dqReader.DepthSize(), int64(0))

	t.Logf("closing readExitChan")
	close(readExitChan)
	wg.Wait()

	equal(t, atomic.LoadInt64(&read), depth)
}

func BenchmarkDiskQueueWriterPut16(b *testing.B) {
	benchmarkDiskQueueWriterPut(16, 2500, b)
}
func BenchmarkDiskQueueWriterPut64(b *testing.B) {
	benchmarkDiskQueueWriterPut(64, 2500, b)
}
func BenchmarkDiskQueueWriterPut256(b *testing.B) {
	benchmarkDiskQueueWriterPut(256, 2500, b)
}
func BenchmarkDiskQueueWriterPut1024(b *testing.B) {
	benchmarkDiskQueueWriterPut(1024, 2500, b)
}
func BenchmarkDiskQueueWriterPut4096(b *testing.B) {
	benchmarkDiskQueueWriterPut(4096, 2500, b)
}
func BenchmarkDiskQueueWriterPut16384(b *testing.B) {
	benchmarkDiskQueueWriterPut(16384, 2500, b)
}
func BenchmarkDiskQueueWriterPut65536(b *testing.B) {
	benchmarkDiskQueueWriterPut(65536, 2500, b)
}
func BenchmarkDiskQueueWriterPut262144(b *testing.B) {
	benchmarkDiskQueueWriterPut(262144, 2500, b)
}
func BenchmarkDiskQueueWriterPut1048576(b *testing.B) {
	benchmarkDiskQueueWriterPut(1048576, 2500, b)
}

func BenchmarkDiskQueueWriterPut16Sync(b *testing.B) {
	benchmarkDiskQueueWriterPut(16, 1, b)
}
func BenchmarkDiskQueueWriterPut64Sync(b *testing.B) {
	benchmarkDiskQueueWriterPut(64, 1, b)
}
func BenchmarkDiskQueueWriterPut256Sync(b *testing.B) {
	benchmarkDiskQueueWriterPut(256, 1, b)
}
func BenchmarkDiskQueueWriterPut1024Sync(b *testing.B) {
	benchmarkDiskQueueWriterPut(1024, 1, b)
}
func BenchmarkDiskQueueWriterPut4096Sync(b *testing.B) {
	benchmarkDiskQueueWriterPut(4096, 1, b)
}

func benchmarkDiskQueueWriterPut(size int64, syncEvery int64, b *testing.B) {
	b.StopTimer()
	l := newTestLogger(b)
	nsqLog.Logger = l
	nsqLog.SetLevel(0)
	dqName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq, _ := newDiskQueueWriter(dqName, tmpDir, 1024768*100, 0, 1<<20, syncEvery)
	defer dq.Close()
	b.SetBytes(size)
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _, _, err := dq.Put(data)
		if err != nil {
			panic(err)
		}
	}
	b.StopTimer()
}

// you might want to run this like
// $ go test -bench=DiskQueueReaderGet -benchtime 0.1s
// too avoid doing too many iterations.
func BenchmarkDiskQueueReaderGet16(b *testing.B) {
	benchmarkDiskQueueReaderGet(16, b)
}
func BenchmarkDiskQueueReaderGet64(b *testing.B) {
	benchmarkDiskQueueReaderGet(64, b)
}
func BenchmarkDiskQueueReaderGet256(b *testing.B) {
	benchmarkDiskQueueReaderGet(256, b)
}
func BenchmarkDiskQueueReaderGet1024(b *testing.B) {
	benchmarkDiskQueueReaderGet(1024, b)
}
func BenchmarkDiskQueueReaderGet4096(b *testing.B) {
	benchmarkDiskQueueReaderGet(4096, b)
}
func BenchmarkDiskQueueReaderGet16384(b *testing.B) {
	benchmarkDiskQueueReaderGet(16384, b)
}
func BenchmarkDiskQueueReaderGet65536(b *testing.B) {
	benchmarkDiskQueueReaderGet(65536, b)
}
func BenchmarkDiskQueueReaderGet262144(b *testing.B) {
	benchmarkDiskQueueReaderGet(262144, b)
}
func BenchmarkDiskQueueReaderGet1048576(b *testing.B) {
	benchmarkDiskQueueReaderGet(1048576, b)
}

func benchmarkDiskQueueReaderGet(size int64, b *testing.B) {
	b.StopTimer()
	l := newTestLogger(b)
	nsqLog.Logger = l
	nsqLog.SetLevel(0)
	dqName := "bench_disk_queue_get" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq, _ := newDiskQueueWriter(dqName, tmpDir, 1024768, 0, 1<<20, 2500)
	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024768, 0, 1<<20,
		2500, 2*time.Second, nil, true)
	defer dqReader.Close()
	defer dq.Close()
	b.SetBytes(size)
	data := make([]byte, size)
	var e BackendQueueEnd
	for i := 0; i < b.N; i++ {
		dq.Put(data)
	}
	dq.Flush()
	e = dq.GetQueueReadEnd()
	b.StartTimer()
	dqReader.UpdateQueueEnd(e, false)

	for i := 0; i < b.N; i++ {
		dqReader.TryReadOne()
	}
}
