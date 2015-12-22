package nsqd

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiskQueueWriter(t *testing.T) {
	l := newTestLogger(t)

	dqName := "test_disk_queue" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dqWriter := newDiskQueueWriter(dqName, tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, l).(*diskQueueWriter)
	defer dqWriter.Close()
	nequal(t, dqWriter, nil)
	equal(t, dqWriter.totalMsgCnt, int64(0))

	msg := []byte("test")
	end, err := dqWriter.Put(msg)
	equal(t, err, nil)
	equal(t, dqWriter.totalMsgCnt, int64(1))
	equal(t, end.(*diskQueueEndInfo).EndFileNum, int64(0))
	equal(t, end.(*diskQueueEndInfo).EndFileNum, dqWriter.writeFileNum)
	equal(t, end.(*diskQueueEndInfo).EndPos, int64(len(msg)+4))
	equal(t, end.(*diskQueueEndInfo).EndPos, dqWriter.writePos)

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024, 4, 1<<10, 1, 2*time.Second, true, l)
	dqReader.UpdateQueueEnd(end)
	msgOut := <-dqReader.ReadChan()
	equal(t, msgOut.data, msg)
	dqReader.Close()
}

func TestDiskQueueWriterRoll(t *testing.T) {
	l := newTestLogger(t)
	dqName := "test_disk_queue_roll" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	ml := int64(len(msg))
	dq := newDiskQueueWriter(dqName, tmpDir, 9*(ml+4), int32(ml), 1<<10, 1, 2*time.Second, l)
	dqObj := dq.(*diskQueueWriter)
	defer dq.Close()
	nequal(t, dq, nil)
	nequal(t, dqObj, nil)
	equal(t, dq.(*diskQueueWriter).totalMsgCnt, int64(0))

	for i := 0; i < 10; i++ {
		_, err := dq.Put(msg)
		equal(t, err, nil)
		equal(t, dqObj.totalMsgCnt, int64(i+1))
	}

	equal(t, dqObj.writeFileNum, int64(1))
	equal(t, dqObj.writePos, int64(0))
}

func TestDiskQueueWriterEmpty(t *testing.T) {
	l := newTestLogger(t)
	dqName := "test_disk_queue_empty" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	msg := bytes.Repeat([]byte{0}, 10)
	msgRawSize := 4 + len(msg)
	maxBytesPerFile := 100
	dq := newDiskQueueWriter(dqName, tmpDir, int64(maxBytesPerFile), 0, 1<<10, 1, 2*time.Second, l)
	dqReader := newDiskQueueReader(dqName, dqName, tmpDir,
		int64(maxBytesPerFile), 0, 1<<10, 1, 2*time.Second, true, l)
	dqObj := dq.(*diskQueueWriter)
	defer dq.Close()

	nequal(t, dq, nil)
	nequal(t, dqObj, nil)

	t.Logf("test begin ... 000\n")
	var end BackendQueueEnd
	for i := 0; i < 100; i++ {
		end, err = dq.Put(msg)
		equal(t, err, nil)
		equal(t, dqObj.totalMsgCnt, int64(i+1))
	}
	dq.Flush()
	end2 := dq.GetQueueReadEnd().(*diskQueueEndInfo)
	equal(t, end2, end.(*diskQueueEndInfo))
	equal(t, int64(end2.VirtualEnd), int64(100*msgRawSize))
	equal(t, int64(end2.EndFileNum), int64(100/(maxBytesPerFile/msgRawSize+1)))

	equal(t, int64(end2.EndPos), int64(msgRawSize)*(100-
		end2.EndFileNum*int64(maxBytesPerFile/msgRawSize+1)))

	dqReader.UpdateQueueEnd(dq.GetQueueReadEnd())

	for i := 0; i < 3; i++ {
		<-dqReader.ReadChan()
	}

	dqReader.ConfirmRead(BackendOffset(-1))
	time.Sleep(time.Second)
	equal(t, dqReader.(*diskQueueReader).virtualReadOffset, dqReader.(*diskQueueReader).virtualConfirmedOffset)
	equal(t, dqReader.(*diskQueueReader).readPos, dqReader.(*diskQueueReader).confirmedOffset)
	equal(t, dqReader.(*diskQueueReader).virtualConfirmedOffset,
		BackendOffset(3*msgRawSize))
	equal(t, dqReader.(*diskQueueReader).confirmedOffset.Pos,
		int64(3*msgRawSize))
	equal(t, dqReader.(*diskQueueReader).confirmedOffset.FileNum, int64(0))

	t.Logf("test begin here...0\n")
	for {
		if dqReader.Depth() == int64(97*msgRawSize) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	equal(t, dqReader.Depth(), int64(97*msgRawSize))

	err = dqReader.(*diskQueueReader).SkipToEnd()
	equal(t, err, nil)
	equal(t, dqReader.(*diskQueueReader).readPos,
		dqReader.(*diskQueueReader).endPos)
	equal(t, dqReader.(*diskQueueReader).virtualEnd,
		dqReader.(*diskQueueReader).virtualReadOffset)

	dqReader.ConfirmRead(BackendOffset(-1))
	equal(t, dqReader.(*diskQueueReader).virtualReadOffset, dqReader.(*diskQueueReader).virtualConfirmedOffset)
	equal(t, dqReader.(*diskQueueReader).readPos, dqReader.(*diskQueueReader).confirmedOffset)
	equal(t, dqReader.(*diskQueueReader).readPos,
		dqReader.(*diskQueueReader).endPos)
	equal(t, dqReader.(*diskQueueReader).virtualConfirmedOffset,
		BackendOffset(100*msgRawSize))

	numFiles := dqObj.writeFileNum
	dq.Empty()
	end = dq.GetQueueReadEnd().(*diskQueueEndInfo)
	dqReader.UpdateQueueEnd(end)
	dqReader.(*diskQueueReader).SkipToEnd()
	equal(t, dqReader.(*diskQueueReader).readPos,
		dqReader.(*diskQueueReader).endPos)
	dqReader.Close()

	for i := int64(0); i <= numFiles; i++ {
		assertFileNotExist(t, dqObj.fileName(i))
	}
	equal(t, dqObj.totalMsgCnt, int64(100))

	dqReader = newDiskQueueReader(dqName, dqName, tmpDir, int64(maxBytesPerFile), 0, 1<<10, 1, 2*time.Second, true, l)
	end = dq.GetQueueReadEnd().(*diskQueueEndInfo)
	dqReader.UpdateQueueEnd(end)
	for i := 0; i < 100; i++ {
		_, err := dq.Put(msg)
		equal(t, err, nil)
		equal(t, dq.(*diskQueueWriter).totalMsgCnt, int64(i+101))
	}
	dq.Flush()
	end = dq.GetQueueReadEnd().(*diskQueueEndInfo)
	dqReader.UpdateQueueEnd(end)
	time.Sleep(time.Second)
	equal(t, int64(end.(*diskQueueEndInfo).VirtualEnd), int64(200*msgRawSize))
	equal(t,
		dqReader.(*diskQueueReader).virtualEnd, end.(*diskQueueEndInfo).VirtualEnd)
	equal(t, dqReader.(*diskQueueReader).virtualConfirmedOffset,
		BackendOffset(100*msgRawSize))
	equal(t, dqReader.(*diskQueueReader).virtualReadOffset, dqReader.(*diskQueueReader).virtualConfirmedOffset)
	equal(t,
		dqReader.(*diskQueueReader).endPos.GreatThan(&dqReader.(*diskQueueReader).readPos),
		true)

	for i := 0; i < 100; i++ {
		<-dqReader.ReadChan()
	}
	dqReader.ConfirmRead(BackendOffset(-1))
	equal(t, dqReader.(*diskQueueReader).virtualReadOffset, dqReader.(*diskQueueReader).virtualConfirmedOffset)
	equal(t, dqReader.(*diskQueueReader).readPos, dqReader.(*diskQueueReader).confirmedOffset)
	equal(t, dqReader.(*diskQueueReader).readPos,
		dqReader.(*diskQueueReader).endPos)
	equal(t, dqReader.(*diskQueueReader).virtualConfirmedOffset,
		BackendOffset(200*msgRawSize))

	for {
		if dqReader.Depth() == int64(0) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	equal(t, dqReader.Depth(), int64(0))
	equal(t, dqReader.(*diskQueueReader).readPos.FileNum, dqObj.writeFileNum)
	equal(t, dqReader.(*diskQueueReader).nextReadPos.Pos, dqObj.writePos)
	dqReader.Close()
}

func TestDiskQueueWriterCorruption(t *testing.T) {
	l := newTestLogger(t)
	dqName := "test_disk_queue_corruption" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	// require a non-zero message length for the corrupt (len 0) test below
	dq := newDiskQueueWriter(dqName, tmpDir, 1000, 10, 1<<10, 1, 2*time.Second, l)
	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1000, 10, 1<<10, 5, 2*time.Second, true, l)
	defer dqReader.Close()
	defer dq.Close()

	msg := make([]byte, 123) // 127 bytes per message, 8 (1016 bytes) messages per file
	var e BackendQueueEnd
	for i := 0; i < 25; i++ {
		e, _ = dq.Put(msg)
	}
	dq.Flush()
	dqReader.UpdateQueueEnd(e)

	equal(t, dq.(*diskQueueWriter).totalMsgCnt, int64(25))

	// corrupt the 2nd file
	dqFn := dq.(*diskQueueWriter).fileName(1)
	os.Truncate(dqFn, 500) // 3 valid messages, 5 corrupted

	for i := 0; i < 19; i++ { // 1 message leftover in 4th file
		m := <-dqReader.ReadChan()
		equal(t, m.data, msg)
		equal(t, m.err, nil)
	}

	// corrupt the 4th (current) file
	dqFn = dq.(*diskQueueWriter).fileName(3)
	os.Truncate(dqFn, 100)

	e, _ = dq.Put(msg) // in 5th file
	dq.Flush()
	dqReader.UpdateQueueEnd(e)
	readResult := <-dqReader.ReadChan()
	equal(t, readResult.data, msg)

	// write a corrupt (len 0) message at the 5th (current) file
	dq.(*diskQueueWriter).writeFile.Write([]byte{0, 0, 0, 0})

	// force a new 6th file - put into 5th, then readOne errors, then put into 6th
	dq.Put(msg)
	e, _ = dq.Put(msg)
	dq.Flush()
	dqReader.UpdateQueueEnd(e)
	readResult = <-dqReader.ReadChan()

	equal(t, readResult.data, msg)
}

func TestDiskQueueWriterHandleError(t *testing.T) {
	// TODO: handle error manually.
}

func TestDiskQueueWriterSkipTo(t *testing.T) {
	//TODO: skip and msg count check
}

func TestDiskQueueWriterTorture(t *testing.T) {
	var wg sync.WaitGroup

	l := newTestLogger(t)
	dqName := "test_disk_queue_torture" + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := newDiskQueueWriter(dqName, tmpDir, 262144, 0, 1<<10, 1, 2*time.Second, l)
	nequal(t, dq, nil)

	msg := []byte("aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeffffffffff")
	msgRawSize := 4 + len(msg)

	numWriters := 4
	numReaders := 4
	readExitChan := make(chan int)
	writeExitChan := make(chan int)

	var depth int64
	var e BackendQueueEnd
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
					tmpe, err := dq.Put(msg)
					if err == nil {
						atomic.AddInt64(&depth, int64(msgRawSize))
						e = tmpe
					} else {
						t.Logf("put error %v", err)
					}
				}
			}
		}()
	}

	time.Sleep(1 * time.Second)

	t.Logf("closing writeExitChan")
	close(writeExitChan)
	wg.Wait()

	dq.Flush()
	e = dq.GetQueueReadEnd()
	t.Logf("diskqueue end : %v", e)
	t.Logf("restarting diskqueue")
	dq.Close()

	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 262144, 0, 1<<10, 1, 2*time.Second, true, l)
	defer dqReader.Close()
	dqReader.UpdateQueueEnd(e)
	time.Sleep(time.Second * 1)
	equal(t, dqReader.Depth(), depth)
	equal(t, dqReader.(*diskQueueReader).virtualEnd, BackendOffset(depth))
	equal(t, dqReader.(*diskQueueReader).virtualConfirmedOffset,
		BackendOffset(0))

	var read int64
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				time.Sleep(100000 * time.Nanosecond)
				select {
				case m := <-dqReader.ReadChan():
					equal(t, msg, m.data)
					equal(t, nil, m.err)
					atomic.AddInt64(&read, int64(4+len(m.data)))
					dqReader.ConfirmRead(BackendOffset(atomic.LoadInt64(&read)))
				case <-readExitChan:
					return
				}
			}
		}()
	}

	t.Logf("waiting for depth 0")
	for {
		if dqReader.Depth() == int64(0) {
			break
		}
		//dqReader.ConfirmRead(BackendOffset(-1))
		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("closing readExitChan")
	close(readExitChan)
	wg.Wait()

	equal(t, read, depth)
}

func BenchmarkDiskQueueWriterPut16(b *testing.B) {
	benchmarkDiskQueueWriterPut(16, b)
}
func BenchmarkDiskQueueWriterPut64(b *testing.B) {
	benchmarkDiskQueueWriterPut(64, b)
}
func BenchmarkDiskQueueWriterPut256(b *testing.B) {
	benchmarkDiskQueueWriterPut(256, b)
}
func BenchmarkDiskQueueWriterPut1024(b *testing.B) {
	benchmarkDiskQueueWriterPut(1024, b)
}
func BenchmarkDiskQueueWriterPut4096(b *testing.B) {
	benchmarkDiskQueueWriterPut(4096, b)
}
func BenchmarkDiskQueueWriterPut16384(b *testing.B) {
	benchmarkDiskQueueWriterPut(16384, b)
}
func BenchmarkDiskQueueWriterPut65536(b *testing.B) {
	benchmarkDiskQueueWriterPut(65536, b)
}
func BenchmarkDiskQueueWriterPut262144(b *testing.B) {
	benchmarkDiskQueueWriterPut(262144, b)
}
func BenchmarkDiskQueueWriterPut1048576(b *testing.B) {
	benchmarkDiskQueueWriterPut(1048576, b)
}
func benchmarkDiskQueueWriterPut(size int64, b *testing.B) {
	b.StopTimer()
	l := newTestLogger(b)
	dqName := "bench_disk_queue_put" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := newDiskQueueWriter(dqName, tmpDir, 1024768*100, 0, 1<<20, 1, 2*time.Second, l)
	defer dq.Close()
	b.SetBytes(size)
	data := make([]byte, size)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := dq.Put(data)
		if err != nil {
			panic(err)
		}
	}
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
	dqName := "bench_disk_queue_get" + strconv.Itoa(b.N) + strconv.Itoa(int(time.Now().Unix()))
	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("nsq-test-%d", time.Now().UnixNano()))
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	dq := newDiskQueueWriter(dqName, tmpDir, 1024768, 0, 1<<10, 1, 2*time.Second, l)
	dqReader := newDiskQueueReader(dqName, dqName, tmpDir, 1024768, 0, 1<<10, 1, 2*time.Second, true, l)
	defer dqReader.Close()
	defer dq.Close()
	b.SetBytes(size)
	data := make([]byte, size)
	var e BackendQueueEnd
	for i := 0; i < b.N; i++ {
		e, _ = dq.Put(data)
	}
	b.StartTimer()
	dqReader.UpdateQueueEnd(e)

	for i := 0; i < b.N; i++ {
		<-dqReader.ReadChan()
	}
}
