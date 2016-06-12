package nsqd

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAX_POSSIBLE_MSG_SIZE = 1 << 28
)

var (
	ErrConfirmSizeInvalid = errors.New("Confirm data size invalid.")
	ErrMoveOffsetInvalid  = errors.New("move offset invalid")
	ErrOffsetTypeMismatch = errors.New("offset type mismatch")
)

type diskQueueOffset struct {
	FileNum int64
	Pos     int64
}

type diskQueueEndInfo struct {
	EndOffset   diskQueueOffset
	VirtualEnd  BackendOffset
	TotalMsgCnt int64
}

func (d *diskQueueEndInfo) GetOffset() BackendOffset {
	return d.VirtualEnd
}

func (d *diskQueueEndInfo) GetTotalMsgCnt() int64 {
	return d.TotalMsgCnt
}

func (d *diskQueueEndInfo) IsSame(other BackendQueueEnd) bool {
	if otherDiskEnd, ok := other.(*diskQueueEndInfo); ok {
		return *d == *otherDiskEnd
	}
	return false
}

func (d *diskQueueOffset) GreatThan(o *diskQueueOffset) bool {
	if d.FileNum > o.FileNum {
		return true
	}
	if d.FileNum == o.FileNum && d.Pos > o.Pos {
		return true
	}
	return false
}

// diskQueueReader implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueueReader struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	readPos           diskQueueOffset
	virtualReadOffset BackendOffset
	queueEndInfo      diskQueueEndInfo
	// left message number for read
	depth int64
	// total need to read

	sync.RWMutex

	// instantiation time metadata
	readerMetaName  string
	readFrom        string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	maxMsgSize      int32
	syncEvery       int64 // number of writes per fsync
	exitFlag        int32
	needSync        bool

	confirmedOffset        diskQueueOffset
	virtualConfirmedOffset BackendOffset

	readFile *os.File
	reader   *bufio.Reader

	exitChan        chan int
	autoSkipError   bool
	waitingMoreData int32
}

// newDiskQueue instantiates a new instance of diskQueueReader, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueReader(readFrom string, metaname string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, autoSkip bool) BackendQueueReader {

	d := diskQueueReader{
		readFrom:        readFrom,
		readerMetaName:  metaname,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		minMsgSize:      minMsgSize,
		maxMsgSize:      maxMsgSize,
		exitChan:        make(chan int),
		syncEvery:       syncEvery,
		autoSkipError:   autoSkip,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		nsqLog.LogErrorf("diskqueue(%s) failed to retrieveMetaData %v - %s",
			d.readFrom, d.readerMetaName, err)
	}

	return &d
}

func (d *diskQueueReader) getCurrentFileEnd(offset diskQueueOffset) (int64, error) {
	curFileName := d.fileName(offset.FileNum)
	f, err := os.Stat(curFileName)
	if err != nil {
		return 0, err
	}
	return f.Size(), nil
}

// Depth returns the depth of the queue
func (d *diskQueueReader) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

func (d *diskQueueReader) GetQueueReadEnd() BackendQueueEnd {
	d.RLock()
	e := d.queueEndInfo
	d.RUnlock()
	return &e
}

// Put writes a []byte to the queue
func (d *diskQueueReader) UpdateQueueEnd(e BackendQueueEnd) (bool, error) {
	end, ok := e.(*diskQueueEndInfo)
	if !ok || end == nil {
		if nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.Logf("%v got nil end while update queue end", d.readerMetaName)
		}
		return false, nil
	}
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return false, errors.New("exiting")
	}
	return d.internalUpdateEnd(end)
}

func (d *diskQueueReader) Delete() error {
	return d.exit(true)
}

// Close cleans up the queue and persists metadata
func (d *diskQueueReader) Close() error {
	return d.exit(false)
}

func (d *diskQueueReader) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1
	close(d.exitChan)
	nsqLog.Logf("diskqueue(%s) exiting ", d.readerMetaName)
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.sync()
	if deleted {
		d.skipToEndofFile()
		err := os.Remove(d.metaDataFileName())

		if err != nil && !os.IsNotExist(err) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove metadata file - %s", d.readerMetaName, err)
			return err
		}
	}
	return nil
}

func (d *diskQueueReader) ConfirmRead(offset BackendOffset) error {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	oldConfirm := d.virtualConfirmedOffset
	err := d.internalConfirm(offset)
	if oldConfirm != d.virtualConfirmedOffset {
		d.needSync = true
		if d.syncEvery == 1 {
			d.sync()
		}
	}
	return err
}

func (d *diskQueueReader) Flush() {
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return
	}
	d.internalUpdateEnd(nil)
}

func (d *diskQueueReader) ResetReadToConfirmed() (BackendOffset, error) {
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return 0, errors.New("exiting")
	}
	old := d.virtualConfirmedOffset
	skiperr := d.internalSkipTo(d.virtualConfirmedOffset)
	if skiperr == nil {
		if old != d.virtualConfirmedOffset {
			d.needSync = true
			if d.syncEvery == 1 {
				d.sync()
			}
		}
	}

	return d.virtualConfirmedOffset, skiperr
}

func (d *diskQueueReader) SkipReadToOffset(offset BackendOffset) (BackendOffset, error) {
	d.Lock()
	defer d.Unlock()
	if d.exitFlag == 1 {
		return 0, errors.New("exiting")
	}
	old := d.virtualConfirmedOffset
	skiperr := d.internalSkipTo(offset)
	if skiperr == nil {
		if old != d.virtualConfirmedOffset {
			d.needSync = true
			if d.syncEvery == 1 {
				d.sync()
			}
		}
	}

	return d.virtualConfirmedOffset, skiperr
}

func (d *diskQueueReader) SkipToEnd() (BackendOffset, error) {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return 0, errors.New("exiting")
	}
	old := d.virtualConfirmedOffset
	skiperr := d.internalSkipTo(d.queueEndInfo.VirtualEnd)
	if skiperr == nil {
		if old != d.virtualConfirmedOffset {
			d.needSync = true
			if d.syncEvery == 1 {
				d.sync()
			}
		}
	}

	return d.virtualConfirmedOffset, skiperr
}

func (d *diskQueueReader) IsWaitingMoreData() bool {
	return atomic.LoadInt32(&d.waitingMoreData) == 1
}

func (d *diskQueueReader) TryReadOne() (ReadResult, bool) {
	d.Lock()
	defer d.Unlock()
	for {
		if (d.readPos.FileNum < d.queueEndInfo.EndOffset.FileNum) || (d.readPos.Pos < d.queueEndInfo.EndOffset.Pos) {
			dataRead := d.readOne()
			rerr := dataRead.Err
			if rerr != nil {
				nsqLog.LogErrorf("reading from diskqueue(%s) at %d of %s - %s",
					d.readerMetaName, d.readPos, d.fileName(d.readPos.FileNum), dataRead.Err)
				if d.autoSkipError {
					d.handleReadError()
					continue
				}
			}
			return dataRead, true
		} else {
			if nsqLog.Level() >= levellogger.LOG_DETAIL {
				nsqLog.LogDebugf("reading from diskqueue(%s) no more data: %v, %v, confirmed: %v",
					d.readerMetaName, d.readPos, d.queueEndInfo, d.confirmedOffset)
			}
			return ReadResult{}, false
		}
	}
}

func (d *diskQueueReader) updateDepth() {
	newDepth := int64(0)
	if d.confirmedOffset.FileNum > d.queueEndInfo.EndOffset.FileNum {
		atomic.StoreInt64(&d.depth, 0)
	} else {
		newDepth = int64(d.queueEndInfo.VirtualEnd - d.virtualConfirmedOffset)
		atomic.StoreInt64(&d.depth, newDepth)
	}
	if newDepth == 0 {
		atomic.StoreInt32(&d.waitingMoreData, 1)
	}
}

func (d *diskQueueReader) getVirtualOffsetDistance(prev diskQueueOffset, next diskQueueOffset) (BackendOffset, error) {
	diff := int64(0)
	if prev.GreatThan(&next) {
		return BackendOffset(diff), ErrMoveOffsetInvalid
	}
	if prev.FileNum == next.FileNum {
		diff = next.Pos - prev.Pos
		return BackendOffset(diff), nil
	}

	fsize, err := d.getCurrentFileEnd(prev)
	if err != nil {
		return BackendOffset(diff), err
	}
	left := fsize - prev.Pos
	prev.FileNum++
	prev.Pos = 0
	vdiff := BackendOffset(0)
	vdiff, err = d.getVirtualOffsetDistance(prev, next)
	return BackendOffset(int64(vdiff) + left), err
}

func (d *diskQueueReader) SkipToNext() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	// TODO: skip to next file number.
	return nil
}

func (d *diskQueueReader) stepOffset(virtualCur BackendOffset, cur diskQueueOffset, step BackendOffset, maxVirtual BackendOffset, maxStep diskQueueOffset) (diskQueueOffset, error) {
	newOffset := cur
	var err error
	if cur.FileNum > maxStep.FileNum {
		return newOffset, fmt.Errorf("offset invalid: %v , %v", cur, maxStep)
	}
	if step == 0 {
		return newOffset, nil
	}
	if virtualCur+step < 0 {
		// backward exceed
		return newOffset, fmt.Errorf("move offset step %v from %v to exceed begin", step, virtualCur)
	} else if virtualCur+step > maxVirtual {
		// forward exceed
		return newOffset, fmt.Errorf("move offset step %v from %v exceed max: %v", step, virtualCur, maxVirtual)
	}
	if step < 0 {
		// handle backward
		step = 0 - step
		for step > BackendOffset(newOffset.Pos) {
			nsqLog.Logf("step read back to previous file: %v, %v", step, newOffset)
			virtualCur -= BackendOffset(newOffset.Pos)
			step -= BackendOffset(newOffset.Pos)
			newOffset.FileNum--
			if newOffset.FileNum < 0 {
				nsqLog.Logf("reset read acrossed the begin %v, %v", step, newOffset)
				return newOffset, ErrMoveOffsetInvalid
			}
			f, err := os.Stat(d.fileName(newOffset.FileNum))
			if err != nil {
				nsqLog.LogErrorf("stat data file error %v, %v", step, newOffset)
				return newOffset, err
			}
			newOffset.Pos = f.Size()
		}
		newOffset.Pos -= int64(step)
		return newOffset, nil
	}
	for {
		end := int64(0)
		if cur.FileNum < maxStep.FileNum {
			end, err = d.getCurrentFileEnd(newOffset)
			if err != nil {
				return newOffset, err
			}
		} else {
			end = maxStep.Pos
		}
		diff := end - newOffset.Pos
		if step > BackendOffset(diff) {
			newOffset.FileNum++
			newOffset.Pos = 0
			if newOffset.GreatThan(&maxStep) {
				return newOffset, fmt.Errorf("offset invalid: %v , %v, need step: %v",
					newOffset, maxStep, step)
			}
			step -= BackendOffset(diff)
			if step == 0 {
				return newOffset, nil
			}
		} else {
			newOffset.Pos += int64(step)
			if newOffset.GreatThan(&maxStep) {
				return newOffset, fmt.Errorf("offset invalid: %v , %v, need step: %v",
					newOffset, maxStep, step)
			}
			return newOffset, nil
		}
	}
}

func (d *diskQueueReader) internalConfirm(offset BackendOffset) error {
	if int64(offset) == -1 {
		d.confirmedOffset = d.readPos
		d.virtualConfirmedOffset = d.virtualReadOffset
		d.updateDepth()
		nsqLog.LogDebugf("confirmed to end: %v", d.virtualConfirmedOffset)
		return nil
	}
	if offset <= d.virtualConfirmedOffset {
		nsqLog.LogDebugf("already confirmed to : %v", d.virtualConfirmedOffset)
		return nil
	}
	if offset > d.virtualReadOffset {
		nsqLog.LogErrorf("confirm exceed read: %v, %v", offset, d.virtualReadOffset)
		return ErrConfirmSizeInvalid
	}
	diffVirtual := offset - d.virtualConfirmedOffset
	newConfirm, err := d.stepOffset(d.virtualConfirmedOffset, d.confirmedOffset, diffVirtual, d.virtualReadOffset, d.readPos)
	if err != nil {
		nsqLog.LogErrorf("confirmed exceed the read pos: %v, %v", offset, d.virtualReadOffset)
		return ErrConfirmSizeInvalid
	}
	d.confirmedOffset = newConfirm
	d.virtualConfirmedOffset = offset
	d.updateDepth()
	nsqLog.LogDebugf("confirmed to offset: %v", offset)
	return nil
}

func (d *diskQueueReader) internalSkipTo(voffset BackendOffset) error {
	if voffset == d.virtualReadOffset && voffset == d.virtualConfirmedOffset {
		return nil
	}

	if voffset != d.virtualReadOffset && d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	newPos := d.queueEndInfo.EndOffset
	var err error
	if voffset < d.virtualConfirmedOffset {
		nsqLog.Logf("skip backward to less than confirmed: %v, %v", voffset, d.virtualConfirmedOffset)
		return ErrMoveOffsetInvalid
	}
	if voffset > d.queueEndInfo.VirtualEnd {
		nsqLog.Logf("internal skip great than end : %v, skipping to : %v", d.queueEndInfo, voffset)
		return ErrMoveOffsetInvalid
	} else if voffset == d.queueEndInfo.VirtualEnd {
		newPos = d.queueEndInfo.EndOffset
	} else {
		newPos, err = d.stepOffset(d.virtualReadOffset, d.readPos, voffset-d.virtualReadOffset, d.queueEndInfo.VirtualEnd, d.queueEndInfo.EndOffset)
		if err != nil {
			nsqLog.LogErrorf("internal skip error : %v, skipping to : %v", err, voffset)
			return err
		}
	}

	if nsqLog.Level() >= levellogger.LOG_DEBUG {
		nsqLog.LogDebugf("==== read skip to : %v, %v", voffset, newPos)
	}
	d.readPos = newPos
	d.virtualReadOffset = voffset

	d.confirmedOffset = d.readPos
	d.virtualConfirmedOffset = d.virtualReadOffset
	d.updateDepth()
	return nil
}

func (d *diskQueueReader) skipToNextFile() error {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	if d.readPos.FileNum >= d.queueEndInfo.EndOffset.FileNum {
		return d.skipToEndofFile()
	}

	d.readPos.FileNum++
	d.readPos.Pos = 0
	if d.confirmedOffset != d.readPos {
		nsqLog.LogErrorf("skip confirm from %v to %v.", d.confirmedOffset, d.readPos)
	}
	d.confirmedOffset = d.readPos
	d.virtualConfirmedOffset = d.virtualReadOffset
	d.updateDepth()
	return nil
}

func (d *diskQueueReader) skipToEndofFile() error {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	d.readPos = d.queueEndInfo.EndOffset
	d.virtualReadOffset = d.queueEndInfo.VirtualEnd
	if d.confirmedOffset != d.readPos {
		nsqLog.LogErrorf("skip confirm from %v to %v.", d.confirmedOffset, d.readPos)
	}
	d.confirmedOffset = d.readPos
	d.virtualConfirmedOffset = d.virtualReadOffset
	d.updateDepth()

	return nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueueReader) readOne() ReadResult {
	var result ReadResult
	var msgSize int32
	var stat os.FileInfo
	result.Offset = BackendOffset(0)

CheckFileOpen:

	result.Offset = d.virtualReadOffset
	if d.readFile == nil {
		curFileName := d.fileName(d.readPos.FileNum)
		d.readFile, result.Err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if result.Err != nil {
			return result
		}

		if nsqLog.Level() >= levellogger.LOG_DEBUG {
			nsqLog.LogDebugf("DISKQUEUE(%s): readOne() opened %s", d.readerMetaName, curFileName)
		}

		if d.readPos.Pos > 0 {
			_, result.Err = d.readFile.Seek(d.readPos.Pos, 0)
			if result.Err != nil {
				d.readFile.Close()
				d.readFile = nil
				return result
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}
	if d.readPos.FileNum < d.queueEndInfo.EndOffset.FileNum {
		stat, result.Err = d.readFile.Stat()
		if result.Err != nil {
			return result
		}
		if d.readPos.Pos >= stat.Size() {
			d.readPos.FileNum++
			d.readPos.Pos = 0
			nsqLog.Logf("DISKQUEUE(%s): readOne() read end, try next: %v",
				d.readerMetaName, d.readPos.FileNum)
			d.readFile.Close()
			d.readFile = nil
			goto CheckFileOpen
		}
	}

	result.Err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if result.Err != nil {
		d.readFile.Close()
		d.readFile = nil
		return result
	}

	if msgSize <= 0 || msgSize > MAX_POSSIBLE_MSG_SIZE {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		result.Err = fmt.Errorf("invalid message read size (%d)", msgSize)
		return result
	}

	result.Data = make([]byte, msgSize)
	_, result.Err = io.ReadFull(d.reader, result.Data)
	if result.Err != nil {
		d.readFile.Close()
		d.readFile = nil
		return result
	}

	result.Offset = d.virtualReadOffset

	totalBytes := int64(4 + msgSize)
	result.MovedSize = BackendOffset(totalBytes)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	oldPos := d.readPos
	d.readPos.Pos = d.readPos.Pos + totalBytes
	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.LogDebugf("=== read move forward: %v, %v, %v", oldPos,
			d.virtualReadOffset, d.readPos)
	}

	d.virtualReadOffset += BackendOffset(totalBytes)

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	isEnd := false
	if d.readPos.FileNum < d.queueEndInfo.EndOffset.FileNum {
		stat, result.Err = d.readFile.Stat()
		if result.Err == nil {
			isEnd = d.readPos.Pos >= stat.Size()
		} else {
			return result
		}
	}
	if (d.readPos.Pos > d.maxBytesPerFile) && !isEnd {
		// this can happen if the maxbytesperfile configure is changed.
		nsqLog.Logf("should be end since next position is larger than maxfile size. %v", d.readPos)
	}
	if isEnd {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.readPos.FileNum++
		d.readPos.Pos = 0
	}
	return result
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueueReader) sync() error {
	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueueReader) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "%d\n%d,%d,%d\n%d,%d,%d\n",
		&d.queueEndInfo.TotalMsgCnt,
		&d.confirmedOffset.FileNum, &d.confirmedOffset.Pos, &d.virtualConfirmedOffset,
		&d.queueEndInfo.EndOffset.FileNum, &d.queueEndInfo.EndOffset.Pos, &d.queueEndInfo.VirtualEnd)
	if err != nil {
		return err
	}
	d.readPos = d.confirmedOffset
	d.virtualReadOffset = d.virtualConfirmedOffset
	d.updateDepth()

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueueReader) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n%d,%d,%d\n",
		d.queueEndInfo.TotalMsgCnt,
		d.confirmedOffset.FileNum, d.confirmedOffset.Pos, d.virtualConfirmedOffset,
		d.queueEndInfo.EndOffset.FileNum, d.queueEndInfo.EndOffset.Pos, d.queueEndInfo.VirtualEnd)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return atomicRename(tmpFileName, fileName)
}

func (d *diskQueueReader) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.reader.dat"),
		d.readerMetaName)
}

func (d *diskQueueReader) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.readFrom, fileNum)
}

func (d *diskQueueReader) checkTailCorruption() {
	if d.readPos.FileNum < d.queueEndInfo.EndOffset.FileNum || d.readPos.Pos < d.queueEndInfo.EndOffset.Pos {
		return
	}

	// we reach file end, the readPos should be exactly at the end of file.
	if d.readPos != d.queueEndInfo.EndOffset {
		nsqLog.LogErrorf(
			"diskqueue(%s) read to end at readPos != endPos (%v > %v), corruption, skipping to end ...",
			d.readerMetaName, d.readPos, d.queueEndInfo.EndOffset)
		d.skipToEndofFile()
		d.needSync = true
	}
}

func (d *diskQueueReader) handleReadError() {
	// shadow should not change the bad file, just log it.
	// TODO: try to find next message position from index log.
	newRead := d.readPos
	newRead.FileNum++
	newRead.Pos = 0
	if newRead.GreatThan(&d.queueEndInfo.EndOffset) {
		newRead = d.queueEndInfo.EndOffset
		d.virtualReadOffset = d.queueEndInfo.VirtualEnd
	} else {
		vdiff, err := d.getVirtualOffsetDistance(d.readPos, newRead)
		if err != nil {
			nsqLog.LogErrorf("diskqueue(%s) move error (%v > %v), corruption, skipping to next...",
				d.readerMetaName, d.readPos, newRead)
			d.skipToNextFile()
			return
		}
		d.virtualReadOffset += vdiff
	}
	d.readPos = newRead
	d.confirmedOffset = d.readPos
	d.virtualConfirmedOffset = d.virtualReadOffset
	d.updateDepth()

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

func (d *diskQueueReader) internalUpdateEnd(endPos *diskQueueEndInfo) (bool, error) {
	if endPos == nil {
		if d.needSync {
			d.sync()
		}
		return false, nil
	}
	if endPos.VirtualEnd == d.queueEndInfo.VirtualEnd && endPos.TotalMsgCnt == d.queueEndInfo.TotalMsgCnt {
		return false, nil
	}
	d.needSync = true
	if d.readPos.FileNum > endPos.EndOffset.FileNum {
		nsqLog.Logf("new end old than the read end: %v, %v", d.readPos, endPos)
		d.readPos.FileNum = endPos.EndOffset.FileNum
		d.readPos.Pos = endPos.EndOffset.Pos
		d.virtualReadOffset = endPos.VirtualEnd
	}
	if (d.readPos.FileNum == endPos.EndOffset.FileNum) && (d.readPos.Pos > endPos.EndOffset.Pos) {
		nsqLog.Logf("new end old than the read end: %v, %v", d.readPos, endPos)
		d.readPos.Pos = endPos.EndOffset.Pos
		d.virtualReadOffset = endPos.VirtualEnd
	}
	if d.confirmedOffset.GreatThan(&d.readPos) {
		d.confirmedOffset = d.readPos
	}
	if d.virtualConfirmedOffset > d.virtualReadOffset {
		d.virtualConfirmedOffset = d.virtualReadOffset
	}

	if endPos.VirtualEnd > d.virtualConfirmedOffset {
		atomic.StoreInt32(&d.waitingMoreData, 0)
	}
	oldPos := d.queueEndInfo
	d.queueEndInfo = *endPos
	d.updateDepth()
	if nsqLog.Level() >= levellogger.LOG_DETAIL {
		nsqLog.LogDebugf("read end %v updated to : %v", oldPos, endPos)
	}

	return true, nil
}
