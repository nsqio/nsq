package nsqd

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidOffset = errors.New("invalid offset")
)

// diskQueueWriter implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueueWriter struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	// run-time state (also persisted to disk)
	diskWriteEnd diskQueueEndInfo
	diskReadEnd  diskQueueEndInfo
	sync.RWMutex

	// instantiation time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	minMsgSize      int32
	maxMsgSize      int32
	exitFlag        int32
	needSync        bool

	writeFile    *os.File
	bufferWriter *bufio.Writer
}

// newDiskQueue instantiates a new instance of diskQueueWriter, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueWriter(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64) BackendQueueWriter {

	d := diskQueueWriter{
		name:            name,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		minMsgSize:      minMsgSize,
		maxMsgSize:      maxMsgSize,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		nsqLog.LogErrorf("diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	return &d
}

func (d *diskQueueWriter) PutV2(data []byte) (BackendOffset, int32, diskQueueEndInfo, error) {
	d.Lock()

	if d.exitFlag == 1 {
		d.Unlock()
		return 0, 0, diskQueueEndInfo{}, errors.New("exiting")
	}
	offset, writeBytes, dend, werr := d.writeOne(data)
	var e diskQueueEndInfo
	if dend != nil {
		e = *dend
	}
	d.needSync = true
	d.Unlock()
	return offset, writeBytes, e, werr
}

// Put writes a []byte to the queue
func (d *diskQueueWriter) Put(data []byte) (BackendOffset, int32, int64, error) {
	d.Lock()

	if d.exitFlag == 1 {
		d.Unlock()
		return 0, 0, 0, errors.New("exiting")
	}
	offset, writeBytes, dend, werr := d.writeOne(data)
	var e diskQueueEndInfo
	if dend != nil {
		e = *dend
	}
	d.needSync = true
	d.Unlock()
	return offset, writeBytes, e.TotalMsgCnt, werr
}

func (d *diskQueueWriter) RollbackWriteV2(offset BackendOffset, diffCnt uint64) (diskQueueEndInfo, error) {
	d.Lock()
	defer d.Unlock()
	if d.needSync {
		d.sync()
	}

	if offset > d.diskWriteEnd.VirtualEnd {
		return d.diskWriteEnd, ErrInvalidOffset
	}
	if offset < d.diskWriteEnd.VirtualEnd-BackendOffset(d.diskWriteEnd.EndOffset.Pos) {
		nsqLog.Logf("rollback write position can not across file %v, %v, %v", offset, d.diskWriteEnd.VirtualEnd, d.diskWriteEnd.EndOffset.Pos)
		return d.diskWriteEnd, ErrInvalidOffset
	}
	nsqLog.Logf("rollback from %v-%v, %v to %v, roll cnt: %v", d.diskWriteEnd.EndOffset.FileNum, d.diskWriteEnd.EndOffset.Pos, d.diskWriteEnd.VirtualEnd, offset, diffCnt)
	d.diskWriteEnd.EndOffset.Pos -= int64(d.diskWriteEnd.VirtualEnd - offset)
	d.diskWriteEnd.VirtualEnd = offset
	atomic.AddInt64(&d.diskWriteEnd.TotalMsgCnt, -1*int64(diffCnt))

	if d.diskReadEnd.EndOffset.Pos > d.diskWriteEnd.EndOffset.Pos ||
		d.diskReadEnd.VirtualEnd > d.diskWriteEnd.VirtualEnd {
		d.diskReadEnd = d.diskWriteEnd
	}
	nsqLog.Logf("after rollback : %v, %v, read end: %v", d.diskWriteEnd.EndOffset.Pos, d.diskWriteEnd.TotalMsgCnt, d.diskReadEnd)
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
	return d.diskWriteEnd, nil
}

func (d *diskQueueWriter) RollbackWrite(offset BackendOffset, diffCnt uint64) error {
	_, err := d.RollbackWriteV2(offset, diffCnt)
	return err
}

func (d *diskQueueWriter) ResetWriteEnd(offset BackendOffset, totalCnt int64) error {
	_, err := d.ResetWriteEndV2(offset, totalCnt)
	return err
}

func (d *diskQueueWriter) closeCurrentFile() {
	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
	}
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
}

func (d *diskQueueWriter) ResetWriteEndV2(offset BackendOffset, totalCnt int64) (diskQueueEndInfo, error) {
	d.Lock()
	defer d.Unlock()
	if offset > d.diskWriteEnd.VirtualEnd {
		return d.diskWriteEnd, ErrInvalidOffset
	}
	if d.needSync {
		d.sync()
	}
	nsqLog.Logf("reset write end from %v to %v, reset to totalCnt: %v", d.diskWriteEnd.VirtualEnd, offset, totalCnt)
	if offset == 0 {
		d.closeCurrentFile()
		d.diskWriteEnd.VirtualEnd = 0
		d.diskWriteEnd.EndOffset.Pos = 0
		d.diskWriteEnd.EndOffset.FileNum = 0
		atomic.StoreInt64(&d.diskWriteEnd.TotalMsgCnt, 0)
		d.diskReadEnd = d.diskWriteEnd
		return d.diskWriteEnd, nil
	}
	newEnd := d.diskWriteEnd.VirtualEnd
	newWriteFileNum := d.diskWriteEnd.EndOffset.FileNum
	newWritePos := d.diskWriteEnd.EndOffset.Pos
	for offset < newEnd-BackendOffset(newWritePos) {
		nsqLog.Logf("reset write acrossing file %v, %v, %v, %v", offset, newEnd, newWritePos, newWriteFileNum)
		newEnd -= BackendOffset(newWritePos)
		newWriteFileNum--
		if newWriteFileNum < 0 {
			nsqLog.Logf("reset write acrossed the begin %v, %v, %v", offset, newEnd, newWriteFileNum)
			return d.diskWriteEnd, ErrInvalidOffset
		}
		f, err := os.Stat(d.fileName(newWriteFileNum))
		if err != nil {
			nsqLog.LogErrorf("stat data file error %v, %v", offset, newWriteFileNum)
			return d.diskWriteEnd, err
		}
		newWritePos = f.Size()
	}
	d.diskWriteEnd.EndOffset.FileNum = newWriteFileNum
	newWritePos -= int64(newEnd - offset)
	d.diskWriteEnd.EndOffset.Pos = newWritePos
	d.diskWriteEnd.VirtualEnd = offset
	atomic.StoreInt64(&d.diskWriteEnd.TotalMsgCnt, int64(totalCnt))
	d.diskReadEnd = d.diskWriteEnd
	d.closeCurrentFile()
	nsqLog.Logf("reset write end result : %v", d.diskWriteEnd)

	return d.diskWriteEnd, nil
}

// Close cleans up the queue and persists metadata
func (d *diskQueueWriter) Close() error {
	return d.exit(false)
}

func (d *diskQueueWriter) Delete() error {
	return d.exit(true)
}

func (d *diskQueueWriter) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		nsqLog.Logf("DISKQUEUE(%s): deleting", d.name)
	} else {
		nsqLog.Logf("DISKQUEUE(%s): closing", d.name)
	}

	d.sync()
	if deleted {
		return d.deleteAllFiles(deleted)
	}
	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueueWriter) Empty() error {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	nsqLog.Logf("DISKQUEUE(%s): emptying", d.name)
	return d.deleteAllFiles(false)
}

func (d *diskQueueWriter) deleteAllFiles(deleted bool) error {
	d.cleanOldData()

	if deleted {
		nsqLog.Logf("DISKQUEUE(%s): deleting meta file", d.name)
		innerErr := os.Remove(d.metaDataFileName())
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr)
			return innerErr
		}
	}
	return nil
}

func (d *diskQueueWriter) cleanOldData() error {

	d.closeCurrentFile()

	d.saveFileOffsetMeta()

	for i := int64(0); i <= d.diskWriteEnd.EndOffset.FileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		nsqLog.Logf("DISKQUEUE(%s): removed data file", fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
		}
	}

	d.diskWriteEnd.EndOffset.FileNum++
	d.diskWriteEnd.EndOffset.Pos = 0
	d.diskReadEnd = d.diskWriteEnd
	return nil
}

func (d *diskQueueWriter) saveFileOffsetMeta() {
	fName := d.fileName(d.diskWriteEnd.EndOffset.FileNum) + ".offsetmeta.dat"
	f, err := os.OpenFile(fName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		nsqLog.LogErrorf("diskqueue(%s) failed to save data offset meta: %v", d.name, err)
		return
	}
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n",
		atomic.LoadInt64(&d.diskWriteEnd.TotalMsgCnt),
		d.diskWriteEnd.VirtualEnd-BackendOffset(d.diskWriteEnd.EndOffset.Pos), d.diskWriteEnd.VirtualEnd)
	if err != nil {
		f.Close()
		nsqLog.LogErrorf("diskqueue(%s) failed to save data offset meta: %v", d.name, err)
		return
	}
	f.Sync()
	f.Close()
}

func (d *diskQueueWriter) GetQueueWriteEnd() BackendQueueEnd {
	d.RLock()
	e := &diskQueueEndInfo{}
	*e = d.diskWriteEnd
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) GetQueueReadEndV2() *diskQueueEndInfo {
	d.RLock()
	e := d.internalGetQueueReadEnd()
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) GetQueueReadEnd() BackendQueueEnd {
	d.RLock()
	e := d.internalGetQueueReadEnd()
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) internalGetQueueReadEnd() *diskQueueEndInfo {
	e := &diskQueueEndInfo{}
	*e = d.diskReadEnd
	return e
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueueWriter) writeOne(data []byte) (BackendOffset, int32, *diskQueueEndInfo, error) {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.diskWriteEnd.EndOffset.FileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, 0, nil, err
		}

		nsqLog.Logf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.diskWriteEnd.EndOffset.Pos > 0 {
			_, err = d.writeFile.Seek(d.diskWriteEnd.EndOffset.Pos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return 0, 0, nil, err
			}
		}
		if d.bufferWriter == nil {
			d.bufferWriter = bufio.NewWriter(d.writeFile)
		} else {
			d.bufferWriter.Reset(d.writeFile)
		}
	}

	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return 0, 0, nil, fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	err = binary.Write(d.bufferWriter, binary.BigEndian, dataLen)
	if err != nil {
		d.sync()
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		nsqLog.Logf("DISKQUEUE(%s): writeOne() faled %s", d.name, err)
		return 0, 0, nil, err
	}

	_, err = d.bufferWriter.Write(data)
	if err != nil {
		d.sync()
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		nsqLog.Logf("DISKQUEUE(%s): writeOne() faled %s", d.name, err)
		return 0, 0, nil, err
	}

	writeOffset := d.diskWriteEnd.VirtualEnd
	totalBytes := int64(4 + dataLen)
	d.diskWriteEnd.EndOffset.Pos += totalBytes
	d.diskWriteEnd.VirtualEnd += BackendOffset(totalBytes)
	atomic.AddInt64(&d.diskWriteEnd.TotalMsgCnt, 1)

	if d.diskWriteEnd.EndOffset.Pos >= d.maxBytesPerFile {
		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			nsqLog.LogErrorf("diskqueue(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.saveFileOffsetMeta()
		nsqLog.LogDebugf("DISKQUEUE(%s): new file write, last file: %v", d.name, d.diskWriteEnd)

		d.diskWriteEnd.EndOffset.FileNum++
		d.diskWriteEnd.EndOffset.Pos = 0
		d.diskReadEnd = d.diskWriteEnd
	}

	return writeOffset, int32(totalBytes), &d.diskWriteEnd, err
}

func (d *diskQueueWriter) Flush() error {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	if d.needSync {
		return d.sync()
	}
	return nil
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueueWriter) sync() error {
	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
	}
	d.diskReadEnd = d.diskWriteEnd
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueueWriter) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	var totalCnt int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d,%d\n",
		&totalCnt,
		&d.diskWriteEnd.EndOffset.FileNum, &d.diskWriteEnd.EndOffset.Pos, &d.diskWriteEnd.VirtualEnd)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.diskWriteEnd.TotalMsgCnt, totalCnt)
	d.diskReadEnd = d.diskWriteEnd

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueueWriter) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n",
		atomic.LoadInt64(&d.diskWriteEnd.TotalMsgCnt),
		d.diskWriteEnd.EndOffset.FileNum, d.diskWriteEnd.EndOffset.Pos, d.diskWriteEnd.VirtualEnd)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return atomicRename(tmpFileName, fileName)
}

func (d *diskQueueWriter) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.writer.dat"), d.name)
}

func (d *diskQueueWriter) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}
