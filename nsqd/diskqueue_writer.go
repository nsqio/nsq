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
	writePos           int64
	writeFileNum       int64
	readablePos        int64
	totalMsgCnt        int64
	virtualEnd         BackendOffset
	virtualReadableEnd BackendOffset
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

// Put writes a []byte to the queue
func (d *diskQueueWriter) Put(data []byte) (BackendOffset, int32, int64, error) {
	d.Lock()

	if d.exitFlag == 1 {
		d.Unlock()
		return 0, 0, 0, errors.New("exiting")
	}
	offset, writeBytes, totalCnt, werr := d.writeOne(data)
	d.needSync = true
	d.Unlock()
	return offset, writeBytes, totalCnt, werr
}

func (d *diskQueueWriter) RollbackWrite(offset BackendOffset, diffCnt uint64) error {
	d.Lock()
	defer d.Unlock()
	if d.needSync {
		d.sync()
	}

	if offset > d.virtualEnd {
		return ErrInvalidOffset
	}
	if offset < d.virtualEnd-BackendOffset(d.writePos) {
		nsqLog.Logf("rollback write position can not across file %v, %v, %v", offset, d.virtualEnd, d.writePos)
		return ErrInvalidOffset
	}
	nsqLog.LogDebugf("rollback from %v to %v, roll cnt: %v", d.virtualEnd, offset, diffCnt)
	d.writePos -= int64(d.virtualEnd - offset)
	if d.readablePos > d.writePos {
		d.readablePos = d.writePos
	}
	d.virtualEnd = offset
	atomic.AddInt64(&d.totalMsgCnt, -1*int64(diffCnt))
	if d.virtualReadableEnd > d.virtualEnd {
		d.virtualReadableEnd = d.virtualEnd
	}
	return nil
}

func (d *diskQueueWriter) ResetWriteEnd(offset BackendOffset, totalCnt int64) error {
	d.Lock()
	defer d.Unlock()
	if offset > d.virtualEnd {
		return ErrInvalidOffset
	}
	if d.needSync {
		d.sync()
	}
	nsqLog.Logf("reset write end from %v to %v, reset to totalCnt: %v", d.virtualEnd, offset, totalCnt)
	if offset == 0 {
		d.virtualEnd = 0
		d.writePos = 0
		d.writeFileNum = 0
		d.readablePos = 0
		d.virtualReadableEnd = 0
		atomic.StoreInt64(&d.totalMsgCnt, 0)
		return nil
	}
	newEnd := d.virtualEnd
	newWriteFileNum := d.writeFileNum
	newWritePos := d.writePos
	for offset < newEnd-BackendOffset(newWritePos) {
		nsqLog.Logf("reset write acrossing file %v, %v, %v, %v", offset, newEnd, newWritePos, newWriteFileNum)
		newEnd -= BackendOffset(newWritePos)
		newWriteFileNum--
		if newWriteFileNum < 0 {
			nsqLog.Logf("reset write acrossed the begin %v, %v, %v", offset, newEnd, newWriteFileNum)
			return ErrInvalidOffset
		}
		f, err := os.Stat(d.fileName(newWriteFileNum))
		if err != nil {
			nsqLog.LogErrorf("stat data file error %v, %v", offset, newWriteFileNum)
			return err
		}
		newWritePos = f.Size()
	}
	d.writeFileNum = newWriteFileNum
	newWritePos -= int64(newEnd - offset)
	d.writePos = newWritePos
	if d.readablePos > d.writePos {
		d.readablePos = d.writePos
	}
	d.virtualEnd = offset
	atomic.StoreInt64(&d.totalMsgCnt, int64(totalCnt))
	if d.virtualReadableEnd > d.virtualEnd {
		d.virtualReadableEnd = d.virtualEnd
	}
	return nil
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
	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
	}
	d.virtualReadableEnd = d.virtualEnd
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	d.saveFileOffsetMeta()

	for i := int64(0); i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		nsqLog.Logf("DISKQUEUE(%s): removed data file", fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readablePos = 0
	return nil
}

func (d *diskQueueWriter) saveFileOffsetMeta() {
	fName := d.fileName(d.writeFileNum) + ".offsetmeta.dat"
	f, err := os.OpenFile(fName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		nsqLog.LogErrorf("diskqueue(%s) failed to save data offset meta: %v", d.name, err)
		return
	}
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n",
		atomic.LoadInt64(&d.totalMsgCnt),
		d.virtualEnd-BackendOffset(d.writePos), d.virtualEnd)
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
	e.EndOffset.FileNum = d.writeFileNum
	e.EndOffset.Pos = d.writePos
	e.TotalMsgCnt = d.totalMsgCnt
	e.VirtualEnd = d.virtualEnd
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) GetQueueReadEnd() BackendQueueEnd {
	d.RLock()
	e := d.internalGetQueueReadEnd()
	d.RUnlock()
	return e
}

func (d *diskQueueWriter) internalGetQueueReadEnd() BackendQueueEnd {
	e := &diskQueueEndInfo{}
	e.EndOffset.FileNum = d.writeFileNum
	e.EndOffset.Pos = d.readablePos
	e.TotalMsgCnt = d.totalMsgCnt
	e.VirtualEnd = d.virtualReadableEnd
	return e
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueueWriter) writeOne(data []byte) (BackendOffset, int32, int64, error) {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return 0, 0, 0, err
		}

		nsqLog.Logf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return 0, 0, 0, err
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
		return 0, 0, 0, fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	err = binary.Write(d.bufferWriter, binary.BigEndian, dataLen)
	if err != nil {
		return 0, 0, 0, err
	}

	_, err = d.bufferWriter.Write(data)
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return 0, 0, 0, err
	}

	writeOffset := d.virtualEnd
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	d.virtualEnd += BackendOffset(totalBytes)
	totalCnt := atomic.AddInt64(&d.totalMsgCnt, 1)

	if d.writePos >= d.maxBytesPerFile {
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
		nsqLog.LogDebugf("DISKQUEUE(%s): new file write, last file: %v, %v, %v, %v", d.name, d.writeFileNum, d.writePos, d.virtualEnd, totalCnt)

		d.writeFileNum++
		d.writePos = 0
		d.readablePos = 0
	}

	return writeOffset, int32(totalBytes), totalCnt, err
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
	d.virtualReadableEnd = d.virtualEnd
	d.readablePos = d.writePos
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
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var totalCnt int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d,%d\n",
		&totalCnt,
		&d.writeFileNum, &d.writePos, &d.virtualEnd)
	if err != nil {
		return err
	}
	d.readablePos = d.writePos
	d.virtualReadableEnd = d.virtualEnd
	atomic.StoreInt64(&d.totalMsgCnt, totalCnt)

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueueWriter) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n",
		atomic.LoadInt64(&d.totalMsgCnt),
		d.writeFileNum, d.writePos, d.virtualEnd)
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
