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
	"time"
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
	syncEvery int64, syncTimeout time.Duration) BackendQueueWriter {

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
func (d *diskQueueWriter) Put(data []byte) (BackendQueueEnd, error) {
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return nil, errors.New("exiting")
	}
	e, werr := d.writeOne(data)
	d.needSync = true
	return e, werr
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
	d.skipToNextRWFile()

	if deleted {
		innerErr := os.Remove(d.metaDataFileName())
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr)
			return innerErr
		}
	}
	return nil
}

func (d *diskQueueWriter) skipToNextRWFile() error {
	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
	}
	d.virtualReadableEnd = d.virtualEnd
	d.readablePos = 0
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := int64(0); i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			nsqLog.LogErrorf("diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
		}
	}

	d.writeFileNum++
	d.writePos = 0
	return nil
}

func (d *diskQueueWriter) GetQueueReadEnd() BackendQueueEnd {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return nil
	}
	return d.internalGetQueueReadEnd()
}

func (d *diskQueueWriter) internalGetQueueReadEnd() BackendQueueEnd {
	e := &diskQueueEndInfo{}
	e.EndFileNum = d.writeFileNum
	e.EndPos = d.readablePos
	e.TotalMsgCnt = d.totalMsgCnt
	e.VirtualEnd = d.virtualReadableEnd
	return e
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueueWriter) writeOne(data []byte) (*diskQueueEndInfo, error) {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}

		nsqLog.Logf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return nil, err
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
		return nil, fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	err = binary.Write(d.bufferWriter, binary.BigEndian, dataLen)
	if err != nil {
		return nil, err
	}

	_, err = d.bufferWriter.Write(data)
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return nil, err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	d.virtualEnd += BackendOffset(totalBytes)
	totalCnt := atomic.AddInt64(&d.totalMsgCnt, 1)

	endInfo := &diskQueueEndInfo{
		EndFileNum:  d.writeFileNum,
		EndPos:      d.writePos,
		TotalMsgCnt: totalCnt,
		VirtualEnd:  d.virtualEnd,
	}

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			nsqLog.LogErrorf("diskqueue(%s) failed to sync - %s", d.name, err)
		}

		d.readablePos = 0
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return endInfo, err
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
