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
	syncEvery       int64         // number of writes per fsync
	syncTimeout     time.Duration // duration of time per fsync
	exitFlag        int32
	needSync        bool

	writeFile    *os.File
	bufferWriter *bufio.Writer

	// internal channels
	writeChan         chan []byte
	writeResponseChan chan struct {
		end *diskQueueEndInfo
		err error
	}
	emptyChan         chan int
	emptyResponseChan chan error
	flushChan         chan int
	flushResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int
	getEndChan        chan int
	endResponseChan   chan BackendQueueEnd
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
		writeChan:       make(chan []byte),
		writeResponseChan: make(chan struct {
			end *diskQueueEndInfo
			err error
		}),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		flushChan:         make(chan int),
		flushResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		getEndChan:        make(chan int),
		endResponseChan:   make(chan BackendQueueEnd),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		nsqLog.LogErrorf("diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()

	return &d
}

// Put writes a []byte to the queue
func (d *diskQueueWriter) Put(data []byte) (BackendQueueEnd, error) {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return nil, errors.New("exiting")
	}

	d.writeChan <- data
	ret := <-d.writeResponseChan
	return ret.end, ret.err
}

// Close cleans up the queue and persists metadata
func (d *diskQueueWriter) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
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

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.bufferWriter != nil {
		d.bufferWriter.Flush()
	}
	d.virtualReadableEnd = d.virtualEnd
	d.readablePos = d.writePos
	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueueWriter) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	nsqLog.Logf("DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *diskQueueWriter) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	//innerErr := os.Remove(d.metaDataFileName())
	//if innerErr != nil && !os.IsNotExist(innerErr) {
	//	nsqLog.Logf("ERROR: diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr)
	//	return innerErr
	//}

	return err
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
	d.getEndChan <- 1
	return <-d.endResponseChan
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

		if d.bufferWriter != nil {
			d.bufferWriter.Flush()
		}
		d.virtualReadableEnd = d.virtualEnd
		d.readablePos = 0
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return endInfo, err
}

func (d *diskQueueWriter) Flush() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}
	d.flushChan <- 1
	return <-d.flushResponseChan
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

// NOTE: never call any lock op here or it may cause deadlock.
func (d *diskQueueWriter) ioLoop() {
	var err error
	var count int64

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		if count == d.syncEvery {
			count = 0
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				nsqLog.LogErrorf("diskqueue(%s) failed to sync - %s", d.name, err)
			}
		}

		select {
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case <-d.getEndChan:
			d.endResponseChan <- d.internalGetQueueReadEnd()
		case dataWrite := <-d.writeChan:
			count++
			e, werr := d.writeOne(dataWrite)
			d.writeResponseChan <- struct {
				end *diskQueueEndInfo
				err error
			}{
				end: e,
				err: werr,
			}
		case wait := <-d.flushChan:
			if count > 0 {
				count = 0
				d.needSync = true
				if wait > 1 {
					d.flushResponseChan <- d.sync()
					continue
				}
			}
			d.flushResponseChan <- nil
		case <-syncTicker.C:
			if count > 0 {
				count = 0
				d.needSync = true
			}
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	nsqLog.Logf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
