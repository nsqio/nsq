package nsqd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

type diskQueueEndInfo struct {
	EndFileNum  int64
	EndPos      int64
	TotalMsgCnt int64
}

type diskQueueSkipInfo struct {
	SkipToFileNum int64
	SkipToPos     int64
	SkippedCnt    int64
}

// diskQueueReader implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueueReader struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos     int64
	endPos      int64
	readFileNum int64
	endFileNum  int64
	// left message number for read
	depth int64
	// total need to read
	totalMsgCnt int64
	// already read before
	readMsgCnt int64

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

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile *os.File
	reader   *bufio.Reader

	// exposed via ReadChan()
	readResultChan   chan ReadResult
	skipReadErrChan  chan diskQueueSkipInfo
	skipChan         chan diskQueueSkipInfo
	skipResponseChan chan error

	// internal channels
	endUpdatedChan chan *diskQueueEndInfo
	exitChan       chan int
	exitSyncChan   chan int
	autoSkipError  bool

	logger logger
}

// newDiskQueue instantiates a new instance of diskQueueReader, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueueReader(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, autoSkip bool,
	logger logger) BackendQueueReader {
	d := diskQueueReader{
		name:             name,
		dataPath:         dataPath,
		maxBytesPerFile:  maxBytesPerFile,
		minMsgSize:       minMsgSize,
		maxMsgSize:       maxMsgSize,
		readResultChan:   make(chan ReadResult),
		skipReadErrChan:  make(chan diskQueueSkipInfo),
		skipChan:         make(chan diskQueueSkipInfo),
		skipResponseChan: make(chan error),
		endUpdatedChan:   make(chan *diskQueueEndInfo),
		exitChan:         make(chan int),
		exitSyncChan:     make(chan int),
		syncEvery:        syncEvery,
		syncTimeout:      syncTimeout,
		autoSkipError:    autoSkip,
		logger:           logger,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()

	return &d
}

func (d *diskQueueReader) logf(f string, args ...interface{}) {
	if d.logger == nil {
		return
	}
	d.logger.Output(2, fmt.Sprintf(f, args...))
}

// Depth returns the depth of the queue
func (d *diskQueueReader) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

func (d *diskQueueReader) ReadChan() chan ReadResult {
	return d.readResultChan
}

// Put writes a []byte to the queue
func (d *diskQueueReader) UpdateQueueEnd(e BackendQueueEnd) {
	end, ok := e.(*diskQueueEndInfo)
	if !ok || end == nil {
		return
	}
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return
	}
	d.endUpdatedChan <- end
}

// Close cleans up the queue and persists metadata
func (d *diskQueueReader) Close() error {
	err := d.exit()
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueueReader) exit() error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1
	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	return nil
}

func (d *diskQueueReader) IgnoreErrorAndSkipTo(s diskQueueSkipInfo) error {
	d.RLock()
	defer d.RUnlock()
	d.skipReadErrChan <- s
	return <-d.skipResponseChan
}

func (d *diskQueueReader) SkipTo(fileNum int64, pos int64, skippedCnt int64) error {
	d.RLock()
	defer d.RUnlock()
	var info diskQueueSkipInfo
	info.SkipToFileNum = fileNum
	info.SkipToPos = pos
	info.SkippedCnt = skippedCnt
	d.skipChan <- info
	return <-d.skipResponseChan
}

func (d *diskQueueReader) SkipToEnd() {
	d.RLock()
	defer d.RUnlock()
	var info diskQueueSkipInfo
	info.SkipToFileNum = d.endFileNum
	info.SkipToPos = d.endPos
	info.SkippedCnt = d.totalMsgCnt - d.readMsgCnt
	d.skipChan <- info
	<-d.skipResponseChan
}

func (d *diskQueueReader) internalSkipTo(fileNum int64, pos int64, skippedCnt int64) {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	if fileNum > d.endFileNum {
		fileNum = d.endFileNum
		pos = d.endPos
	}
	if pos > d.endPos {
		pos = d.endPos
	}

	d.readFileNum = fileNum
	d.readPos = pos
	d.nextReadFileNum = fileNum
	d.nextReadPos = pos
	atomic.AddInt64(&d.readMsgCnt, skippedCnt)
	atomic.StoreInt64(&d.depth, d.totalMsgCnt-d.readMsgCnt)
}

func (d *diskQueueReader) skipToEndofFile() error {
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	d.readFileNum = d.endFileNum
	d.readPos = d.endPos
	d.nextReadFileNum = d.endFileNum
	d.nextReadPos = d.endPos
	d.readMsgCnt = d.totalMsgCnt
	atomic.StoreInt64(&d.depth, 0)

	return nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueueReader) readOne() ([]byte, error) {
	var err error
	var msgSize int32

CheckFileOpen:
	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf("DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}
	if d.readFileNum < d.endFileNum {
		stat, err := d.readFile.Stat()
		if err != nil {
			return nil, err
		}
		if d.readPos >= stat.Size() {
			d.readFileNum++
			d.readPos = 0
			d.logf("DISKQUEUE(%s): readOne() read end, try next: %v", d.readFileNum)
			d.readFile.Close()
			d.readFile = nil
			goto CheckFileOpen
		}
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	isEnd := false
	if d.nextReadFileNum < d.endFileNum {
		stat, err := d.readFile.Stat()
		if err == nil {
			isEnd = d.nextReadPos >= stat.Size()
		} else {
			return readBuf, err
		}
	}
	if d.nextReadPos > d.maxBytesPerFile || isEnd {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
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
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	_, err = fmt.Fscanf(f, "%d,%d\n%d,%d\n%d,%d\n",
		&d.readMsgCnt, &d.totalMsgCnt,
		&d.readFileNum, &d.readPos,
		&d.endFileNum, &d.endPos)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueueReader) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d,%d\n%d,%d\n%d,%d\n",
		d.readMsgCnt, d.totalMsgCnt,
		d.readFileNum, d.readPos,
		d.endFileNum, d.endPos)
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
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.reader.dat"), d.name)
}

func (d *diskQueueReader) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueueReader) checkTailCorruption(depth int64) {
	if d.readFileNum < d.endFileNum || d.readPos < d.endPos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(
				"ERROR: diskqueue(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(
				"ERROR: diskqueue(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)
		atomic.StoreInt64(&d.readMsgCnt, d.totalMsgCnt)
		d.needSync = true
	}

	if d.readFileNum != d.endFileNum || d.readPos != d.endPos {
		if d.readFileNum > d.endFileNum {
			d.logf(
				"ERROR: diskqueue(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.endFileNum)
		}

		if d.readPos > d.endPos {
			d.logf(
				"ERROR: diskqueue(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.endPos)
		}

		d.skipToEndofFile()
		d.needSync = true
	}
}

func (d *diskQueueReader) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	readMsgCnt := atomic.AddInt64(&d.readMsgCnt, 1)
	d.depth = d.totalMsgCnt - readMsgCnt

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true
	}

	d.checkTailCorruption(d.depth)
}

func (d *diskQueueReader) handleReadError() {
	// shadow should not change the bad file, just log it.
	// TODO: try to find next message position from index log.
	d.readFileNum++
	d.readPos = 0
	if d.readFileNum > d.endFileNum {
		d.readFileNum = d.endFileNum
		d.readPos = d.endPos
	}
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueueReader) ioLoop() {
	var dataRead []byte
	var rerr error
	var syncerr error
	var count int64
	var r chan ReadResult
	var skipChan chan diskQueueSkipInfo

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		if count == d.syncEvery {
			count = 0
			d.needSync = true
		}

		if d.needSync {
			syncerr = d.sync()
			if syncerr != nil {
				d.logf("ERROR: diskqueue(%s) failed to sync - %s", d.name, syncerr)
			}
		}

		if rerr != nil {
			skipChan = d.skipReadErrChan
			r = nil
		} else {
			skipChan = nil
			if (d.readFileNum < d.endFileNum) || (d.readPos < d.endPos) {
				if d.nextReadPos == d.readPos {
					dataRead, rerr = d.readOne()
					if rerr != nil {
						d.logf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
							d.name, d.readPos, d.fileName(d.readFileNum), rerr)
						if d.autoSkipError {
							d.handleReadError()
							rerr = nil
							continue
						}
					}
				}
				r = d.readResultChan
			} else {
				r = nil
			}
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- ReadResult{dataRead, rerr}:
			// moveForward sets needSync flag if a file is removed
			if rerr == nil {
				d.moveForward()
			}
		case skipInfo := <-skipChan:
			d.internalSkipTo(skipInfo.SkipToFileNum, skipInfo.SkipToPos, skipInfo.SkippedCnt)
			rerr = nil
			d.skipResponseChan <- nil
		case skipInfo := <-d.skipChan:
			d.internalSkipTo(skipInfo.SkipToFileNum, skipInfo.SkipToPos, skipInfo.SkippedCnt)
			d.skipResponseChan <- nil
		case endPos := <-d.endUpdatedChan:
			count++
			if d.endPos != endPos.EndPos && endPos.EndPos == 0 {
				// a new file for the position
				d.needSync = true
			}
			if d.readFileNum > endPos.EndFileNum {
				d.readFileNum = endPos.EndFileNum
				d.readPos = endPos.EndPos
				d.readMsgCnt = endPos.TotalMsgCnt
			}
			if (d.readFileNum == endPos.EndFileNum) && (d.readPos > endPos.EndPos) {
				d.readPos = endPos.EndPos
				d.readMsgCnt = endPos.TotalMsgCnt
			}
			if d.readMsgCnt > endPos.TotalMsgCnt {
				d.readMsgCnt = endPos.TotalMsgCnt
			}
			d.endPos = endPos.EndPos
			d.endFileNum = endPos.EndFileNum
			d.totalMsgCnt = endPos.TotalMsgCnt
			atomic.StoreInt64(&d.depth, d.totalMsgCnt-d.readMsgCnt)

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
	d.logf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
