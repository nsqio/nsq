package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
)

// DiskQueue implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type DiskQueue struct {
	// separate mutexes are used to be able to exclusively
	// read/write or update meta-data
	metaMutex  sync.Mutex
	readMutex  sync.Mutex
	writeMutex sync.Mutex

	// instatiation time meta-data
	name            string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	syncEvery       int64 // number of writes per sync
	exitFlag        int32

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	emptyChan     chan int
	readStateChan chan int
	exitChan      chan int
	exitSyncChan  chan int
}

// NewDiskQueue instantiates a new instance of DiskQueue, retrieving meta-data
// from the filesystem and starting the read ahead goroutine
func NewDiskQueue(name string, dataPath string, maxBytesPerFile int64, syncEvery int64) BackendQueue {
	d := DiskQueue{
		name:            name,
		dataPath:        dataPath,
		maxBytesPerFile: maxBytesPerFile,
		readChan:        make(chan []byte),
		emptyChan:       make(chan int),
		exitChan:        make(chan int),
		exitSyncChan:    make(chan int),
		readStateChan:   make(chan int, 1),
		syncEvery:       syncEvery,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		log.Printf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err.Error())
	}

	go d.readAheadPump()

	return &d
}

// Depth returns the depth of the queue
func (d *DiskQueue) Depth() int64 {
	d.metaMutex.Lock()
	defer d.metaMutex.Unlock()
	return d.depth
}

// ReadChan returns the []byte channel for reading data
func (d *DiskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a single []byte to the queue
func (d *DiskQueue) Put(p []byte) error {
	return d.writeOne(p)
}

// Close cleans up the queue and persists meta-data
func (d *DiskQueue) Close() error {
	log.Printf("DISKQUEUE(%s): closing", d.name)

	// this guarantees that no other goroutines can successfully read/write that may
	// currently be waiting on either of the below mutex
	atomic.StoreInt32(&d.exitFlag, 1)

	d.readMutex.Lock()
	d.writeMutex.Lock()

	close(d.exitChan)

	if d.readFile != nil {
		d.readFile.Close()
	}

	if d.writeFile != nil {
		d.writeFile.Close()
	}

	err := d.persistMetaData()

	d.readMutex.Unlock()
	d.writeMutex.Unlock()

	// ensure that readAheadPump has exited
	// this needs to be done *after* readMutex is unlocked
	<-d.exitSyncChan

	return err
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *DiskQueue) Empty() error {
	d.emptyChan <- 1
	return nil
}

func (d *DiskQueue) doEmpty() error {
	d.readMutex.Lock()
	defer d.readMutex.Unlock()

	d.writeMutex.Lock()
	defer d.writeMutex.Unlock()

	if atomic.LoadInt32(&d.exitFlag) == 1 {
		return errors.New("E_EXITING")
	}

	log.Printf("DISKQUEUE(%s): emptying", d.name)

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	d.metaMutex.Lock()

	readFileNum := d.readFileNum
	writeFileNum := d.writeFileNum
	writePos := d.writePos

	// make a list of read file numbers to remove (later)
	numsToRemove := make([]int64, 0)
	for readFileNum < writeFileNum {
		numsToRemove = append(numsToRemove, readFileNum)
		readFileNum++
	}

	d.readFileNum = writeFileNum
	d.readPos = writePos
	d.nextReadFileNum = writeFileNum
	d.nextReadPos = writePos
	d.depth = 0

	d.metaMutex.Unlock()

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	// only if we've successfully persisted metadata do we remove old files
	for _, num := range numsToRemove {
		fn := d.fileName(num)
		err := os.Remove(fn)
		if err != nil {
			return err
		}
	}

	return nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *DiskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	d.readMutex.Lock()
	defer d.readMutex.Unlock()

	if atomic.LoadInt32(&d.exitFlag) == 1 {
		return nil, errors.New("E_EXITING")
	}

	readPos := d.readPos
	readFileNum := d.readFileNum

	if d.readFile == nil {
		curFileName := d.fileName(readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		log.Printf("DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if readPos > 0 {
			_, err = d.readFile.Seek(readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
	}

	err = binary.Read(d.readFile, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.readFile, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// because readOne is only called inside readAheadPump
	// we do not need to lock around these
	nextReadPos := readPos + totalBytes
	nextReadFileNum := readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	if nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		nextReadFileNum++
		nextReadPos = 0
	}

	// we only advance next* because we have not yet sent this to consumers 
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadFileNum = nextReadFileNum
	d.nextReadPos = nextReadPos

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *DiskQueue) writeOne(data []byte) error {
	var err error
	var buf bytes.Buffer

	d.writeMutex.Lock()
	defer d.writeMutex.Unlock()

	if atomic.LoadInt32(&d.exitFlag) == 1 {
		return errors.New("E_EXITING")
	}

	persist := false

	d.metaMutex.Lock()
	writePos := d.writePos
	writeFileNum := d.writeFileNum
	d.metaMutex.Unlock()

	if d.writeFile == nil {
		curFileName := d.fileName(writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}

		log.Printf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if writePos > 0 {
			_, err = d.writeFile.Seek(writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := len(data)

	err = binary.Write(&buf, binary.BigEndian, int32(dataLen))
	if err != nil {
		return err
	}

	_, err = buf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(buf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	writePos += totalBytes

	if writePos > d.maxBytesPerFile {
		d.writeFile.Close()
		d.writeFile = nil

		writeFileNum++
		writePos = 0

		persist = true
	}

	d.metaMutex.Lock()
	d.writeFileNum = writeFileNum
	d.writePos = writePos
	d.depth++
	d.metaMutex.Unlock()

	// dont sync all the time :)
	sync := d.depth % d.syncEvery
	if d.writeFile != nil && sync == 0 {
		err = d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}

		persist = true
	}

	if persist {
		err = d.persistMetaData()
		if err != nil {
			return err
		}
	}

	// you can always *try* to write to readStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway
	select {
	case d.readStateChan <- 1:
	default:
	}

	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *DiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&d.depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *DiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	d.metaMutex.Lock()
	depth := d.depth
	readFileNum := d.readFileNum
	readPos := d.readPos
	writeFileNum := d.writeFileNum
	writePos := d.writePos
	d.metaMutex.Unlock()

	fileName := d.metaDataFileName()
	tmpFileName := fileName + ".tmp"

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		depth,
		readFileNum, readPos,
		writeFileNum, writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *DiskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *DiskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

// readAheadPump provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *DiskQueue) readAheadPump() {
	var data []byte
	var err error

	// readStateChan has a buffer of 1 to guarantee that in the event
	// there is a race before we enter either of the select loops where 
	// readStateChan is read from that the update is not lost
	for {
		nextReadPos := d.nextReadPos
		readFileNum := d.readFileNum
		readPos := d.readPos

		d.metaMutex.Lock()
		writeFileNum := d.writeFileNum
		writePos := d.writePos
		d.metaMutex.Unlock()

		if (readFileNum < writeFileNum) || (readPos < writePos) {
			if nextReadPos == readPos {
				data, err = d.readOne()
				if err != nil {
					if err.Error() == "E_EXITING" {
						goto exit
					}
					log.Printf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
						d.name, readPos, d.fileName(readFileNum), err.Error())
					// we assume that all read errors are recoverable...
					// it will probably turn out that this is a terrible assumption
					// as this could certainly result in an infinite busy loop
					runtime.Gosched()
					continue
				}
			}

			select {
			case d.readChan <- data:
				oldReadFileNum := d.readFileNum

				d.metaMutex.Lock()
				d.depth--
				d.readFileNum = d.nextReadFileNum
				d.readPos = d.nextReadPos
				d.metaMutex.Unlock()

				// see if we need to clean up the old file
				if oldReadFileNum != d.nextReadFileNum {
					fn := d.fileName(readFileNum)

					err := d.persistMetaData()
					if err != nil {
						log.Printf("ERROR: failed to persistMetaData (not removing %s) - %s", fn, err.Error())
						continue
					}

					// only if we've successfully persisted metadata do we remove old files
					err = os.Remove(fn)
					if err != nil {
						log.Printf("ERROR: failed to Remove(%s) - %s", fn, err.Error())
					}
				}
			case <-d.emptyChan:
				err := d.doEmpty()
				if err != nil {
					log.Printf("ERROR: doEmpty() - %s", err.Error())
				}
			case <-d.readStateChan:
			case <-d.exitChan:
				goto exit
			}
		} else {
			select {
			case <-d.emptyChan:
				err := d.doEmpty()
				if err != nil {
					log.Printf("ERROR: doEmpty() - %s", err.Error())
				}
			case <-d.readStateChan:
			case <-d.exitChan:
				goto exit
			}
		}
	}

exit:
	log.Printf("DISKQUEUE(%s): closing ... readAheadPump", d.name)
	d.exitSyncChan <- 1
}
