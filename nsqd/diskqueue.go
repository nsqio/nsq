package main

import (
	"../nsq"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

// DiskQueue implements the nsq.BackendQueue interface
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

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos int64

	readFile  *os.File
	writeFile *os.File
	open      bool

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	writeContinueChan chan int
	exitChan          chan int
}

// NewDiskQueue instantiates a new instance of DiskQueue, retrieving meta-data
// from the filesystem and starting the read ahead goroutine
func NewDiskQueue(name string, dataPath string, maxBytesPerFile int64) nsq.BackendQueue {
	d := DiskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		readChan:          make(chan []byte),
		exitChan:          make(chan int),
		writeContinueChan: make(chan int),
		open:              true,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil {
		log.Printf("WARNING: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err.Error())
	}

	go d.readAheadPump()

	return &d
}

// Depth returns the depth of the queue
func (d *DiskQueue) Depth() int64 {
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
	d.readMutex.Lock()
	defer d.readMutex.Unlock()

	d.writeMutex.Lock()
	defer d.writeMutex.Unlock()

	// this guarantees that no other goroutines can successfully read/write that may
	// currently be waiting on either of the above mutex
	d.open = false
	close(d.exitChan)

	if d.readFile != nil {
		d.readFile.Close()
	}

	if d.writeFile != nil {
		d.writeFile.Close()
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *DiskQueue) Empty() error {
	d.readMutex.Lock()
	defer d.readMutex.Unlock()

	d.writeMutex.Lock()
	defer d.writeMutex.Unlock()

	d.metaMutex.Lock()
	for d.readFileNum < d.writeFileNum {
		fn := d.fileName(d.readFileNum)
		err := os.Remove(fn)
		if err != nil {
			// only log this state because, although unexpected, 
			// it's effectively the same as having removed the file
			log.Printf("ERROR: failed to Remove(%s) - %s", fn, err.Error())
		}
		d.readFileNum++
	}
	d.readPos = d.writePos
	d.nextReadPos = d.readPos
	d.depth = 0
	d.metaMutex.Unlock()

	d.writeContinueChan <- 1

	return nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *DiskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	d.readMutex.Lock()
	defer d.readMutex.Unlock()

	if !d.open {
		return nil, errors.New("E_CLOSED")
	}

	if d.readPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}
		fn := d.fileName(d.readFileNum)
		err := os.Remove(fn)
		if err != nil {
			log.Printf("ERROR: failed to Remove(%s) - %s", fn, err.Error())
		}

		d.metaMutex.Lock()
		d.readFileNum++
		d.readPos = 0
		d.metaMutex.Unlock()
		err = d.persistMetaData()
		if err != nil {
			return nil, err
		}
	}

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
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

	totalBytes := 4 + msgSize
	// we only advance nextReadPos because we have not yet sent
	// this to consumers of the queue (where readPos will actually
	// be advanced)
	d.nextReadPos = d.readPos + int64(totalBytes)

	// log.Printf("DISK: read %d bytes - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d\n",
	// 	totalBytes, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *DiskQueue) writeOne(data []byte) error {
	var err error
	var buf bytes.Buffer

	d.writeMutex.Lock()
	defer d.writeMutex.Unlock()

	if !d.open {
		return errors.New("E_CLOSED")
	}

	if d.writePos > d.maxBytesPerFile {
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}

		d.metaMutex.Lock()
		d.writeFileNum++
		d.writePos = 0
		d.metaMutex.Unlock()
		err = d.persistMetaData()
		if err != nil {
			return err
		}
	}

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
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

	_, err = d.writeFile.Write(buf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	err = d.writeFile.Sync()
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := 4 + dataLen
	d.metaMutex.Lock()
	d.writePos += int64(totalBytes)
	d.depth++
	d.metaMutex.Unlock()

	d.writeContinueChan <- 1

	// log.Printf("DISK: wrote %d bytes - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d\n",
	// 	totalBytes, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *DiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	d.metaMutex.Lock()
	defer d.metaMutex.Unlock()

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
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *DiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	d.metaMutex.Lock()
	defer d.metaMutex.Unlock()

	fileName := d.metaDataFileName()
	tmpFileName := fileName + ".tmp"

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		d.depth,
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
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

func (d *DiskQueue) hasDataToRead() bool {
	return (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos)
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
	for {
		if d.hasDataToRead() {
			if d.nextReadPos == d.readPos {
				data, err = d.readOne()
				if err != nil {
					if err.Error() == "E_CLOSED" {
						return
					}
					log.Printf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err.Error())
					// we assume that all read errors are recoverable...
					// it will probably turn out that this is a terrible assumption
					// as this could certainly result in an infinite busy loop
					continue
				}
			}
			select {
			case d.readChan <- data:
				d.metaMutex.Lock()
				d.depth--
				d.readPos = d.nextReadPos
				d.metaMutex.Unlock()
			case <-d.writeContinueChan:
			case <-d.exitChan:
				return
			}
		} else {
			select {
			case <-d.writeContinueChan:
			case <-d.exitChan:
				return
			}
		}
	}
}
