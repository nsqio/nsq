package nsq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

type DiskQueue struct {
	readMutex         sync.Mutex
	writeMutex        sync.Mutex
	name              string
	dataPath          string
	maxBytesPerFile   int64
	readPos           int64
	nextReadPos       int64
	writePos          int64
	readFileNum       int64
	writeFileNum      int64
	depth             int64
	readFile          *os.File
	writeFile         *os.File
	readChan          chan []byte
	exitChan          chan int
	writeContinueChan chan int
}

func NewDiskQueue(name string, dataPath string, maxBytesPerFile int64) *DiskQueue {
	diskQueue := DiskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		readChan:          make(chan []byte),
		exitChan:          make(chan int),
		writeContinueChan: make(chan int),
	}

	err := diskQueue.retrieveMetaData()
	if err != nil {
		log.Printf("WARNING: failed to retrieveMetaData() - %s", err.Error())
	}

	go diskQueue.readAheadPump()

	return &diskQueue
}

func (d *DiskQueue) Depth() int64 {
	return d.depth
}

func (d *DiskQueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *DiskQueue) Put(p []byte) error {
	err := d.writeOne(p)
	if err == nil {
		d.depth += 1
	}
	d.writeContinueChan <- 1
	return err
}

func (d *DiskQueue) Close() error {
	d.exitChan <- 1

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

func (d *DiskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	d.readMutex.Lock()
	defer d.readMutex.Unlock()

	if d.readPos > d.maxBytesPerFile {
		d.readFileNum++
		d.readPos = 0
		d.readFile.Close()
		d.readFile = nil

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

	totalBytes := 4 + msgSize

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.readFile, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	d.nextReadPos = d.readPos + int64(totalBytes)

	// log.Printf("DISK: read %d bytes - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d\n",
	// 	totalBytes, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return readBuf, nil
}

func (d *DiskQueue) writeOne(data []byte) error {
	var err error
	var buf bytes.Buffer

	d.writeMutex.Lock()
	defer d.writeMutex.Unlock()

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0
		d.writeFile.Close()
		d.writeFile = nil

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
	totalBytes := 4 + dataLen

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

	d.writePos += int64(totalBytes)

	// log.Printf("DISK: wrote %d bytes - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d\n",
	// 	totalBytes, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return nil
}

func (d *DiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "%d,%d\n%d,%d\n", &d.readFileNum, &d.readPos, &d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.nextReadPos = d.readPos

	log.Printf("DISK: retrieved meta data for (%s) - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d",
		d.name, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return nil
}

func (d *DiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fileName + ".tmp"

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d,%d\n%d,%d\n", d.readFileNum, d.readPos, d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	log.Printf("DISK: persisted meta data for (%s) - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d",
		d.name, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

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

// read the next message off disk to prime ReadChan
func (d *DiskQueue) readAheadPump() {
	var data []byte
	var err error
	for {
		if d.hasDataToRead() {
			if d.nextReadPos == d.readPos {
				data, err = d.readOne()
				if err != nil {
					// TODO: should this be fatal?
					log.Printf("ERROR: reading from diskqueue(%s) at %d of %s - %s", d.name, d.readPos, d.fileName(d.readFileNum), err.Error())
					continue
				}
			}
			select {
			case d.readChan <- data:
				d.readPos = d.nextReadPos
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
