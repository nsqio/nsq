package nsq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

const (
	// 100mb
	maxBytesPerFile = 104857600
)

type DiskQueue struct {
	name              string
	dataPath          string
	readPos           int64
	writePos          int64
	readFileNum       int64
	writeFileNum      int64
	depth             int64
	readFile          *os.File
	writeFile         *os.File
	readChan          chan int
	exitChan          chan int
	readContinueChan  chan int
	writeContinueChan chan int
}

func NewDiskQueue(name string, dataPath string) *DiskQueue {
	diskQueue := DiskQueue{
		name:              name,
		dataPath:          dataPath,
		readChan:          make(chan int),
		exitChan:          make(chan int),
		readContinueChan:  make(chan int),
		writeContinueChan: make(chan int),
	}

	err := diskQueue.retrieveMetaData()
	if err != nil {
		log.Printf("WARNING: failed to retrieveMetaData() - %s", err.Error())
	}

	go diskQueue.router()

	return &diskQueue
}

func (d *DiskQueue) Depth() int64 {
	return d.depth
}

func (d *DiskQueue) ReadReadyChan() chan int {
	return d.readChan
}

func (d *DiskQueue) Get() ([]byte, error) {
	buf, err := d.readOne()
	if err == nil {
		d.depth -= 1
	}
	d.readContinueChan <- 1
	return buf, err
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

	if d.readPos > maxBytesPerFile {
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
		return nil, err
	}

	d.readPos += int64(totalBytes)

	// log.Printf("DISK: read %d bytes - readFileNum=%d writeFileNum=%d readPos=%d writePos=%d\n",
	// 	totalBytes, d.readFileNum, d.writeFileNum, d.readPos, d.writePos)

	return readBuf, nil
}

func (d *DiskQueue) writeOne(data []byte) error {
	var err error
	var buf bytes.Buffer

	if d.writePos > maxBytesPerFile {
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

// Router selects from the input and output channel
// ensuring that we're either reading from or writing to disk
func (d *DiskQueue) router() {
	for {
		if d.hasDataToRead() {
			select {
			// in order to read only when we actually want a message we use
			// readChan to wrap outChan
			case d.readChan <- 1:
				<-d.readContinueChan
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
