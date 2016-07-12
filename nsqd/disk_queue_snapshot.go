package nsqd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/absolute8511/nsq/internal/levellogger"
	"io"
	"os"
	"path"
	"sync"
)

type DiskQueueSnapshot struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	readPos           diskQueueOffset
	endPos            diskQueueOffset
	virtualReadOffset BackendOffset
	virtualEnd        BackendOffset

	sync.RWMutex

	readFrom string
	dataPath string
	exitFlag int32

	readFile *os.File
	reader   *bufio.Reader
}

// newDiskQueue instantiates a new instance of DiskQueueSnapshot, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func NewDiskQueueSnapshot(readFrom string, dataPath string, endInfo BackendQueueEnd) *DiskQueueSnapshot {
	d := DiskQueueSnapshot{
		readFrom: readFrom,
		dataPath: dataPath,
	}

	d.UpdateQueueEnd(endInfo)
	return &d
}

func (d *DiskQueueSnapshot) getCurrentFileEnd(offset diskQueueOffset) (int64, error) {
	curFileName := d.fileName(offset.FileNum)
	f, err := os.Stat(curFileName)
	if err != nil {
		return 0, err
	}
	return f.Size(), nil
}

// Put writes a []byte to the queue
func (d *DiskQueueSnapshot) UpdateQueueEnd(e BackendQueueEnd) {
	endPos, ok := e.(*diskQueueEndInfo)
	if !ok || endPos == nil {
		return
	}
	d.Lock()
	defer d.Unlock()

	if d.exitFlag == 1 {
		return
	}
	if d.readPos.FileNum > endPos.EndOffset.FileNum {
		d.readPos.FileNum = endPos.EndOffset.FileNum
		d.readPos.Pos = endPos.EndOffset.Pos
		d.virtualReadOffset = endPos.Offset()
	}
	if (d.readPos.FileNum == endPos.EndOffset.FileNum) && (d.readPos.Pos > endPos.EndOffset.Pos) {
		d.readPos.Pos = endPos.EndOffset.Pos
		d.virtualReadOffset = endPos.Offset()
	}
	d.endPos.Pos = endPos.EndOffset.Pos
	d.endPos.FileNum = endPos.EndOffset.FileNum
	d.virtualEnd = endPos.Offset()
}

// Close cleans up the queue and persists metadata
func (d *DiskQueueSnapshot) Close() error {
	return d.exit()
}

func (d *DiskQueueSnapshot) exit() error {
	d.Lock()

	d.exitFlag = 1
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	d.Unlock()
	return nil
}

func (d *DiskQueueSnapshot) getVirtualOffsetDistance(prev diskQueueOffset, next diskQueueOffset) (BackendOffset, error) {
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

func (d *DiskQueueSnapshot) stepOffset(cur diskQueueOffset, step int64, maxStep diskQueueOffset) (diskQueueOffset, error) {
	newOffset := cur
	var err error
	if cur.FileNum > maxStep.FileNum {
		return newOffset, fmt.Errorf("offset invalid: %v , %v", cur, maxStep)
	}
	if step == 0 {
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
		if step > diff {
			newOffset.FileNum++
			newOffset.Pos = 0
			if newOffset.GreatThan(&maxStep) {
				return newOffset, fmt.Errorf("offset invalid: %v , %v, need step: %v",
					newOffset, maxStep, step)
			}
			step -= diff
			if step == 0 {
				return newOffset, nil
			}
		} else {
			newOffset.Pos += step
			if newOffset.GreatThan(&maxStep) {
				return newOffset, fmt.Errorf("offset invalid: %v , %v, need step: %v",
					newOffset, maxStep, step)
			}
			return newOffset, nil
		}
	}
}

func (d *DiskQueueSnapshot) SeekTo(voffset BackendOffset) error {
	d.Lock()
	defer d.Unlock()
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}
	newPos := d.endPos
	var err error
	if voffset > d.virtualEnd {
		nsqLog.LogErrorf("internal skip error : skipping overflow to : %v, %v", voffset, d.virtualEnd)
		return ErrMoveOffsetInvalid
	} else if voffset == d.virtualEnd {
		newPos = d.endPos
	} else {
		newPos, err = d.stepOffset(d.readPos, int64(voffset-d.virtualReadOffset), d.endPos)
		if err != nil {
			nsqLog.LogErrorf("internal skip error : %v, skipping to : %v", err, voffset)
			return err
		}
	}

	nsqLog.LogDebugf("===snapshot read seek from %v, %v to: %v, %v", d.readPos,
		d.virtualReadOffset, newPos, voffset)
	d.readPos = newPos
	d.virtualReadOffset = voffset
	return nil
}

func (d *DiskQueueSnapshot) SeekToEnd() error {
	d.Lock()
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	d.readPos = d.endPos
	d.virtualReadOffset = d.virtualEnd
	d.Unlock()
	return nil
}

func (d *DiskQueueSnapshot) ReadRaw(size int32) ([]byte, error) {
	d.Lock()
	defer d.Unlock()

	result := make([]byte, size)
	readOffset := int32(0)
	var stat os.FileInfo
	var err error

	for readOffset < size {

	CheckFileOpen:
		if d.readFile == nil {
			curFileName := d.fileName(d.readPos.FileNum)
			d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
			if err != nil {
				return result, err
			}
			nsqLog.LogDebugf("DISKQUEUE snapshot(%s): readRaw() opened %s", d.readFrom, curFileName)
			if d.readPos.Pos > 0 {
				_, err = d.readFile.Seek(d.readPos.Pos, 0)
				if err != nil {
					d.readFile.Close()
					d.readFile = nil
					return result, err
				}
			}
			d.reader = bufio.NewReader(d.readFile)
		}
		stat, err = d.readFile.Stat()
		if err != nil {
			return result, err
		}
		if d.readPos.FileNum < d.endPos.FileNum {
			if d.readPos.Pos >= stat.Size() {
				d.readPos.FileNum++
				d.readPos.Pos = 0
				nsqLog.Logf("DISKQUEUE snapshot(%s): readRaw() read end, try next: %v",
					d.readFrom, d.readPos.FileNum)
				d.readFile.Close()
				d.readFile = nil
				goto CheckFileOpen
			}
		}

		fileLeft := stat.Size() - d.readPos.Pos
		if d.readPos.FileNum == d.endPos.FileNum && fileLeft == 0 {
			return result[:readOffset], io.EOF
		}
		currentRead := int64(size - readOffset)
		if fileLeft < currentRead {
			currentRead = fileLeft
		}
		_, err = io.ReadFull(d.reader, result[readOffset:int64(readOffset)+currentRead])
		if err != nil {
			d.readFile.Close()
			d.readFile = nil
			return result, err
		}

		oldPos := d.readPos
		d.readPos.Pos = d.readPos.Pos + currentRead
		readOffset += int32(currentRead)
		d.virtualReadOffset += BackendOffset(currentRead)
		if nsqLog.Level() >= levellogger.LOG_DETAIL {
			nsqLog.LogDebugf("===snapshot read move forward: %v, %v, %v", oldPos,
				d.virtualReadOffset, d.readPos)
		}

		isEnd := false
		if d.readPos.FileNum < d.endPos.FileNum {
			isEnd = d.readPos.Pos >= stat.Size()
		}
		if isEnd {
			if d.readFile != nil {
				d.readFile.Close()
				d.readFile = nil
			}
			d.readPos.FileNum++
			d.readPos.Pos = 0
		}
	}
	return result, nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *DiskQueueSnapshot) ReadOne() ReadResult {
	d.Lock()
	defer d.Unlock()

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

		nsqLog.Logf("DISKQUEUE(%s): readOne() opened %s", d.readFrom, curFileName)

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
	if d.readPos.FileNum < d.endPos.FileNum {
		stat, result.Err = d.readFile.Stat()
		if result.Err != nil {
			return result
		}
		if d.readPos.Pos >= stat.Size() {
			d.readPos.FileNum++
			d.readPos.Pos = 0
			nsqLog.Logf("DISKQUEUE(%s): readOne() read end, try next: %v",
				d.readFrom, d.readPos.FileNum)
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

	oldPos := d.readPos
	d.readPos.Pos = d.readPos.Pos + totalBytes
	d.virtualReadOffset += BackendOffset(totalBytes)
	nsqLog.LogDebugf("=== read move forward: %v, %v, %v", oldPos,
		d.virtualReadOffset, d.readPos)

	isEnd := false
	if d.readPos.FileNum < d.endPos.FileNum {
		stat, result.Err = d.readFile.Stat()
		if result.Err == nil {
			isEnd = d.readPos.Pos >= stat.Size()
		} else {
			return result
		}
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

func (d *DiskQueueSnapshot) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.readFrom, fileNum)
}
