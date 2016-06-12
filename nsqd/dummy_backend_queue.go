package nsqd

type dummyBackendQueue struct {
	readChan       chan []byte
	readResultChan chan ReadResult
}

type dummyBackendQueueReader struct {
	dummyBackendQueue
}

type dummyBackendQueueWriter struct {
	dummyBackendQueue
}

func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readChan: make(chan []byte)}
}

func newDummyBackendQueueReader() BackendQueueReader {
	return &dummyBackendQueueReader{}
}

func newDummyBackendQueueWriter() BackendQueueWriter {
	return &dummyBackendQueueWriter{}
}

func (d *dummyBackendQueue) Put([]byte) error {
	return nil
}

func (d *dummyBackendQueue) ReadChan() chan []byte {
	return d.readChan
}

func (d *dummyBackendQueue) Close() error {
	return nil
}

func (d *dummyBackendQueue) Delete() error {
	return nil
}

func (d *dummyBackendQueue) GetQueueReadEnd() BackendQueueEnd {
	return nil
}

func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *dummyBackendQueue) Empty() error {
	return nil
}

func (d *dummyBackendQueueWriter) Put([]byte) (BackendOffset, int32, int64, error) {
	return 0, 0, 0, nil
}
func (d *dummyBackendQueueWriter) GetQueueReadEnd() BackendQueueEnd {
	return nil
}
func (d *dummyBackendQueueWriter) GetQueueWriteEnd() BackendQueueEnd {
	return nil
}
func (d *dummyBackendQueueWriter) ResetWriteEnd(offset BackendOffset, totalCnt int64) error {
	return nil
}

func (d *dummyBackendQueueWriter) RollbackWrite(offset BackendOffset, diffCnt uint64) error {
	return nil
}

func (d *dummyBackendQueueWriter) Flush() error {
	return nil
}

func (d *dummyBackendQueueReader) TryReadOne() (ReadResult, bool) {
	return ReadResult{}, true
}

func (d *dummyBackendQueueReader) ConfirmRead(offset BackendOffset) error {
	return nil
}

func (d *dummyBackendQueueReader) ResetReadToConfirmed() (BackendOffset, error) {
	return 0, nil
}

func (d *dummyBackendQueueReader) SkipReadToOffset(offset BackendOffset) (BackendOffset, error) {
	return 0, nil
}

func (d *dummyBackendQueue) UpdateQueueEnd(end BackendQueueEnd, force bool) (bool, error) {
	return true, nil
}
