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

func (d *dummyBackendQueue) Depth() int64 {
	return int64(0)
}

func (d *dummyBackendQueue) Empty() error {
	return nil
}

func (d *dummyBackendQueueWriter) Put([]byte) (BackendQueueEnd, error) {
	return nil, nil
}
func (d *dummyBackendQueueWriter) GetQueueReadEnd() BackendQueueEnd {
	return nil
}
func (d *dummyBackendQueueWriter) Flush() error {
	return nil
}

func (d *dummyBackendQueueReader) ReadChan() <-chan ReadResult {
	return d.readResultChan
}

func (d *dummyBackendQueueReader) ConfirmRead(offset BackendOffset) error {
	return nil
}

func (d *dummyBackendQueue) UpdateQueueEnd(end BackendQueueEnd) {
}
