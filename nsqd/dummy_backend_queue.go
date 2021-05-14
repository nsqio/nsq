package nsqd

type dummyBackendQueue struct {
	readChan chan []byte
}

func newDummyBackendQueue() BackendQueue {
	return &dummyBackendQueue{readChan: make(chan []byte)}
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

func tryQueueToMemoryChan(msgChan chan *Message, m *Message) error {
	for i := 0; i < 100; i++ {
		select {
		case msgChan <- m:
			return nil
		default:
			// Wait for retry...
		}

		select {
		case <- msgChan:
		default:
			// Wait for retry...
		}
	}

	return nil
}
