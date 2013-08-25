package nsq

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Writer struct {
	net.Conn

	WriteTimeout      time.Duration
	Addr              string
	HeartbeatInterval time.Duration
	ShortIdentifier   string
	LongIdentifier    string

	transactionChan chan *WriterTransaction
	dataChan        chan []byte
	transactions    []*WriterTransaction
	state           int32
	stopFlag        int32
	exitChan        chan int
	closeChan       chan int
	wg              sync.WaitGroup
}

type WriterTransaction struct {
	cmd       *Command
	doneChan  chan *WriterTransaction
	FrameType int32
	Data      []byte
	Error     error
	Args      []interface{}
}

var ErrNotConnected = errors.New("not connected")
var ErrStopped = errors.New("stopped")

func NewWriter(addr string) *Writer {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	return &Writer{
		transactionChan: make(chan *WriterTransaction),
		exitChan:        make(chan int),
		closeChan:       make(chan int),
		dataChan:        make(chan []byte),

		// can be overriden before connecting
		Addr:              addr,
		WriteTimeout:      time.Second,
		HeartbeatInterval: DefaultClientTimeout / 2,
		ShortIdentifier:   strings.Split(hostname, ".")[0],
		LongIdentifier:    hostname,
	}
}

func (w *Writer) String() string {
	return w.Addr
}

func (w *Writer) Stop() {
	if !atomic.CompareAndSwapInt32(&w.stopFlag, 0, 1) {
		return
	}
	w.close()
	w.wg.Wait()
}

func (w *Writer) PublishAsync(topic string, body []byte, doneChan chan *WriterTransaction, args ...interface{}) error {
	return w.sendCommandAsync(Publish(topic, body), doneChan, args)
}

func (w *Writer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *WriterTransaction, args ...interface{}) error {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return err
	}
	return w.sendCommandAsync(cmd, doneChan, args)
}

func (w *Writer) Publish(topic string, body []byte) (int32, []byte, error) {
	return w.sendCommand(Publish(topic, body))
}

func (w *Writer) MultiPublish(topic string, body [][]byte) (int32, []byte, error) {
	cmd, err := MultiPublish(topic, body)
	if err != nil {
		return -1, nil, err
	}
	return w.sendCommand(cmd)
}

func (w *Writer) sendCommand(cmd *Command) (int32, []byte, error) {
	doneChan := make(chan *WriterTransaction)
	err := w.sendCommandAsync(cmd, doneChan, nil)
	if err != nil {
		close(doneChan)
		return -1, nil, err
	}
	t := <-doneChan
	return t.FrameType, t.Data, t.Error
}

func (w *Writer) sendCommandAsync(cmd *Command, doneChan chan *WriterTransaction, args []interface{}) error {
	if atomic.LoadInt32(&w.state) != StateConnected {
		err := w.connect()
		if err != nil {
			return err
		}
	}
	t := &WriterTransaction{
		cmd:       cmd,
		doneChan:  doneChan,
		FrameType: -1,
		Args:      args,
	}
	select {
	case w.transactionChan <- t:
	case <-w.exitChan:
		return ErrStopped
	}
	return nil
}

func (w *Writer) connect() error {
	if atomic.LoadInt32(&w.stopFlag) == 1 {
		return ErrStopped
	}

	if !atomic.CompareAndSwapInt32(&w.state, StateInit, StateConnected) {
		return nil
	}

	conn, err := net.DialTimeout("tcp", w.Addr, time.Second*5)
	if err != nil {
		log.Printf("ERROR: [%s] failed to dial %s - %s", w, w.Addr, err)
		atomic.StoreInt32(&w.state, StateInit)
		return err
	}

	w.closeChan = make(chan int)
	w.Conn = conn

	w.SetWriteDeadline(time.Now().Add(w.WriteTimeout))
	_, err = w.Write(MagicV2)
	if err != nil {
		log.Printf("ERROR: [%s] failed to write magic - %s", w, err)
		w.close()
		return err
	}

	ci := make(map[string]interface{})
	ci["short_id"] = w.ShortIdentifier
	ci["long_id"] = w.LongIdentifier
	ci["heartbeat_interval"] = int64(w.HeartbeatInterval / time.Millisecond)
	ci["feature_negotiation"] = true
	cmd, err := Identify(ci)
	if err != nil {
		log.Printf("ERROR: [%s] failed to create IDENTIFY command - %s", w, err)
		w.close()
		return err
	}

	w.SetWriteDeadline(time.Now().Add(w.WriteTimeout))
	err = cmd.Write(w)
	if err != nil {
		log.Printf("ERROR: [%s] failed to write IDENTIFY - %s", w, err)
		w.close()
		return err
	}

	w.SetReadDeadline(time.Now().Add(w.HeartbeatInterval * 2))
	resp, err := ReadResponse(w)
	if err != nil {
		log.Printf("ERROR: [%s] failed to read IDENTIFY response - %s", w, err)
		w.close()
		return err
	}

	frameType, data, err := UnpackResponse(resp)
	if err != nil {
		log.Printf("ERROR: [%s] failed to unpack IDENTIFY response - %s", w, resp)
		w.close()
		return err
	}

	if frameType == FrameTypeError {
		return errors.New(string(data))
	}

	w.wg.Add(1)
	go w.readLoop()

	w.wg.Add(1)
	go w.messageRouter()

	return nil
}

func (w *Writer) close() {
	if !atomic.CompareAndSwapInt32(&w.state, StateConnected, StateDisconnected) {
		return
	}
	close(w.closeChan)
	w.Conn.Close()
	go func() {
		w.wg.Wait()
		atomic.StoreInt32(&w.state, StateInit)
	}()
}

func (w *Writer) messageRouter() {
	defer w.transactionCleanup()

	for {
		select {
		case t := <-w.transactionChan:
			w.transactions = append(w.transactions, t)
			w.SetWriteDeadline(time.Now().Add(w.WriteTimeout))
			err := t.cmd.Write(w.Conn)
			if err != nil {
				log.Printf("ERROR: [%s] failed writing %s", w, err)
				w.close()
				goto exit
			}
		case buf := <-w.dataChan:
			frameType, data, err := UnpackResponse(buf)
			if err != nil {
				log.Printf("ERROR: [%s] failed (%s) unpacking response %d %s", w, err, frameType, data)
				w.close()
				goto exit
			}

			if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
				log.Printf("[%s] heartbeat received", w)
				w.SetWriteDeadline(time.Now().Add(w.WriteTimeout))
				err := Nop().Write(w.Conn)
				if err != nil {
					log.Printf("ERROR: [%s] failed sending heartbeat - %s", w, err)
					w.close()
					goto exit
				}
				continue
			}

			t := w.transactions[0]
			w.transactions = w.transactions[1:]
			t.FrameType = frameType
			t.Data = data
			t.Error = err
			t.doneChan <- t
		case <-w.closeChan:
			goto exit
		}
	}

exit:
	w.wg.Done()
	log.Printf("[%s] exiting messageRouter()", w)
}

func (w *Writer) transactionCleanup() {
	for _, t := range w.transactions {
		t.Error = ErrNotConnected
		t.doneChan <- t
	}
	w.transactions = w.transactions[:0]
}

func (w *Writer) readLoop() {
	rbuf := bufio.NewReader(w.Conn)
	for {
		w.SetReadDeadline(time.Now().Add(w.HeartbeatInterval * 2))
		resp, err := ReadResponse(rbuf)
		if err != nil {
			log.Printf("ERROR: [%s] reading response %s", w, err)
			if !strings.Contains(err.Error(), "use of closed network connection") {
				w.close()
			}
			goto exit
		}
		select {
		case w.dataChan <- resp:
		case <-w.closeChan:
			goto exit
		}
	}

exit:
	w.wg.Done()
	log.Printf("[%s] exiting readLoop()", w)
}
