package nsq

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"time"
)

type Message struct {
	Uuid      []byte
	Body      []byte
	Timestamp int64
	Retries   uint16
	timerChan chan int
}

func NewMessage(uuid []byte, body []byte) *Message {
	return &Message{
		Uuid:      uuid,
		Body:      body,
		Timestamp: time.Now().Unix(),
		Retries:   0,
		timerChan: make(chan int),
	}
}

func (m *Message) EndTimer() {
	select {
	case m.timerChan <- 1:
	default:
	}
}

func (m *Message) ShouldRequeue(sleepMS int) bool {
	timer := time.NewTimer(time.Duration(sleepMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-m.timerChan:
		return false
	}

	return true
}

func DecodeMessage(byteBuf []byte) (*Message, error) {
	var timestamp int64
	var retries uint16

	buf := bytes.NewBuffer(byteBuf)

	err := binary.Read(buf, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	err = binary.Read(buf, binary.BigEndian, &retries)
	if err != nil {
		return nil, err
	}

	uuid := make([]byte, 16)
	_, err = io.ReadFull(buf, uuid)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(buf)
	if err != nil {
		return nil, err
	}

	msg := NewMessage(uuid, body)
	msg.Timestamp = timestamp
	msg.Retries = retries

	return msg, nil
}

func (m *Message) Encode() ([]byte, error) {
	var buf bytes.Buffer

	err := binary.Write(&buf, binary.BigEndian, &m.Timestamp)
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, binary.BigEndian, &m.Retries)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(m.Uuid)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(m.Body)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
