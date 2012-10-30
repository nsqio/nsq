package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

const maxTimeout = time.Hour

type ProtocolV2 struct {
	nsq.Protocol
}

func init() {
	// BigEndian client byte sequence "  V2"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.MagicV2))
	binary.Read(buf, binary.BigEndian, &magicInt)
	protocols[magicInt] = &ProtocolV2{}
}

func (p *ProtocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte

	client := NewClientV2(conn)
	atomic.StoreInt32(&client.State, nsq.StateInit)

	err = nil
	client.Reader = bufio.NewReader(client)
	for {
		client.SetReadDeadline(time.Now().Add(nsqd.options.clientTimeout))
		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, []byte(" "))

		if *verbose {
			log.Printf("PROTOCOL(V2): [%s] %s", client, params[0])
		}

		response, err := p.Exec(client, params)
		if err != nil {
			log.Printf("ERROR: CLIENT(%s) - %s", client, err.(*nsq.ClientErr).Description())
			err = p.Send(client, nsq.FrameTypeError, []byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, nsq.FrameTypeResponse, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("PROTOCOL(V2): [%s] exiting ioloop", client)
	// TODO: gracefully send clients the close signal
	conn.Close()
	close(client.ExitChan)

	return err
}

func (p *ProtocolV2) Send(client *ClientV2, frameType int32, data []byte) error {
	client.Lock()
	defer client.Unlock()

	client.frameBuf.Reset()
	err := nsq.Frame(&client.frameBuf, frameType, data)
	if err != nil {
		return err
	}

	attempts := 0
	for {
		client.SetWriteDeadline(time.Now().Add(time.Second))
		_, err := nsq.SendResponse(client, client.frameBuf.Bytes())
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				attempts++
				if attempts == 3 {
					return errors.New("timed out trying to send to client")
				}
				continue
			}
			return err
		}
		break
	}

	return nil
}

func (p *ProtocolV2) Exec(client *ClientV2, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	}
	return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *ProtocolV2) sendHeartbeat(client *ClientV2) error {
	err := p.Send(client, nsq.FrameTypeResponse, []byte("_heartbeat_"))
	if err != nil {
		return err
	}

	return nil
}

func (p *ProtocolV2) messagePump(client *ClientV2) {
	var err error
	var buf bytes.Buffer
	var c chan *nsq.Message

	heartbeat := time.NewTicker(nsqd.options.clientTimeout / 2)

	// ReadyStateChan has a buffer of 1 to guarantee that in the event
	// there is a race the state update is not lost
	for {
		if client.IsReadyForMessages() {
			c = client.Channel.clientMsgChan
		} else {
			c = nil
		}

		select {
		case <-client.ReadyStateChan:
		case <-heartbeat.C:
			err = p.sendHeartbeat(client)
			if err != nil {
				log.Printf("PROTOCOL(V2): error sending heartbeat - %s", err.Error())
			}
		case msg, ok := <-c:
			if !ok {
				goto exit
			}

			if *verbose {
				log.Printf("PROTOCOL(V2): writing msg(%s) to client(%s) - %s",
					msg.Id, client, msg.Body)
			}

			buf.Reset()
			err = msg.Write(&buf)
			if err != nil {
				goto exit
			}

			client.Channel.StartInFlightTimeout(msg, client)
			client.SendingMessage()

			err = p.Send(client, nsq.FrameTypeMessage, buf.Bytes())
			if err != nil {
				goto exit
			}
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	log.Printf("PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeat.Stop()
	client.Channel.RemoveClient(client)
	if err != nil {
		log.Printf("PROTOCOL(V2): messagePump error - %s", err.Error())
	}
}

func (p *ProtocolV2) SUB(client *ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateInit {
		return nil, nsq.NewClientErr("E_INVALID", "client not initialized")
	}

	if len(params) < 3 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewClientErr("E_BAD_TOPIC", fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	channelName := string(params[2])
	if !nsq.IsValidChannelName(channelName) {
		return nil, nsq.NewClientErr("E_BAD_CHANNEL", fmt.Sprintf("channel name '%s' is not valid", channelName))
	}

	if len(params) == 5 {
		client.ShortIdentifier = string(params[3])
		client.LongIdentifier = string(params[4])
	}

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client)

	client.Channel = channel
	atomic.StoreInt32(&client.State, nsq.StateSubscribed)

	go p.messagePump(client)

	return nil, nil
}

func (p *ProtocolV2) RDY(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error

	state := atomic.LoadInt32(&client.State)

	if state == nsq.StateClosing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): ignoring RDY after CLS in state ClientStateV2Closing")
		return nil, nil
	}

	if state != nsq.StateSubscribed {
		return nil, nsq.NewClientErr("E_INVALID", "client not subscribed")
	}

	count := 1
	if len(params) > 1 {
		count, err = strconv.Atoi(string(params[1]))
		if err != nil {
			return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("could not parse RDY count %s", params[1]))
		}
	}

	if count > nsq.MaxReadyCount {
		log.Printf("ERROR: client(%s) sent ready count %d > %d", client, count, nsq.MaxReadyCount)
		count = nsq.MaxReadyCount
	}

	client.SetReadyCount(int64(count))

	return nil, nil
}

func (p *ProtocolV2) FIN(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.NewClientErr("E_INVALID", "cannot finish in current state")
	}

	if len(params) < 2 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	idStr := params[1]
	err := client.Channel.FinishMessage(client, idStr)
	if err != nil {
		return nil, nsq.NewClientErr("E_FIN_FAILED", err.Error())
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *ProtocolV2) REQ(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.NewClientErr("E_INVALID", "cannot re-queue in current state")
	}

	if len(params) < 3 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	idStr := params[1]
	timeoutMs, err := strconv.Atoi(string(params[2]))
	if err != nil {
		return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > maxTimeout {
		return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("timeout %d out of range", timeoutDuration))
	}

	err = client.Channel.RequeueMessage(client, idStr, timeoutDuration)
	if err != nil {
		return nil, nsq.NewClientErr("E_REQ_FAILED", err.Error())
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *ProtocolV2) CLS(client *ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateSubscribed {
		return nil, nsq.NewClientErr("E_INVALID", "client not subscribed")
	}

	if len(params) > 1 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

func (p *ProtocolV2) NOP(client *ClientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

func (p *ProtocolV2) PUB(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewClientErr("E_BAD_TOPIC", fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	var bodyLen int32
	err = binary.Read(client.Reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, nsq.NewClientErr("E_PUT_FAILED", err.Error())
	}

	return []byte("OK"), nil
}
