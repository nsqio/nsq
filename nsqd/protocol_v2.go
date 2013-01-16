package main

import (
	"../nsq"
	"../util"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
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
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, []byte(" "))

		if *verbose {
			log.Printf("PROTOCOL(V2): [%s] %s", client, params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			log.Printf("ERROR: CLIENT(%s) - %s", client, err.(nsq.DescriptiveError).Description())
			err = p.Send(client, nsq.FrameTypeError, []byte(err.Error()))
			if err != nil {
				break
			}
			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*nsq.FatalClientErr); ok {
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

func (p *ProtocolV2) SendMessage(client *ClientV2, msg *nsq.Message, buf *bytes.Buffer) error {
	if *verbose {
		log.Printf("PROTOCOL(V2): writing msg(%s) to client(%s) - %s",
			msg.Id, client, msg.Body)
	}

	buf.Reset()
	err := msg.Write(buf)
	if err != nil {
		return err
	}

	client.Channel.StartInFlightTimeout(msg, client)
	client.SendingMessage()

	err = p.Send(client, nsq.FrameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *ProtocolV2) Send(client *ClientV2, frameType int32, data []byte) error {
	client.Lock()
	defer client.Unlock()

	client.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := nsq.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		return err
	}

	if frameType != nsq.FrameTypeMessage {
		err = client.Writer.Flush()
	}

	return err
}

func (p *ProtocolV2) Flush(client *ClientV2) error {
	client.Lock()
	defer client.Unlock()

	if client.Writer.Buffered() > 0 {
		client.SetWriteDeadline(time.Now().Add(time.Second))
		return client.Writer.Flush()
	}

	return nil
}

func (p *ProtocolV2) Exec(client *ClientV2, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("IDENTIFY")):
		return p.IDENTIFY(client, params)
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
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	}
	return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *ProtocolV2) messagePump(client *ClientV2) {
	var err error
	var buf bytes.Buffer
	var clientMsgChan chan *nsq.Message
	var flusherChan <-chan time.Time

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	// NOTE: `flusher` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	flusher := time.NewTicker(5 * time.Millisecond)
	heartbeat := time.NewTicker(nsqd.options.clientTimeout / 2)
	flushed := true

	for {
		if !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			clientMsgChan = nil
			flusherChan = nil
			// force flush
			err = p.Flush(client)
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			clientMsgChan = client.Channel.clientMsgChan
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			clientMsgChan = client.Channel.clientMsgChan
			flusherChan = flusher.C
		}

		select {
		case <-flusherChan:
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush
			err := p.Flush(client)
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan:
		case <-heartbeat.C:
			err = p.Send(client, nsq.FrameTypeResponse, []byte("_heartbeat_"))
			if err != nil {
				log.Printf("PROTOCOL(V2): error sending heartbeat - %s", err.Error())
			}
		case msg, ok := <-clientMsgChan:
			if !ok {
				goto exit
			}

			err = p.SendMessage(client, msg, &buf)
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	log.Printf("PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeat.Stop()
	flusher.Stop()
	client.Channel.RemoveClient(client)
	if err != nil {
		log.Printf("PROTOCOL(V2): messagePump error - %s", err.Error())
	}
}

func (p *ProtocolV2) IDENTIFY(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != nsq.StateInit {
		return nil, nsq.NewFatalClientErr("E_INVALID", "client not initialized")
	}

	var bodyLen int32
	err = binary.Read(client.Reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	if int64(bodyLen) > nsqd.options.maxBodySize {
		return nil, nsq.NewFatalClientErr("E_BODY_TOO_BIG",
			fmt.Sprintf("body too big %d > %d", bodyLen, nsqd.options.maxBodySize))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	// body is a json structure with producer information
	clientInfo := struct {
		ShortId string `json:"short_id"`
		LongId  string `json:"long_id"`
	}{}
	err = json.Unmarshal(body, &clientInfo)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	client.ShortIdentifier = clientInfo.ShortId
	client.LongIdentifier = clientInfo.LongId

	return nil, nil
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
		return nil, nsq.NewClientErr("E_BAD_TOPIC",
			fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	channelName := string(params[2])
	if !nsq.IsValidChannelName(channelName) {
		return nil, nsq.NewClientErr("E_BAD_CHANNEL",
			fmt.Sprintf("channel name '%s' is not valid", channelName))
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
	state := atomic.LoadInt32(&client.State)

	if state == nsq.StateClosing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): ignoring RDY after CLS in state ClientStateV2Closing")
		return nil, nil
	}

	if state != nsq.StateSubscribed {
		return nil, nsq.NewClientErr("E_INVALID", "client not subscribed")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := util.ByteToBase10(params[1])
		if err != nil {
			return nil, nsq.NewClientErr("E_INVALID",
				fmt.Sprintf("could not parse RDY count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 {
		log.Printf("ERROR: client(%s) sent invalid ready count %d < 0", client, count)
		count = 0
	} else if count > nsq.MaxReadyCount {
		log.Printf("ERROR: client(%s) sent invalid ready count %d > %d", client, count, nsq.MaxReadyCount)
		count = nsq.MaxReadyCount
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *ProtocolV2) FIN(client *ClientV2, params [][]byte) ([]byte, error) {
	var id nsq.MessageID

	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.NewClientErr("E_INVALID", "cannot finish in current state")
	}

	if len(params) < 2 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	copy(id[:], params[1])
	err := client.Channel.FinishMessage(client, id)
	if err != nil {
		return nil, nsq.NewClientErr("E_FIN_FAILED", err.Error())
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *ProtocolV2) REQ(client *ClientV2, params [][]byte) ([]byte, error) {
	var id nsq.MessageID

	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.NewClientErr("E_INVALID", "cannot re-queue in current state")
	}

	if len(params) < 3 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	copy(id[:], params[1])
	timeoutMs, err := util.ByteToBase10(params[2])
	if err != nil {
		return nil, nsq.NewClientErr("E_INVALID",
			fmt.Sprintf("could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > maxTimeout {
		return nil, nsq.NewClientErr("E_INVALID",
			fmt.Sprintf("timeout %d out of range", timeoutDuration))
	}

	err = client.Channel.RequeueMessage(client, id, timeoutDuration)
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
	var bodyLen int32

	if len(params) < 2 {
		return nil, nsq.NewFatalClientErr("E_MISSING_PARAMS", "insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewFatalClientErr("E_BAD_TOPIC",
			fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	err = binary.Read(client.Reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	if int64(bodyLen) > nsqd.options.maxMessageSize {
		return nil, nsq.NewFatalClientErr("E_MSG_TOO_BIG",
			fmt.Sprintf("message too big %d > %d", bodyLen, nsqd.options.maxMessageSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_PUT_FAILED", err.Error())
	}

	return []byte("OK"), nil
}

func (p *ProtocolV2) MPUB(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error
	var bodyLen int32
	var numMessages int32
	var messageSize int32

	if len(params) < 2 {
		return nil, nsq.NewFatalClientErr("E_MISSING_PARAMS", "insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewFatalClientErr("E_BAD_TOPIC",
			fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	err = binary.Read(client.Reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	if int64(bodyLen) > nsqd.options.maxBodySize {
		return nil, nsq.NewFatalClientErr("E_BODY_TOO_BIG",
			fmt.Sprintf("body too big %d > %d", bodyLen, nsqd.options.maxBodySize))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	buf := bytes.NewBuffer(body)
	err = binary.Read(buf, binary.BigEndian, &numMessages)
	if err != nil {
		return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
	}

	topic := nsqd.GetTopic(topicName)
	for i := int32(0); i < numMessages; i++ {
		err = binary.Read(buf, binary.BigEndian, &messageSize)
		if err != nil {
			return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
		}

		if int64(messageSize) > nsqd.options.maxMessageSize {
			return nil, nsq.NewFatalClientErr("E_MSG_TOO_BIG",
				fmt.Sprintf("message too big %d > %d", messageSize, nsqd.options.maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(buf, msgBody)
		if err != nil {
			return nil, nsq.NewFatalClientErr("E_BAD_BODY", err.Error())
		}

		msg := nsq.NewMessage(<-nsqd.idChan, msgBody)
		err := topic.PutMessage(msg)
		if err != nil {
			return nil, nsq.NewFatalClientErr("E_PUT_FAILED", err.Error())
		}
	}

	return []byte("OK"), nil
}
