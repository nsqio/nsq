package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"
	"unsafe"
)

const maxTimeout = time.Hour

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type ProtocolV2 struct {
	nsq.Protocol
}

func init() {
	protocols[string(nsq.MagicV2)] = &ProtocolV2{}
}

func (p *ProtocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	client := NewClientV2(conn)
	go p.messagePump(client)
	for {
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

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
		params := bytes.Split(line, separatorBytes)

		if *verbose {
			log.Printf("PROTOCOL(V2): [%s] %s", client, params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			context := ""
			if parentErr := err.(nsq.ChildError).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("ERROR: [%s] - %s%s", client, err.Error(), context)

			sendErr := p.Send(client, nsq.FrameTypeError, []byte(err.Error()))
			if sendErr != nil {
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
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	}
	return nil, nsq.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *ProtocolV2) messagePump(client *ClientV2) {
	var err error
	var buf bytes.Buffer
	var clientMsgChan chan *nsq.Message
	var subChannel *Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	subEventChan := client.SubEventChan
	heartbeatUpdateChan := client.HeartbeatUpdateChan
	outputBufferTimeoutUpdateChan := client.OutputBufferTimeoutUpdateChan
	flushed := true

	for {
		if subChannel == nil || !client.IsReadyForMessages() {
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
			clientMsgChan = subChannel.clientMsgChan
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			clientMsgChan = subChannel.clientMsgChan
			flusherChan = client.OutputBufferTimeout.C
		}

		select {
		case <-flusherChan:
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush
			err = p.Flush(client)
			if err != nil {
				goto exit
			}
			flushed = true
		case subChannel = <-subEventChan:
			// you can't subscribe anymore
			subEventChan = nil
		case <-client.ReadyStateChan:
		case timeout := <-outputBufferTimeoutUpdateChan:
			client.OutputBufferTimeout.Stop()
			if timeout > 0 {
				client.OutputBufferTimeout = time.NewTicker(timeout)
			}
			// you can't update output buffer timeout anymore
			outputBufferTimeoutUpdateChan = nil
		case interval := <-heartbeatUpdateChan:
			client.Heartbeat.Stop()
			if interval > 0 {
				client.Heartbeat = time.NewTicker(interval)
			}
			// you can't update heartbeat anymore
			heartbeatUpdateChan = nil
		case <-client.Heartbeat.C:
			err = p.Send(client, nsq.FrameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
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
	client.Heartbeat.Stop()
	client.OutputBufferTimeout.Stop()
	if subChannel != nil {
		subChannel.RemoveClient(client)
	}
	if err != nil {
		log.Printf("PROTOCOL(V2): [%s] messagePump error - %s", client, err.Error())
	}
}

func (p *ProtocolV2) IDENTIFY(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != nsq.StateInit {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	bodyLen, err := p.readLen(client)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > nsqd.options.maxBodySize {
		return nil, nsq.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, nsqd.options.maxBodySize))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData IdentifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	err = client.Identify(identifyData)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	resp := okBytes
	if identifyData.FeatureNegotiation {
		resp, err = json.Marshal(struct {
			MaxRdyCount   int64  `json:"max_rdy_count"`
			Version       string `json:"version"`
			MaxMsgTimeout int64  `json:"max_msg_timeout"`
			MsgTimeout    int64  `json:"msg_timeout"`
		}{
			MaxRdyCount:   nsqd.options.maxRdyCount,
			Version:       util.BINARY_VERSION,
			MaxMsgTimeout: int64(nsqd.options.maxMsgTimeout / time.Millisecond),
			MsgTimeout:    int64(nsqd.options.msgTimeout / time.Millisecond),
		})
		if err != nil {
			panic("should never happen")
		}
	}

	return resp, nil
}

func (p *ProtocolV2) SUB(client *ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateInit {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval < 0 {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name '%s' is not valid", topicName))
	}

	channelName := string(params[2])
	if !nsq.IsValidChannelName(channelName) {
		return nil, nsq.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name '%s' is not valid", channelName))
	}

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client)

	atomic.StoreInt32(&client.State, nsq.StateSubscribed)
	client.Channel = channel
	// update message pump
	client.SubEventChan <- channel

	return okBytes, nil
}

func (p *ProtocolV2) RDY(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == nsq.StateClosing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing", client)
		return nil, nil
	}

	if state != nsq.StateSubscribed {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := util.ByteToBase10(params[1])
		if err != nil {
			return nil, nsq.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > nsqd.options.maxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, nsqd.options.maxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *ProtocolV2) FIN(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	id := *(*nsq.MessageID)(unsafe.Pointer(&params[1][0]))
	err := client.Channel.FinishMessage(client, id)
	if err != nil {
		return nil, nsq.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", id, err.Error()))
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *ProtocolV2) REQ(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	id := *(*nsq.MessageID)(unsafe.Pointer(&params[1][0]))
	timeoutMs, err := util.ByteToBase10(params[2])
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > maxTimeout {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("REQ timeout %d out of range 0-%d", timeoutDuration, maxTimeout))
	}

	err = client.Channel.RequeueMessage(client, id, timeoutDuration)
	if err != nil {
		return nil, nsq.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", id, err.Error()))
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *ProtocolV2) CLS(client *ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateSubscribed {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
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
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name '%s' is not valid", topicName))
	}

	bodyLen, err := p.readLen(client)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	if int64(bodyLen) > nsqd.options.maxMessageSize {
		return nil, nsq.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, nsqd.options.maxMessageSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	return okBytes, nil
}

func (p *ProtocolV2) MPUB(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name '%s' is not valid", topicName))
	}

	bodyLen, err := p.readLen(client)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if int64(bodyLen) > nsqd.options.maxBodySize {
		return nil, nsq.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, nsqd.options.maxBodySize))
	}

	numMessages, err := p.readLen(client)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	messages := make([]*nsq.Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := p.readLen(client)
		if err != nil {
			return nil, nsq.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if int64(messageSize) > nsqd.options.maxMessageSize {
			return nil, nsq.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, nsqd.options.maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(client.Reader, msgBody)
		if err != nil {
			return nil, nsq.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		messages = append(messages, nsq.NewMessage(<-nsqd.idChan, msgBody))
	}

	topic := nsqd.GetTopic(topicName)

	// if we've made it this far we've validated all the input,
	// the only possible error is that the topic is exiting during
	// this next call (and no messages will be queued in that case)
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, nsq.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	return okBytes, nil
}

func (p *ProtocolV2) TOUCH(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, nsq.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	id := *(*nsq.MessageID)(unsafe.Pointer(&params[1][0]))
	err := client.Channel.TouchMessage(client, id)
	if err != nil {
		return nil, nsq.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", id, err.Error()))
	}

	return nil, nil
}

func (p *ProtocolV2) readLen(client *ClientV2) (int32, error) {
	client.lenSlice = client.lenSlice[0:]
	_, err := io.ReadFull(client.Reader, client.lenSlice)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(client.lenSlice)), nil
}
