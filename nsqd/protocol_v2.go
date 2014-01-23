package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"io"
	"log"
	"math"
	"math/rand"
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
	context *Context
}

func (p *ProtocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	clientID := atomic.AddInt64(&p.context.nsqd.clientIDSequence, 1)
	client := NewClientV2(clientID, conn, p.context)
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
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("ERROR: [%s] - %s%s", client, err.Error(), context)

			sendErr := p.Send(client, nsq.FrameTypeError, []byte(err.Error()))
			if sendErr != nil {
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*util.FatalClientErr); ok {
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
	conn.Close()
	close(client.ExitChan)
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

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

	err = p.Send(client, nsq.FrameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *ProtocolV2) Send(client *ClientV2, frameType int32, data []byte) error {
	client.Lock()

	client.SetWriteDeadline(time.Now().Add(time.Second))
	_, err := util.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.Unlock()
		return err
	}

	if frameType != nsq.FrameTypeMessage {
		err = client.Flush()
	}

	client.Unlock()

	return err
}

func (p *ProtocolV2) Exec(client *ClientV2, params [][]byte) ([]byte, error) {
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("IDENTIFY")):
		return p.IDENTIFY(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	}
	return nil, util.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
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
	var sampleRate int32

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	subEventChan := client.SubEventChan
	heartbeatUpdateChan := client.HeartbeatUpdateChan
	outputBufferTimeoutUpdateChan := client.OutputBufferTimeoutUpdateChan
	sampleRateUpdateChan := client.SampleRateUpdateChan
	flushed := true

	for {
		if subChannel == nil || !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			clientMsgChan = nil
			flusherChan = nil
			// force flush
			client.Lock()
			err = client.Flush()
			client.Unlock()
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
			client.Lock()
			err = client.Flush()
			client.Unlock()
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
		case sampleRate = <-sampleRateUpdateChan:
			sampleRateUpdateChan = nil
		case msg, ok := <-clientMsgChan:
			if !ok {
				goto exit
			}

			// if we are sampling, do so here
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}

			subChannel.StartInFlightTimeout(msg, client.ID)
			client.SendingMessage()
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
	if err != nil {
		log.Printf("PROTOCOL(V2): [%s] messagePump error - %s", client, err.Error())
	}
}

func (p *ProtocolV2) IDENTIFY(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != nsq.StateInit {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.context.nsqd.options.MaxBodySize {
		return nil, util.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.context.nsqd.options.MaxBodySize))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData IdentifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	err = client.Identify(identifyData)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.context.nsqd.tlsConfig != nil && identifyData.TLSv1
	deflate := p.context.nsqd.options.DeflateEnabled && identifyData.Deflate
	deflateLevel := 0
	if deflate {
		if identifyData.DeflateLevel <= 0 {
			deflateLevel = 6
		}
		deflateLevel = int(math.Min(float64(deflateLevel), float64(p.context.nsqd.options.MaxDeflateLevel)))
	}
	snappy := p.context.nsqd.options.SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, util.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount     int64  `json:"max_rdy_count"`
		Version         string `json:"version"`
		MaxMsgTimeout   int64  `json:"max_msg_timeout"`
		MsgTimeout      int64  `json:"msg_timeout"`
		TLSv1           bool   `json:"tls_v1"`
		Deflate         bool   `json:"deflate"`
		DeflateLevel    int    `json:"deflate_level"`
		MaxDeflateLevel int    `json:"max_deflate_level"`
		Snappy          bool   `json:"snappy"`
		SampleRate      int32  `json:"sample_rate"`
	}{
		MaxRdyCount:     p.context.nsqd.options.MaxRdyCount,
		Version:         util.BINARY_VERSION,
		MaxMsgTimeout:   int64(p.context.nsqd.options.MaxMsgTimeout / time.Millisecond),
		MsgTimeout:      int64(p.context.nsqd.options.MsgTimeout / time.Millisecond),
		TLSv1:           tlsv1,
		Deflate:         deflate,
		DeflateLevel:    deflateLevel,
		MaxDeflateLevel: p.context.nsqd.options.MaxDeflateLevel,
		Snappy:          snappy,
		SampleRate:      client.SampleRate,
	})
	if err != nil {
		panic("should never happen")
	}

	err = p.Send(client, nsq.FrameTypeResponse, resp)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		log.Printf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, nsq.FrameTypeResponse, okBytes)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		log.Printf("PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, nsq.FrameTypeResponse, okBytes)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		log.Printf("PROTOCOL(V2): [%s] upgrading connection to deflate", client)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, nsq.FrameTypeResponse, okBytes)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

func (p *ProtocolV2) SUB(client *ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateInit {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval < 0 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, util.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name '%s' is not valid", topicName))
	}

	channelName := string(params[2])
	if !nsq.IsValidChannelName(channelName) {
		return nil, util.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name '%s' is not valid", channelName))
	}

	topic := p.context.nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client.ID, client)

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
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := util.ByteToBase10(params[1])
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.context.nsqd.options.MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, util.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.context.nsqd.options.MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *ProtocolV2) FIN(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	id := *(*nsq.MessageID)(unsafe.Pointer(&params[1][0]))
	err := client.Channel.FinishMessage(client.ID, id)
	if err != nil {
		return nil, util.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", id, err.Error()))
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *ProtocolV2) REQ(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	id := *(*nsq.MessageID)(unsafe.Pointer(&params[1][0]))
	timeoutMs, err := util.ByteToBase10(params[2])
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > maxTimeout {
		return nil, util.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("REQ timeout %d out of range 0-%d", timeoutDuration, maxTimeout))
	}

	err = client.Channel.RequeueMessage(client.ID, id, timeoutDuration)
	if err != nil {
		return nil, util.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", id, err.Error()))
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *ProtocolV2) CLS(client *ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateSubscribed {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
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
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, util.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name '%s' is not valid", topicName))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, util.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.context.nsqd.options.MaxMessageSize {
		return nil, util.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.context.nsqd.options.MaxMessageSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}

	topic := p.context.nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-p.context.nsqd.idChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	return okBytes, nil
}

func (p *ProtocolV2) MPUB(client *ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !nsq.IsValidTopicName(topicName) {
		return nil, util.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name '%s' is not valid", topicName))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 {
		return nil, util.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.context.nsqd.options.MaxBodySize {
		return nil, util.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.context.nsqd.options.MaxBodySize))
	}

	messages, err := readMPUB(client.Reader, client.lenSlice, p.context.nsqd.idChan,
		p.context.nsqd.options.MaxMessageSize)
	if err != nil {
		return nil, err
	}
	topic := p.context.nsqd.GetTopic(topicName)

	// if we've made it this far we've validated all the input,
	// the only possible error is that the topic is exiting during
	// this next call (and no messages will be queued in that case)
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	return okBytes, nil
}

func (p *ProtocolV2) TOUCH(client *ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	id := *(*nsq.MessageID)(unsafe.Pointer(&params[1][0]))
	err := client.Channel.TouchMessage(client.ID, id)
	if err != nil {
		return nil, util.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", id, err.Error()))
	}

	return nil, nil
}

func readMPUB(r io.Reader, tmp []byte, idChan chan nsq.MessageID, maxMessageSize int64) ([]*nsq.Message, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	if numMessages <= 0 {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*nsq.Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			return nil, util.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			return nil, util.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, util.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		messages = append(messages, nsq.NewMessage(<-idChan, msgBody))
	}

	return messages, nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}
