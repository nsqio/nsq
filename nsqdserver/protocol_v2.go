package nsqdserver

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/internal/ext"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
	"reflect"
)

const (
	E_INVALID         = "E_INVALID"
	E_TOPIC_NOT_EXIST = "E_TOPIC_NOT_EXIST"
)

const maxTimeout = time.Hour

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")
var offsetSplitStr = ":"
var offsetSplitBytes = []byte(offsetSplitStr)

// no need to support this type (count message maybe changed)
//var offsetCountType = "count"

var offsetVirtualQueueType = "virtual_queue"
var offsetTimestampType = "timestamp"
var offsetSpecialType = "special"
var offsetMsgCountType = "msgcount"

var (
	ErrOrderChannelOnSampleRate = errors.New("order consume is not allowed while sample rate is not 0")
	ErrPubToWaitTimeout         = errors.New("pub to wait channel timeout")
)

type protocolV2 struct {
	ctx *context
}

type ConsumeOffset struct {
	OffsetType  string
	OffsetValue int64
}

func (self *ConsumeOffset) ToString() string {
	return self.OffsetType + offsetSplitStr + strconv.FormatInt(self.OffsetValue, 10)
}

func (self *ConsumeOffset) FromString(s string) error {
	values := strings.Split(s, offsetSplitStr)
	if len(values) != 2 {
		return errors.New("invalid consume offset:" + s)
	}
	self.OffsetType = values[0]
	if self.OffsetType != offsetTimestampType &&
		self.OffsetType != offsetSpecialType &&
		self.OffsetType != offsetVirtualQueueType &&
		self.OffsetType != offsetMsgCountType {
		return errors.New("invalid consume offset:" + s)
	}
	v, err := strconv.ParseInt(values[1], 10, 0)
	if err != nil {
		return err
	}
	self.OffsetValue = v
	return nil
}

func (self *ConsumeOffset) FromBytes(s []byte) error {
	values := bytes.Split(s, offsetSplitBytes)
	if len(values) != 2 {
		return errors.New("invalid consume offset:" + string(s))
	}
	self.OffsetType = string(values[0])
	if self.OffsetType != offsetTimestampType &&
		self.OffsetType != offsetSpecialType &&
		self.OffsetType != offsetVirtualQueueType &&
		self.OffsetType != offsetMsgCountType {
		return errors.New("invalid consume offset:" + string(s))
	}
	v, err := strconv.ParseInt(string(values[1]), 10, 0)
	if err != nil {
		return err
	}
	self.OffsetValue = v
	return nil
}

func (p *protocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time
	left := make([]byte, 100)
	tmpLine := make([]byte, 100)

	clientID := p.ctx.nextClientID()
	client := nsqd.NewClientV2(clientID, conn, p.ctx.getOpts(), p.ctx.GetTlsConfig())
	client.SetWriteDeadline(zeroTime)

	// synchronize the startup of messagePump in order
	// to guarantee that it gets a chance to initialize
	// goroutine local state derived from client attributes
	// and avoid a potential race with IDENTIFY (where a client
	// could have changed or disabled said attributes)
	messagePumpStartedChan := make(chan bool)
	msgPumpStoppedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan, msgPumpStoppedChan)
	<-messagePumpStartedChan

	for {
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 3))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		if nsqd.NsqLogger().Level() > levellogger.LOG_DETAIL {
			nsqd.NsqLogger().Logf("PROTOCOL(V2) got client command: %v ", line)
		}
		// handle the compatible for message id.
		// Since the new message id is id+traceid. we can not
		// use \n to check line.
		// REQ, FIN, TOUCH (with message id as param) should be handled.
		// FIN 16BYTES\n
		// REQ 16bytes time\n
		// TOUCH 16bytes\n
		isSpecial := false
		params := make([][]byte, 0)
		if len(line) >= 3 {
			if bytes.Equal(line[:3], []byte("FIN")) ||
				bytes.Equal(line[:3], []byte("REQ")) {
				isSpecial = true
				if len(line) < 21 {
					left = left[:20-len(line)]
					nr := 0
					nr, err = io.ReadFull(client.Reader, left)
					if err != nil {
						nsqd.NsqLogger().LogErrorf("read param err:%v", err)
					}
					line = append(line, left[:nr]...)
					tmpLine = tmpLine[:len(line)]
					copy(tmpLine, line)
					// the readslice will overwrite the slice line,
					// so we should copy it and copy back.
					extra, extraErr := client.Reader.ReadSlice('\n')
					tmpLine = append(tmpLine, extra...)
					line = append(line[:0], tmpLine...)
					if extraErr != nil {
						nsqd.NsqLogger().LogErrorf("read param err:%v", extraErr)
					}
				}
				params = append(params, line[:3])
				if len(line) >= 21 {
					params = append(params, line[4:20])
					// it must be REQ
					if bytes.Equal(line[:3], []byte("REQ")) {
						if len(line) >= 22 {
							params = append(params, line[21:len(line)-1])
						}
					} else {
						params = append(params, line[20:])
					}
				} else {
					params = append(params, []byte(""))
				}

			} else if len(line) >= 5 {
				if bytes.Equal(line[:5], []byte("TOUCH")) {
					isSpecial = true
					if len(line) < 23 {
						left = left[:23-len(line)]
						nr := 0
						nr, err = io.ReadFull(client.Reader, left)
						if err != nil {
							nsqd.NsqLogger().Logf("TOUCH param err:%v", err)
						}
						line = append(line, left[:nr]...)
					}
					params = append(params, line[:5])
					if len(line) >= 23 {
						params = append(params, line[6:22])
					} else {
						params = append(params, []byte(""))
					}
				}
			}
		}
		if p.ctx.getOpts().Verbose || nsqd.NsqLogger().Level() > levellogger.LOG_DETAIL {
			nsqd.NsqLogger().Logf("PROTOCOL(V2) got client command: %v ", line)
		}
		if !isSpecial {
			// trim the '\n'
			line = line[:len(line)-1]
			// optionally trim the '\r'
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			params = bytes.Split(line, separatorBytes)
		}

		if p.ctx.getOpts().Verbose || nsqd.NsqLogger().Level() > levellogger.LOG_DETAIL {
			nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] %v, %v", client, string(params[0]), params)
		}

		var response []byte
		response, err = p.Exec(client, params)
		err = handleRequestReponseForClient(client, response, err)
		if err != nil {
			nsqd.NsqLogger().Logf("PROTOCOL(V2) handle client command: %v failed", line)
			break
		}
	}

	if err != nil {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): client [%s] exiting ioloop with error: %v", client, err)
	}
	if nsqd.NsqLogger().Level() >= levellogger.LOG_DEBUG {
		nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): client [%s] exiting ioloop", client)
	}
	close(client.ExitChan)
	p.ctx.nsqd.CleanClientPubStats(client.String(), "tcp")
	<-msgPumpStoppedChan

	if nsqd.NsqLogger().Level() >= levellogger.LOG_DEBUG {
		nsqd.NsqLogger().Logf("msg pump stopped client %v", client)
	}

	if client.Channel != nil {
		client.Channel.RequeueClientMessages(client.ID, client.String())
		client.Channel.RemoveClient(client.ID, client.GetTag())
	}
	client.FinalClose()

	return err
}

func shouldHandleAsync(client *nsqd.ClientV2, params [][]byte) bool {
	return bytes.Equal(params[0], []byte("PUB"))
}

func handleRequestReponseForClient(client *nsqd.ClientV2, response []byte, err error) error {
	if err != nil {
		ctx := ""

		if childErr, ok := err.(protocol.ChildErr); ok {
			if parentErr := childErr.Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
		}

		nsqd.NsqLogger().LogDebugf("Error response for [%s] - %s - %s",
			client, err, ctx)

		sendErr := Send(client, frameTypeError, []byte(err.Error()))
		if sendErr != nil {
			nsqd.NsqLogger().LogErrorf("Send response error: [%s] - %s%s", client, sendErr, ctx)
			return err
		}

		// errors of type FatalClientErr should forceably close the connection
		if _, ok := err.(*protocol.FatalClientErr); ok {
			return err
		}
		return nil
	}

	if response != nil {
		sendErr := Send(client, frameTypeResponse, response)
		if sendErr != nil {
			err = fmt.Errorf("failed to send response - %s", sendErr)
		}
	}

	return err
}

func SendMessage(client *nsqd.ClientV2, msg *nsqd.Message, writeExt bool, buf *bytes.Buffer, needFlush bool) error {
	buf.Reset()
	if !client.EnableTrace {
		_, err := msg.WriteTo(buf, writeExt)
		if err != nil {
			return err
		}
	} else {
		_, err := msg.WriteToWithDetail(buf, writeExt)
		if err != nil {
			return err
		}
	}

	err := internalSend(client, frameTypeMessage, buf.Bytes(), needFlush)
	if err != nil {
		return err
	}

	return nil
}

func SendNow(client *nsqd.ClientV2, frameType int32, data []byte) error {
	return internalSend(client, frameType, data, true)
}

func internalSend(client *nsqd.ClientV2, frameType int32, data []byte, needFlush bool) error {
	client.LockWrite()
	defer client.UnlockWrite()
	if client.Writer == nil {
		return errors.New("client closed")
	}

	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		return err
	}

	if needFlush || frameType != frameTypeMessage {
		err = client.Flush()
	}
	return err
}

func Send(client *nsqd.ClientV2, frameType int32, data []byte) error {
	return internalSend(client, frameType, data, false)
}

func (p *protocolV2) Exec(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("PUB_TRACE")):
		return p.PUBTRACE(client, params)
	case bytes.Equal(params[0], []byte("PUB_EXT")):
		return p.PUBEXT(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB_TRACE")):
		return p.MPUBTRACE(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("SUB_ADVANCED")):
		return p.SUBADVANCED(client, params)
	case bytes.Equal(params[0], []byte("SUB_ORDERED")):
		return p.SUBORDERED(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params)
	case bytes.Equal(params[0], []byte("INTERNAL_CREATE_TOPIC")):
		return p.internalCreateTopic(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, E_INVALID, fmt.Sprintf("invalid command %v", params))
}

func (p *protocolV2) messagePump(client *nsqd.ClientV2, startedChan chan bool,
	stoppedChan chan bool) {
	var err error
	var buf bytes.Buffer
	var clientMsgChan chan *nsqd.Message
	var subChannel *nsqd.Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time
	var sampleRate int32

	subEventChan := client.SubEventChan
	identifyEventChan := client.IdentifyEventChan
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout)
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C
	heartbeatFailedCnt := 0
	msgTimeout := client.MsgTimeout
	lastActiveTime := time.Now()
	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	flushed := true

	// signal to the goroutine that started the messagePump
	// that we've started up
	close(startedChan)

	for {
		if subChannel == nil || !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			clientMsgChan = nil
			flusherChan = nil
			// force flush
			client.LockWrite()
			err = client.Flush()
			client.UnlockWrite()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			clientMsgChan = client.GetTagMsgChannel()
			if clientMsgChan == nil {
				clientMsgChan = subChannel.GetClientMsgChan()
			}
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			clientMsgChan = client.GetTagMsgChannel()
			if clientMsgChan == nil {
				clientMsgChan = subChannel.GetClientMsgChan()
			}
			flusherChan = outputBufferTicker.C
		}

		select {
		case <-client.ExitChan:
			goto exit
		case <-flusherChan:
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush
			client.LockWrite()
			err = client.Flush()
			client.UnlockWrite()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan:
		case subChannel = <-subEventChan:
			// you can't SUB anymore
			nsqd.NsqLogger().Logf("client %v sub to topic %v channel: %v", client,
				subChannel.GetTopicName(),
				subChannel.GetName())
			subEventChan = nil
			tag := client.GetTag()
			if tag != "" {
				client.SetTagMsgChannel(subChannel.GetOrCreateClientMsgChannel(tag))
			}
		case identifyData := <-identifyEventChan:
			// you can't IDENTIFY anymore
			identifyEventChan = nil

			outputBufferTicker.Stop()
			if identifyData.OutputBufferTimeout > 0 {
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 {
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 {
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout
		case <-heartbeatChan:
			if subChannel != nil && client.IsReadyForMessages() {
				// try wake up the channel
				subChannel.TryWakeupRead()
			}

			if subChannel != nil && subChannel.Depth() <= 0 {
				lastActiveTime = time.Now()
			}
			// close this client if depth is large and the active is long ago.
			// maybe some bug blocking this client, reconnect can solve bug.
			if subChannel != nil &&
				time.Since(lastActiveTime) > (msgTimeout*10+client.HeartbeatInterval) &&
				!subChannel.IsOrdered() && subChannel.Depth() > 10 &&
				subChannel.GetInflightNum() <= 0 && !subChannel.IsPaused() {
				nsqd.NsqLogger().Warningf("client %s not active since %v, current : %v, %v, %v", client, lastActiveTime,
					subChannel.Depth(), subChannel.DepthTimestamp(), subChannel.GetChannelDebugStats())
				goto exit
			}
			err = Send(client, frameTypeResponse, heartbeatBytes)
			nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): [%s] send heartbeat", client)
			if err != nil {
				heartbeatFailedCnt++
				nsqd.NsqLogger().LogWarningf("PROTOCOL(V2): [%s] send heartbeat failed %v times, %v", client, heartbeatFailedCnt, err)
				if heartbeatFailedCnt > 2 {
					goto exit
				}
			} else {
				heartbeatFailedCnt = 0
			}
		case msg, ok := <-clientMsgChan:
			if !ok {
				goto exit
			}

			if sampleRate > 0 && rand.Int31n(100) > sampleRate && msg.DelayedType != nsqd.ChannelDelayed {
				// FIN automatically, all message will not wait to confirm if not sending,
				// and the reader keep moving forward.
				offset, confirmedCnt, changed := subChannel.ConfirmBackendQueue(msg)
				subChannel.CleanWaitingRequeueChan(msg)
				if changed && p.ctx.nsqdCoord != nil {
					p.ctx.nsqdCoord.SetChannelConsumeOffsetToCluster(subChannel, int64(offset), confirmedCnt, true)
				}
				continue
			}
			if subChannel.ShouldWaitDelayed(msg) {
				subChannel.ConfirmBackendQueue(msg)
				subChannel.CleanWaitingRequeueChan(msg)
				continue
			}
			// avoid re-send some confirmed message,
			// this may happen while the channel reader is reset to old position
			// due to some retry or leader change.
			if subChannel.IsConfirmed(msg) {
				subChannel.CleanWaitingRequeueChan(msg)
				subChannel.ContinueConsumeForOrder()
				continue
			}
			shouldSend, err := subChannel.StartInFlightTimeout(msg, client, client.String(), msgTimeout)
			if !shouldSend || err != nil {
				continue
			}

			lastActiveTime = time.Now()
			client.SendingMessage()
			err = SendMessage(client, msg, subChannel.IsExt(), &buf, subChannel.IsOrdered())
			if err != nil {
				goto exit
			}
			flushed = false
		}
	}

exit:
	if nsqd.NsqLogger().Level() > levellogger.LOG_DEBUG {
		nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): [%s] exiting messagePump", client)
	}
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
	close(stoppedChan)
}

func (p *protocolV2) IDENTIFY(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	var err error

	state := atomic.LoadInt32(&client.State)
	if state != stateInit {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.ctx.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData nsqd.IdentifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	nsqd.NsqLogger().LogDebugf("PROTOCOL(V2): [%s] %+v", client, identifyData)

	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.ctx.GetTlsConfig() != nil && identifyData.TLSv1
	deflate := p.ctx.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 0
	if deflate {
		if identifyData.DeflateLevel <= 0 {
			deflateLevel = 6
		}
		deflateLevel = int(math.Min(float64(deflateLevel), float64(p.ctx.getOpts().MaxDeflateLevel)))
	}
	snappy := p.ctx.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
		DesiredTag          string `json:"desired_tag,omitempty"`
	}{
		MaxRdyCount:         p.ctx.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.ctx.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.ctx.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.ctx.isAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
		DesiredTag:          client.GetTag(),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to deflate", client)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

func (p *protocolV2) AUTH(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateInit {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot AUTH in current state")
	}

	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "AUTH invalid number of parameters")
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	if int64(bodyLen) > p.ctx.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.ctx.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "AUTH Already set")
	}

	if !p.ctx.isAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH Disabled")
	}

	if err = client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH No authorizations found")
	}

	var resp []byte
	resp, err = json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

func (p *protocolV2) CheckAuth(client *nsqd.ClientV2, cmd, topicName, channelName string) error {
	// if auth is enabled, the client must have authorized already
	// compare topic/channel against cached authorization data (refetching if expired)
	if p.ctx.isAuthEnabled() {
		if !client.HasAuthorizations() {
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// we don't want to leak errors contacting the auth server to untrusted clients
			nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil
}

// params: [command topic channel partition consume_start]
func (p *protocolV2) SUBADVANCED(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	var consumeStart *ConsumeOffset
	if len(params) > 4 && len(params[4]) > 0 {
		consumeStart = &ConsumeOffset{}
		err := consumeStart.FromBytes(params[4])
		if err != nil {
			return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
		}
	}
	return p.internalSUB(client, params, true, false, consumeStart)
}

//params: [command topic channel partition]
func (p *protocolV2) SUBORDERED(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalSUB(client, params, true, true, nil)
}

func (p *protocolV2) SUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalSUB(client, params, false, false, nil)
}

func (p *protocolV2) internalSUB(client *nsqd.ClientV2, params [][]byte, enableTrace bool,
	ordered bool, startFrom *ConsumeOffset) ([]byte, error) {

	state := atomic.LoadInt32(&client.State)
	if state != stateInit {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	partition := -1
	channelName := ""
	var err error
	channelName = string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	if len(params) == 4 {
		partition, err = strconv.Atoi(string(params[3]))
		if err != nil {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
				fmt.Sprintf("topic partition is not valid: %v", err))
		}
	}

	if err = p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	if partition == -1 {
		partition = p.ctx.getDefaultPartition(topicName)
	}

	topic, err := p.ctx.getExistingTopic(topicName, partition)
	if err != nil {
		nsqd.NsqLogger().Logf("sub to not existing topic: %v, err:%v", topicName, err.Error())
		return nil, protocol.NewFatalClientErr(nil, E_TOPIC_NOT_EXIST, "")
	}
	if topic.IsOrdered() && !ordered {
		return nil, protocol.NewFatalClientErr(nil, "E_SUB_ORDER_IS_MUST", "this topic is configured only allow ordered sub")
	}
	if !p.ctx.checkForMasterWrite(topicName, partition) {
		nsqd.NsqLogger().Logf("sub failed on not leader: %v-%v, remote is : %v", topicName, partition, client.String())
		// we need disable topic here to trigger a notify, maybe we failed to notify lookup last time.
		topic.DisableForSlave()
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotLeader, "")
	}
	channel := topic.GetChannel(channelName)
	if !topic.IsExt() && client.GetTag() != "" {
		return nil, protocol.NewFatalClientErr(nil, ext.E_EXT_NOT_SUPPORT, fmt.Sprintf("IDENTIFY before subscribe has a tag %v to topic %v not support tag.", client.Tag, topicName))
	}

	err = channel.AddClient(client.ID, client)
	if err != nil {
		nsqd.NsqLogger().Logf("sub failed to add client: %v, %v", client, err)
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotWritable, "")
	}

	//if client.Tag != nil {
	//	client.SetTagMsgChannel(channel.GetOrCreateClientMsgChannel(client.Tag))
	//}

	atomic.StoreInt32(&client.State, stateSubscribed)
	client.Channel = channel
	if enableTrace {
		nsqd.NsqLogger().Logf("sub channel %v with trace enabled, remote is : %v", channelName, client.String())
	}
	if ordered {
		if atomic.LoadInt32(&client.SampleRate) != 0 {
			nsqd.NsqLogger().Errorf("%v", ErrOrderChannelOnSampleRate)
			return nil, protocol.NewFatalClientErr(nil, E_INVALID, ErrOrderChannelOnSampleRate.Error())
		}
		channel.SetOrdered(true)
	}

	if startFrom != nil {
		cnt := channel.GetClientsCount()
		if cnt > 1 {
			nsqd.NsqLogger().LogDebugf("the consume offset: %v can only be set by the first client: %v", startFrom, cnt)
		} else {
			queueOffset, cnt, err := p.ctx.SetChannelOffset(channel, startFrom, false)
			if err != nil {
				return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
			}
			nsqd.NsqLogger().Logf("set the channel offset: %v (actual set : %v:%v), by client:%v, %v",
				startFrom, queueOffset, cnt, client.String(), client.UserAgent)
		}
	}
	client.EnableTrace = enableTrace
	// update message pump
	client.SubEventChan <- channel

	return okBytes, nil
}

func (p *protocolV2) RDY(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		// just ignore ready changes on a closing channel
		nsqd.NsqLogger().Logf(
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, E_INVALID,
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.ctx.getOpts().MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, protocol.NewFatalClientErr(nil, E_INVALID,
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ctx.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *protocolV2) FIN(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot FIN in current state")
	}

	if len(params) < 2 {
		nsqd.NsqLogger().LogDebugf("FIN error params: %v", params)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "FIN insufficient number of params")
	}

	id, err := getFullMessageID(params[1])
	if err != nil {
		nsqd.NsqLogger().LogDebugf("FIN error: %v, %v", params[1], err)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
	}
	msgID := nsqd.GetMessageIDFromFullMsgID(*id)
	if int64(msgID) <= 0 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "Invalid Message ID")
	}

	if client.Channel == nil {
		nsqd.NsqLogger().LogDebugf("FIN error no channel: %v", msgID)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}

	if !p.ctx.checkForMasterWrite(client.Channel.GetTopicName(), client.Channel.GetTopicPart()) {
		nsqd.NsqLogger().Logf("topic %v fin message failed for not leader", client.Channel.GetTopicName())
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotLeader, "")
	}

	err = p.ctx.FinishMessage(client.Channel, client.ID, client.String(), msgID)
	if err != nil {
		client.IncrSubError(int64(1))
		nsqd.NsqLogger().LogDebugf("FIN error : %v, err: %v, channel: %v, topic: %v", msgID,
			err, client.Channel.GetName(), client.Channel.GetTopicName())
		if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
			if !clusterErr.IsLocalErr() {
				return nil, protocol.NewFatalClientErr(err, FailedOnNotWritable, "")
			}
		}
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %v failed %s", *id, err.Error()))
	}

	return nil, nil
}

func (p *protocolV2) requeueToEnd(client *nsqd.ClientV2, oldMsg *nsqd.Message,
	timeoutDuration time.Duration) error {
	err := p.ctx.internalRequeueToEnd(client.Channel, oldMsg, timeoutDuration)
	if err != nil {
		nsqd.NsqLogger().LogWarningf("[%s] req channel %v failed: %v", client, client.Channel.GetName(), err)
		return err
	}
	return nil
}

func (p *protocolV2) REQ(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "REQ insufficient number of params")
	}

	id, err := getFullMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, E_INVALID,
			fmt.Sprintf("REQ could not parse timeout %s, %s", params[1], params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	maxReqTimeout := p.ctx.getOpts().MaxReqTimeout
	clampedTimeout := timeoutDuration

	if timeoutDuration < 0 {
		clampedTimeout = 0
	} else if timeoutDuration > maxReqTimeout {
		clampedTimeout = maxReqTimeout
	}
	if clampedTimeout != timeoutDuration {
		nsqd.NsqLogger().Logf("[%s] REQ timeout %d out of range 0-%d. Setting to %d",
			client, timeoutDuration, maxReqTimeout, clampedTimeout)
		timeoutDuration = clampedTimeout
	}
	if client.Channel == nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}
	// in the queue, we confirm the message as a fifo-alike queue,
	// Too much req messages in memory will block the queue read from disk until the requeued message confirmed.
	// To avoid block by req, we put some of the req messages to the end of queue of some conditions meet
	// 1. the req delay time is large than 10 mins (this delay means latency is trival)
	// 2. this message has been req for more than 10 times
	// 3. this message is blocking confirm queue for 10 mins
	// to avoid delivery the delayed message early than required, we
	// can update the inflight message to the new message put backed at the queue

	msgID := nsqd.GetMessageIDFromFullMsgID(*id)
	topic, _ := p.ctx.getExistingTopic(client.Channel.GetTopicName(), client.Channel.GetTopicPart())
	oldMsg, toEnd := client.Channel.ShouldRequeueToEnd(client.ID, client.String(),
		msgID, timeoutDuration, true)
	if topic != nil && topic.IsOrdered() {
		toEnd = false
		// for ordered topic, disable defer since it may block the consume
		if timeoutDuration > 0 {
			nsqd.NsqLogger().Logf("ignore delay for ordered topic: %v, %v, %v, %v",
				client, client.Channel.GetTopicName(), client.Channel.GetName(), timeoutDuration)
			return nil, nil
		}
	}
	if toEnd {
		err = p.requeueToEnd(client, oldMsg, timeoutDuration)
		if err != nil {
			// try to reduce timeout to requeue to memory if failed to requeue to end
			if timeoutDuration > p.ctx.getOpts().ReqToEndThreshold {
				timeoutDuration = p.ctx.getOpts().ReqToEndThreshold
			}
		}
	}
	if !toEnd || err != nil {
		err = client.Channel.RequeueMessage(client.ID, client.String(), msgID, timeoutDuration, true)
	}
	if err != nil {
		client.IncrSubError(int64(1))

		nsqd.NsqLogger().LogWarningf("client %v req failed for topic: %v, %v, %v, %v",
			client, client.Channel.GetTopicName(), client.Channel.GetName(), msgID, timeoutDuration)
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %v failed %s", *id, err.Error()))
	}

	return nil, nil
}

func (p *protocolV2) CLS(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot CLS in current state")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

func (p *protocolV2) NOP(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

func (p *protocolV2) internalCreateTopic(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "CREATE_TOPIC insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("topic name %q is not valid", topicName))
	}

	partition, err := strconv.Atoi(string(params[2]))
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
			fmt.Sprintf("topic partition is not valid: %v", err))
	}
	if err = p.CheckAuth(client, "CREATE_TOPIC", topicName, ""); err != nil {
		return nil, err
	}

	if partition < 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION", "partition should not less than 0")
	}

	var ext bool
	if len(params) >= 4 {
		ext, err = strconv.ParseBool(string(params[3]))
		if err != nil {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_EXT",
				fmt.Sprintf("topic ext is not valid: %v", err))
		}
	}

	if p.ctx.nsqdCoord != nil {
		return nil, protocol.NewClientErr(err, "E_CREATE_TOPIC_FAILED",
			fmt.Sprintf("CREATE_TOPIC is not allowed here while cluster feature enabled."))
	}

	topic := p.ctx.getTopic(topicName, partition, ext)
	if topic == nil {
		return nil, protocol.NewClientErr(err, "E_CREATE_TOPIC_FAILED",
			fmt.Sprintf("CREATE_TOPIC %v failed", topicName))
	}
	return okBytes, nil
}

//if target topic is not configured as extendable and there is a tag, pub request should be stopped here
func (p *protocolV2) preparePub(client *nsqd.ClientV2, params [][]byte, maxBody int64, isMpub bool) (int32, *nsqd.Topic, ext.IExtContent, error) {
	var err error

	if len(params) < 2 {
		return 0, nil, nil, protocol.NewFatalClientErr(nil, E_INVALID, "insufficient number of parameters")
	}

	topicName := string(params[1])
	partition := -1
	if len(params) >= 3 {
		partition, err = strconv.Atoi(string(params[2]))
		if err != nil {
			return 0, nil, nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
				fmt.Sprintf("topic partition is not valid: %v", err))
		}
	}

	origPart := partition
	if partition == -1 {
		partition = p.ctx.getDefaultPartition(topicName)
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return 0, nil, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "failed to read body size")
	}

	if bodyLen <= 0 {
		return bodyLen, nil, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > maxBody {
		nsqd.NsqLogger().Logf("topic: %v message body too large %v vs %v ", topicName, bodyLen, maxBody)
		if isMpub {
			return bodyLen, nil, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
				fmt.Sprintf("body too big %d > %d", bodyLen, maxBody))
		} else {
			return bodyLen, nil, nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("message too big %d > %d", bodyLen, maxBody))
		}
	}

	topic, err := p.ctx.getExistingTopic(topicName, partition)
	if err != nil {
		nsqd.NsqLogger().Logf("not existing topic: %v-%v, err:%v", topicName, partition, err.Error())
		return bodyLen, nil, nil, protocol.NewFatalClientErr(nil, E_TOPIC_NOT_EXIST, "")
	}

	if origPart == -1 && topic.IsOrdered() {
		return 0, nil, nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
			fmt.Sprintf("topic partition is not valid for multi partition: %v", origPart))
	}

	isExt := topic.IsExt()
	extContent, err := parseExtContent(topicName, isExt, params)
	if err != nil {
		return 0, nil, nil, err
	}

	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return bodyLen, nil, nil, err
	}
	// mpub
	return bodyLen, topic, extContent, nil
}

func parseExtContent(topicName string, isExt bool, params [][]byte) (ext.IExtContent, error) {
	var extContent ext.IExtContent
	var err error
	pubExt := isPubExt(params[0])
	if pubExt && isExt {
		//parse as json header ext, leave outer logic to parse json ext header
		extContent = ext.NewJsonHeaderExt()
	} else if pubExt && !isExt {
		err = protocol.NewClientErr(nil, ext.E_EXT_NOT_SUPPORT,
			fmt.Sprintf("ext content not supported in topic %v", topicName))
	} else {
		extContent = ext.NewNoExt()
	}
	return extContent, err
}

// PUB TRACE data format
// 4 bytes length + 8bytes trace id + binary data
func (p *protocolV2) PUBTRACE(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubAndTrace(client, params, true)
}

func (p *protocolV2) PUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubAndTrace(client, params, false)
}

func (p *protocolV2) PUBEXT(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubExtAndTrace(client, params, true, false)
}

func (p *protocolV2) MPUBTRACE(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalMPUBAndTrace(client, params, true)
}

func (p *protocolV2) MPUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalMPUBAndTrace(client, params, false)
}

func getTracedReponse(id nsqd.MessageID, traceID uint64, offset nsqd.BackendOffset, rawSize int32) ([]byte, error) {
	// pub with trace will return OK+16BYTES ID+8bytes offset of the disk queue + 4bytes raw size of disk queue data.
	retLen := 2 + 16 + 8 + 4
	buf := make([]byte, retLen)
	copy(buf[:2], okBytes)
	pos := 2
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(id))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+nsqd.MsgTraceIDLength], uint64(traceID))
	pos += nsqd.MsgTraceIDLength
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(offset))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(rawSize))

	if nsqd.NsqLogger().Level() >= levellogger.LOG_DEBUG {
		nsqd.NsqLogger().Logf("pub traced %v (%v) response : %v", id, offset, buf)
	}
	return buf, nil
}

func internalPubAsync(clientTimer *time.Timer, msgBody *bytes.Buffer, topic *nsqd.Topic, extContent ext.IExtContent) error {
	if topic.Exiting() {
		return nsqd.ErrExiting
	}
	info := &nsqd.PubInfo{
		Done:       make(chan struct{}),
		MsgBody:    msgBody,
		ExtContent: extContent,
		StartPub:   time.Now(),
	}
	if clientTimer == nil {
		clientTimer = time.NewTimer(time.Second * 5)
	} else {
		clientTimer.Reset(time.Second * 5)
	}
	select {
	case topic.GetWaitChan() <- info:
	default:
		select {
		case topic.GetWaitChan() <- info:
		case <-topic.QuitChan():
			nsqd.NsqLogger().Infof("topic %v put messages failed at exiting", topic.GetFullName())
			return nsqd.ErrExiting
		case <-clientTimer.C:
			nsqd.NsqLogger().Infof("topic %v put messages timeout ", topic.GetFullName())
			return ErrPubToWaitTimeout
		}
	}
	<-info.Done
	return info.Err
}

func isPubExt(pubCmdName []byte) bool {
	return bytes.Equal(pubCmdName, []byte("PUB_WITH_EXT"))
}


func (p *protocolV2) internalPubAndTrace(client *nsqd.ClientV2, params [][]byte, traceEnable bool) ([]byte, error) {
	return p.internalPubExtAndTrace(client, params, false, traceEnable)
}

/**
pub ext or pub trace or pub, if pubExt is true, traceEnable is ignored.
 */
func (p *protocolV2) internalPubExtAndTrace(client *nsqd.ClientV2, params [][]byte, pubExt bool, traceEnable bool) ([]byte, error) {
	startPub := time.Now().UnixNano()
	bodyLen, topic, extContent, err := p.preparePub(client, params, p.ctx.getOpts().MaxMsgSize, false)
	if err != nil {
		return nil, err
	}

	if traceEnable && bodyLen <= nsqd.MsgTraceIDLength {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d with trace id enabled", bodyLen))
	}

	if pubExt && bodyLen <= nsqd.MsgJsonHeaderLength {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d with ext json header enabled", bodyLen))
	}

	messageBodyBuffer := topic.BufferPoolGet(int(bodyLen))
	defer topic.BufferPoolPut(messageBodyBuffer)
	asyncAction := shouldHandleAsync(client, params)

	topicName := topic.GetTopicName()
	_, err = io.CopyN(messageBodyBuffer, client.Reader, int64(bodyLen))
	if err != nil {
		nsqd.NsqLogger().Logf("topic: %v message body read error %v ", topicName, err.Error())
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "failed to read message body")
	}
	messageBody := messageBodyBuffer.Bytes()[:bodyLen]

	partition := topic.GetTopicPart()
	var extJsonLen uint16
	var traceID uint64
	var realBody []byte
	if traceEnable && !pubExt {
		traceID = binary.BigEndian.Uint64(messageBody[:nsqd.MsgTraceIDLength])
		realBody = messageBody[nsqd.MsgTraceIDLength:]
	} else if pubExt {
		//read two byte header length
		extJsonLen = binary.BigEndian.Uint16(messageBody[:nsqd.MsgJsonHeaderLength])
		extJsonBytes := messageBody[nsqd.MsgJsonHeaderLength:nsqd.MsgJsonHeaderLength + extJsonLen]
		jhe, ok := extContent.(*ext.JsonHeaderExt)
		if !ok {
			return nil, fmt.Errorf("invalid ext content type, Json Header Ext expected, got %v", reflect.TypeOf(extContent))
		}
		//validate json header passin
		var jsonHeader map[string]interface{}
		err := json.Unmarshal(extJsonBytes, &jsonHeader)
		if err != nil {
			return nil, protocol.NewClientErr(err, ext.E_INVALID_JSON_HEADER, "fail to parse json header")
		}

		//parse traceID, if there is any
		var traceIDStr string
		traceIDI, exist := jsonHeader[ext.TRACE_ID_KEY]
		if exist {
			var ok bool
			if traceIDStr, ok = traceIDI.(string); !ok {
				return nil, protocol.NewClientErr(err, "INVALID_TRACE_ID", "fail to parse trace id")
			}

			traceID, err = strconv.ParseUint(traceIDStr, 10, 0)
			if err != nil {
				return nil, protocol.NewClientErr(err, "INVALID_TRACE_ID", "fail to parse trace id")
			}
		}
		jhe.SetJsonHeaderBytes(extJsonBytes)
		realBody = messageBody[nsqd.MsgJsonHeaderLength + extJsonLen:]
	} else {
		realBody = messageBody
	}
	if p.ctx.checkForMasterWrite(topicName, partition) {
		id := nsqd.MessageID(0)
		offset := nsqd.BackendOffset(0)
		rawSize := int32(0)
		if asyncAction {
			err = internalPubAsync(client.PubTimeout, messageBodyBuffer, topic, extContent)
		} else {
			id, offset, rawSize, _, err = p.ctx.PutMessage(topic, realBody, extContent, traceID)
		}
		//p.ctx.setHealth(err)
		if err != nil {
			topic.GetDetailStats().UpdatePubClientStats(client.String(), client.UserAgent, "tcp", 1, true)
			nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)
			if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
				if !clusterErr.IsLocalErr() {
					return nil, protocol.NewClientErr(err, FailedOnNotWritable, "")
				}
			}
			return nil, protocol.NewClientErr(err, "E_PUB_FAILED", err.Error())
		}
		topic.GetDetailStats().UpdatePubClientStats(client.String(), client.UserAgent, "tcp", 1, false)
		cost := time.Now().UnixNano() - startPub
		topic.GetDetailStats().UpdateTopicMsgStats(int64(len(realBody)), cost/1000)
		if !traceEnable {
			return okBytes, nil
		}
		return getTracedReponse(id, traceID, offset, rawSize)
	} else {
		topic.GetDetailStats().UpdatePubClientStats(client.String(), client.UserAgent, "tcp", 1, true)
		//forward to master of topic
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), client.String())
		topic.DisableForSlave()
		return nil, protocol.NewClientErr(err, FailedOnNotLeader, "")
	}
}

func (p *protocolV2) internalMPUBAndTrace(client *nsqd.ClientV2, params [][]byte, traceEnable bool) ([]byte, error) {
	startPub := time.Now().UnixNano()
	_, topic, extContent, preErr := p.preparePub(client, params, p.ctx.getOpts().MaxBodySize, true)
	if preErr != nil {
		return nil, preErr
	}

	messages, buffers, preErr := readMPUB(client.Reader, client.LenSlice, topic,
		p.ctx.getOpts().MaxMsgSize, p.ctx.getOpts().MaxBodySize, extContent, traceEnable)

	defer func() {
		for _, b := range buffers {
			topic.BufferPoolPut(b)
		}
	}()
	if preErr != nil {
		return nil, preErr
	}

	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	if p.ctx.checkForMasterWrite(topicName, partition) {
		id, offset, rawSize, err := p.ctx.PutMessages(topic, messages)
		//p.ctx.setHealth(err)
		if err != nil {
			topic.GetDetailStats().UpdatePubClientStats(client.String(), client.UserAgent, "tcp", int64(len(messages)), true)
			nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)

			if clusterErr, ok := err.(*consistence.CommonCoordErr); ok {
				if !clusterErr.IsLocalErr() {
					return nil, protocol.NewClientErr(err, FailedOnNotWritable, "")
				}
			}
			return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", err.Error())
		}
		topic.GetDetailStats().UpdatePubClientStats(client.String(), client.UserAgent, "tcp", int64(len(messages)), false)
		cost := time.Now().UnixNano() - startPub
		topic.GetDetailStats().UpdateTopicMsgStats(0, cost/1000/int64(len(messages)))
		if !traceEnable {
			return okBytes, nil
		}
		return getTracedReponse(id, 0, offset, rawSize)
	} else {
		topic.GetDetailStats().UpdatePubClientStats(client.String(), client.UserAgent, "tcp", int64(len(messages)), true)
		//forward to master of topic
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), client.String())
		topic.DisableForSlave()
		return nil, protocol.NewClientErr(preErr, FailedOnNotLeader, "")
	}
}

func (p *protocolV2) TOUCH(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		nsqd.NsqLogger().LogWarningf("[%s] command in wrong state: %v", client, state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "TOUCH insufficient number of params")
	}

	id, err := getFullMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
	}

	client.LockRead()
	msgTimeout := client.MsgTimeout
	client.UnlockRead()

	if client.Channel == nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}
	err = client.Channel.TouchMessage(client.ID, nsqd.GetMessageIDFromFullMsgID(*id), msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %v failed %s", *id, err.Error()))
	}

	return nil, nil
}

func readMPUB(r io.Reader, tmp []byte, topic *nsqd.Topic, maxMessageSize int64,
	maxBodySize int64, extContent ext.IExtContent, traceEnable bool) ([]*nsqd.Message, []*bytes.Buffer, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 4 == total num, 5 == length + min 1
	maxMessages := (maxBodySize - 4) / 5

	if numMessages <= 0 || int64(numMessages) > maxMessages {
		return nil, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*nsqd.Message, 0, numMessages)
	buffers := make([]*bytes.Buffer, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, buffers, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		b := topic.BufferPoolGet(int(messageSize))
		buffers = append(buffers, b)
		_, err = io.CopyN(b, r, int64(messageSize))
		if err != nil {
			return nil, buffers, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}
		msgBody := b.Bytes()[:messageSize]

		traceID := uint64(0)
		var realBody []byte
		if traceEnable {
			if messageSize <= nsqd.MsgTraceIDLength {
				return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
					fmt.Sprintf("MPUB invalid message(%d) body size %d for tracing", i, messageSize))
			}
			traceID = binary.BigEndian.Uint64(msgBody[:nsqd.MsgTraceIDLength])
			realBody = msgBody[nsqd.MsgTraceIDLength:]
		} else {
			realBody = msgBody
		}

		var msg *nsqd.Message
		if !topic.IsExt() {
			msg = nsqd.NewMessage(0, realBody)
		} else {
			msg = nsqd.NewMessageWithExt(0, realBody, extContent.ExtVersion(), extContent.GetBytes())
		}
		msg.TraceID = traceID
		messages = append(messages, msg)
		topic.GetDetailStats().UpdateTopicMsgStats(int64(len(realBody)), 0)
	}

	return messages, buffers, nil
}

// validate and cast the bytes on the wire to a message ID
func getFullMessageID(p []byte) (*nsqd.FullMessageID, error) {
	if len(p) != nsqd.MsgIDLength {
		return nil, errors.New("Invalid Message ID")
	}
	return (*nsqd.FullMessageID)(unsafe.Pointer(&p[0])), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func enforceTLSPolicy(client *nsqd.ClientV2, p *protocolV2, command []byte) error {
	if p.ctx.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, E_INVALID,
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
