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
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/absolute8511/nsq/nsqd"
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

func (self *ConsumeOffset) FromString(s string) {
	strings.Split(s, offsetSplitStr)
}

func (self *ConsumeOffset) FromBytes(s []byte) {
	bytes.Split(s, offsetSplitBytes)
}

func (p *protocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time
	left := make([]byte, 100)

	clientID := p.ctx.nextClientID()
	client := nsqd.NewClientV2(clientID, conn, p.ctx.getOpts(), p.ctx.GetTlsConfig())

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
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
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
			// since we read a command, then left data should come quickly.
			client.SetReadDeadline(time.Now().Add(time.Second * 10))
			if bytes.Equal(line[:3], []byte("FIN")) ||
				bytes.Equal(line[:3], []byte("REQ")) {
				isSpecial = true
				if len(line) < 20 {
					left = left[:20-len(line)]
					nr := 0
					nr, err = client.Reader.Read(left)
					if err != nil {
						nsqd.NsqLogger().LogErrorf("read param err:%v", err)
					}
					line = append(line, left[:nr]...)
					// read last real '\n'
					extra, err := client.Reader.ReadSlice('\n')
					if err != nil {
						nsqd.NsqLogger().LogErrorf("read param err:%v", err)
					}
					line = append(line, extra...)
				}
				params = append(params, line[:3])
				if len(line) >= 20 {
					params = append(params, line[4:20])
					if len(line) > 21 {
						params = append(params, line[21:len(line)-1])
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
						nr, err = client.Reader.Read(left)
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
			client.SetReadDeadline(zeroTime)
		}
		if p.ctx.getOpts().Verbose {
			nsqd.NsqLogger().Logf("PROTOCOL(V2) got client command: %s ", line)
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

		if p.ctx.getOpts().Verbose {
			nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] %s", client, params)
		}

		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			ctx := ""

			if childErr, ok := err.(protocol.ChildErr); ok {
				if parentErr := childErr.Parent(); parentErr != nil {
					ctx = " - " + parentErr.Error()
				}
			}

			nsqd.NsqLogger().LogWarningf("Error response for [%s] - %s - %s, rawline: %v",
				client, err, ctx, line)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				nsqd.NsqLogger().LogErrorf("Send response error: [%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	nsqd.NsqLogger().Logf("PROTOCOL(V2): client [%s] exiting ioloop", client)
	close(client.ExitChan)
	<-msgPumpStoppedChan

	if client.Channel != nil {
		client.Channel.RequeueClientMessages(client.ID)
		client.Channel.RemoveClient(client.ID)
	}
	client.FinalClose()

	return err
}

func (p *protocolV2) SendMessage(client *nsqd.ClientV2, msg *nsqd.Message, buf *bytes.Buffer) error {
	buf.Reset()
	if !client.EnableTrace {
		_, err := msg.WriteTo(buf)
		if err != nil {
			return err
		}
	} else {
		_, err := msg.WriteToV2(buf)
		if err != nil {
			return err
		}
	}

	err := p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *protocolV2) Send(client *nsqd.ClientV2, frameType int32, data []byte) error {
	client.LockWrite()

	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval))
	} else {
		client.SetWriteDeadline(zeroTime)
	}

	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.UnlockWrite()
		return err
	}

	if frameType != frameTypeMessage {
		err = client.Flush()
	}

	client.UnlockWrite()

	return err
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
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params)
	case bytes.Equal(params[0], []byte("INTERNAL_CREATE_TOPIC")):
		return p.internalCreateTopic(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, E_INVALID, fmt.Sprintf("invalid command %s", params[0]))
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
			clientMsgChan = subChannel.GetClientMsgChan()
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			clientMsgChan = subChannel.GetClientMsgChan()
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
			nsqd.NsqLogger().Logf("client %v sub to channel: %v", client.ID,
				subChannel.GetName())
			subEventChan = nil
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
			err = p.Send(client, frameTypeResponse, heartbeatBytes)
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

			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				// FIN automatically, all message will not wait to confirm if not sending,
				// and the reader keep moving forward.
				offset, _ := subChannel.ConfirmBackendQueue(msg)
				// TODO: sync to replica nodes.
				_ = offset
				continue
			}
			// avoid re-send some confirmed message,
			// this may happen while the channel reader is reset to old position
			// due to some retry or leader change.
			if subChannel.IsConfirmed(msg) {
				continue
			}

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			client.SendingMessage()
			err = p.SendMessage(client, msg, &buf)
			if err != nil {
				goto exit
			}
			flushed = false
		}
	}

exit:
	nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
	close(stoppedChan)
}

func (p *protocolV2) IDENTIFY(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != stateInit {
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
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
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

		err = p.Send(client, frameTypeResponse, okBytes)
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

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

func (p *protocolV2) AUTH(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
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

	if err := client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		nsqd.NsqLogger().Logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH No authorizations found")
	}

	resp, err := json.Marshal(struct {
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

	err = p.Send(client, frameTypeResponse, resp)
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

// command topic channel partition ordered consume_start
func (p *protocolV2) SUBADVANCED(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	ordered := false
	consumeStart := &ConsumeOffset{}
	if len(params) > 4 {
		if strings.ToLower(string(params[4])) == "true" {
			ordered = true
		}
	}
	if len(params) > 5 {
		consumeStart.FromBytes(params[5])
	}
	return p.internalSUB(client, params, true, ordered, consumeStart)
}

func (p *protocolV2) SUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalSUB(client, params, false, false, nil)
}

func (p *protocolV2) internalSUB(client *nsqd.ClientV2, params [][]byte, enableTrace bool,
	ordered bool, startFrom *ConsumeOffset) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
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

	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	if partition == -1 {
		partition = p.ctx.getDefaultPartition(topicName)
	}

	topic, err := p.ctx.getExistingTopic(topicName, partition)
	if err != nil {
		nsqd.NsqLogger().Logf("sub to not existing topic: %v", topicName, err.Error())
		return nil, protocol.NewFatalClientErr(nil, E_TOPIC_NOT_EXIST, "")
	}
	if !p.ctx.checkForMasterWrite(topicName, partition) {
		nsqd.NsqLogger().Logf("sub failed on not leader: %v-%v, remote is : %v", topicName, partition, client.RemoteAddr())
		// we need disable topic here to trigger a notify, maybe we failed to notify lookup last time.
		topic.DisableForSlave()
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotLeader, "")
	}
	channel := topic.GetChannel(channelName)
	channel.AddClient(client.ID, client)

	atomic.StoreInt32(&client.State, stateSubscribed)
	client.Channel = channel
	if enableTrace {
		nsqd.NsqLogger().Logf("sub channel %v with trace enabled, remote is : %v", channelName, client.RemoteAddr())
	}
	if ordered {
		channel.SetOrdered(true)
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
		nsqd.NsqLogger().Logf("FIN error at state: %v", state)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "cannot FIN in current state")
	}

	if len(params) < 2 {
		nsqd.NsqLogger().Logf("FIN error params: %v", params)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "FIN insufficient number of params")
	}

	id, err := getFullMessageID(params[1])
	if err != nil {
		nsqd.NsqLogger().Logf("FIN error: %v, %v", params[1], err)
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, err.Error())
	}

	if client.Channel == nil {
		nsqd.NsqLogger().Logf("FIN error no channel: %v", nsqd.GetMessageIDFromFullMsgID(*id))
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}

	if !p.ctx.checkForMasterWrite(client.Channel.GetTopicName(), client.Channel.GetTopicPart()) {
		nsqd.NsqLogger().LogErrorf("topic %v fin message failed for not leader", client.Channel.GetTopicName())
		return nil, protocol.NewFatalClientErr(nil, FailedOnNotLeader, "")
	}

	err = p.ctx.FinishMessage(client.Channel, client.ID, nsqd.GetMessageIDFromFullMsgID(*id))
	if err != nil {
		nsqd.NsqLogger().Logf("FIN error : %v, err: %v", nsqd.GetMessageIDFromFullMsgID(*id),
			err)
		if clusterErr, ok := err.(*consistence.CoordErr); ok {
			if !clusterErr.IsLocalErr() {
				return nil, protocol.NewFatalClientErr(err, FailedOnNotWritable, "")
			}
		}
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %v failed %s", *id, err.Error()))
	}
	client.FinishedMessage()

	return nil, nil
}

func (p *protocolV2) REQ(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
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
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.ctx.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID,
			fmt.Sprintf("REQ timeout %d out of range 0-%d", timeoutDuration, p.ctx.getOpts().MaxReqTimeout))
	}

	if client.Channel == nil {
		return nil, protocol.NewFatalClientErr(nil, E_INVALID, "No channel")
	}
	err = client.Channel.RequeueMessage(client.ID, nsqd.GetMessageIDFromFullMsgID(*id), timeoutDuration)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %v failed %s", *id, err.Error()))
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *protocolV2) CLS(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateSubscribed {
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
	if err := p.CheckAuth(client, "CREATE_TOPIC", topicName, ""); err != nil {
		return nil, err
	}

	if partition < 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION", "partition should not less than 0")
	}

	if p.ctx.nsqdCoord != nil {
		return nil, protocol.NewClientErr(err, "E_CREATE_TOPIC_FAILED",
			fmt.Sprintf("CREATE_TOPIC is not allowed here while cluster feature enabled."))
	}

	topic := p.ctx.getTopic(topicName, partition)
	if topic == nil {
		return nil, protocol.NewClientErr(err, "E_CREATE_TOPIC_FAILED",
			fmt.Sprintf("CREATE_TOPIC %v failed", topicName))
	}
	return okBytes, nil
}

func (p *protocolV2) preparePub(client *nsqd.ClientV2, params [][]byte, maxBody int64) (int32, *nsqd.Topic, error) {
	var err error

	if len(params) < 2 {
		return 0, nil, protocol.NewFatalClientErr(nil, E_INVALID, "insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return 0, nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("topic name %q is not valid", topicName))
	}
	partition := -1
	if len(params) == 3 {
		partition, err = strconv.Atoi(string(params[2]))
		if err != nil {
			return 0, nil, protocol.NewFatalClientErr(nil, "E_BAD_PARTITION",
				fmt.Sprintf("topic partition is not valid: %v", err))
		}
	}

	if partition == -1 {
		partition = p.ctx.getDefaultPartition(topicName)
	}

	bodyLen, err := readLen(client.Reader, client.LenSlice)
	if err != nil {
		return 0, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "failed to read body size")
	}

	if bodyLen <= 0 {
		return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > maxBody {
		return bodyLen, nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("body too big %d > %d", bodyLen, maxBody))
	}

	topic, err := p.ctx.getExistingTopic(topicName, partition)
	if err != nil {
		nsqd.NsqLogger().Logf("not existing topic: %v", topicName, err.Error())
		return bodyLen, nil, protocol.NewFatalClientErr(nil, E_TOPIC_NOT_EXIST, "")
	}

	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return bodyLen, nil, err
	}
	// mpub
	return bodyLen, topic, nil
}

// PUB TRACE data format
// 4 bytes length + 8bytes trace id + binary data
func (p *protocolV2) PUBTRACE(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubAndTrace(client, params, true)
}

func (p *protocolV2) PUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalPubAndTrace(client, params, false)
}

func (p *protocolV2) MPUBTRACE(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalMPUBAndTrace(client, params, true)
}

func (p *protocolV2) MPUB(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	return p.internalMPUBAndTrace(client, params, false)
}

func getTracedReponse(messageBodyBuffer *bytes.Buffer, id nsqd.MessageID, traceID uint64, offset nsqd.BackendOffset, rawSize int32) ([]byte, error) {
	var buf []byte
	// pub with trace will return OK+16BYTES ID+8bytes offset of the disk queue + 4bytes raw size of disk queue data.
	retLen := 2 + 16 + 8 + 4
	if len(messageBodyBuffer.Bytes()) >= retLen {
		buf = messageBodyBuffer.Bytes()[:retLen]
	} else {
		buf = make([]byte, retLen)
	}
	copy(buf[:2], okBytes)
	pos := 2
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(id))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:pos+nsqd.MsgTraceIDLength], uint64(traceID))
	pos += nsqd.MsgTraceIDLength
	binary.BigEndian.PutUint64(buf[pos:pos+8], uint64(offset))
	pos += 8
	binary.BigEndian.PutUint32(buf[pos:pos+4], uint32(rawSize))
	return buf, nil
}

func (p *protocolV2) internalPubAndTrace(client *nsqd.ClientV2, params [][]byte, traceEnable bool) ([]byte, error) {
	bodyLen, topic, err := p.preparePub(client, params, p.ctx.getOpts().MaxMsgSize)
	if err != nil {
		return nil, err
	}
	if bodyLen <= 8 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE", "missing trace id with pub trace")
	}

	messageBodyBuffer := topic.BufferPoolGet(int(bodyLen))
	messageBody := messageBodyBuffer.Bytes()[:bodyLen]
	defer topic.BufferPoolPut(messageBodyBuffer)

	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "failed to read message body")
	}

	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	var traceID uint64
	var realBody []byte
	if traceEnable {
		traceID = binary.BigEndian.Uint64(messageBody[:8])
		realBody = messageBody[8:]
	} else {
		realBody = messageBody
	}
	if p.ctx.checkForMasterWrite(topicName, partition) {
		id, offset, rawSize, _, err := p.ctx.PutMessage(topic, realBody, traceID)
		//p.ctx.setHealth(err)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)
			if clusterErr, ok := err.(*consistence.CoordErr); ok {
				if !clusterErr.IsLocalErr() {
					return nil, protocol.NewClientErr(err, FailedOnNotWritable, "")
				}
			}
			return nil, protocol.NewClientErr(err, "E_PUB_FAILED", err.Error())
		}
		if !traceEnable {
			return okBytes, nil
		}
		return getTracedReponse(messageBodyBuffer, id, traceID, offset, rawSize)
	} else {
		//forward to master of topic
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), client.RemoteAddr)
		topic.DisableForSlave()
		return nil, protocol.NewClientErr(err, FailedOnNotLeader, "")
	}
}

func (p *protocolV2) internalMPUBAndTrace(client *nsqd.ClientV2, params [][]byte, traceEnable bool) ([]byte, error) {
	_, topic, err := p.preparePub(client, params, p.ctx.getOpts().MaxBodySize)
	if err != nil {
		return nil, err
	}

	messages, buffers, err := readMPUB(client.Reader, client.LenSlice, topic,
		p.ctx.getOpts().MaxMsgSize, traceEnable)

	defer func() {
		for _, b := range buffers {
			topic.BufferPoolPut(b)
		}
	}()
	if err != nil {
		return nil, err
	}

	topicName := topic.GetTopicName()
	partition := topic.GetTopicPart()
	if p.ctx.checkForMasterWrite(topicName, partition) {
		id, offset, rawSize, err := p.ctx.PutMessages(topic, messages)
		//p.ctx.setHealth(err)
		if err != nil {
			nsqd.NsqLogger().LogErrorf("topic %v put message failed: %v", topic.GetFullName(), err)

			if clusterErr, ok := err.(*consistence.CoordErr); ok {
				if !clusterErr.IsLocalErr() {
					return nil, protocol.NewClientErr(err, FailedOnNotWritable, "")
				}
			}
			return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", err.Error())
		}
		if !traceEnable {
			return okBytes, nil
		}
		return getTracedReponse(buffers[0], id, 0, offset, rawSize)
	} else {
		//forward to master of topic
		nsqd.NsqLogger().LogDebugf("should put to master: %v, from %v",
			topic.GetFullName(), client.RemoteAddr)
		topic.DisableForSlave()
		return nil, protocol.NewClientErr(err, FailedOnNotLeader, "")
	}
}

func (p *protocolV2) TOUCH(client *nsqd.ClientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
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

func readMPUB(r io.Reader, tmp []byte, topic *nsqd.Topic, maxMessageSize int64, traceEnable bool) ([]*nsqd.Message, []*bytes.Buffer, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	if numMessages <= 0 {
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
		msgBody := b.Bytes()[:messageSize]
		buffers = append(buffers, b)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, buffers, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		traceID := uint64(0)
		var realBody []byte
		if traceEnable {
			if messageSize <= nsqd.MsgTraceIDLength {
				return nil, buffers, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
					fmt.Sprintf("MPUB invalid message(%d) body size %d for tracing", i, messageSize))
			}
			traceID = binary.BigEndian.Uint64(msgBody[:8])
			realBody = msgBody[8:]
		} else {
			realBody = msgBody
		}
		msg := nsqd.NewMessage(0, realBody)
		msg.TraceID = traceID
		messages = append(messages, msg)
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
