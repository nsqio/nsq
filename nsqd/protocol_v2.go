package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const maxTimeout = time.Hour
const maxReadyCount = 2500

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
	var line string

	client := NewClientV2(conn)
	atomic.StoreInt32(&client.State, nsq.StateInit)

	err = nil
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		if *verbose {
			log.Printf("PROTOCOL(V2): [%s] %#v", client.RemoteAddr().String(), params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			clientData, err := nsq.Frame(nsq.FrameTypeError, []byte(err.Error()))
			if err != nil {
				break
			}

			err = p.Write(client, clientData)
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			clientData, err := nsq.Frame(nsq.FrameTypeResponse, response)
			if err != nil {
				break
			}

			err = p.Write(client, clientData)
			if err != nil {
				break
			}
		}
	}

	log.Printf("PROTOCOL(V2): [%s] exiting ioloop", client.RemoteAddr().String())
	// TODO: gracefully send clients the close signal
	close(client.ExitChan)

	return err
}

func (p *ProtocolV2) Write(client *ClientV2, data []byte) error {
	attempts := 0
	for {
		client.Lock()
		client.SetWriteDeadline(time.Now().Add(time.Second))
		_, err := nsq.SendResponse(client, data)
		client.Unlock()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				attempts++
				if attempts == 3 {
					return errors.New("E_WRITE_TIMEOUT")
				}
				continue
			}
			return err
		}
		break
	}
	return nil
}

func (p *ProtocolV2) Exec(client *ClientV2, params []string) ([]byte, error) {
	switch params[0] {
	case "SUB":
		return p.SUB(client, params)
	case "RDY":
		return p.RDY(client, params)
	case "FIN":
		return p.FIN(client, params)
	case "REQ":
		return p.REQ(client, params)
	case "CLS":
		return p.CLS(client, params)
	}
	return nil, nsq.ClientErrInvalid
}

func (p *ProtocolV2) messagePump(client *ClientV2) {
	var err error

	for {
		if !client.IsReadyForMessages() {
			// wait for a change in state
			select {
			case <-client.ReadyStateChan:
			case <-client.ExitChan:
				goto exit
			}
		} else {
			select {
			case <-client.ReadyStateChan:
			case msg := <-client.Channel.clientMsgChan:
				if *verbose {
					log.Printf("PROTOCOL(V2): writing msg(%s) to client(%s) - %s",
						msg.Id, client.RemoteAddr().String(), msg.Body)
				}

				data, err := msg.Encode()
				if err != nil {
					goto exit
				}

				clientData, err := nsq.Frame(nsq.FrameTypeMessage, data)
				if err != nil {
					goto exit
				}

				client.Channel.StartInFlightTimeout(msg, client)
				client.SendingMessage()

				err = p.Write(client, clientData)
				if err != nil {
					goto exit
				}
			case <-client.ExitChan:
				goto exit
			}
		}
	}

exit:
	log.Printf("PROTOCOL(V2): [%s] exiting messagePump", client.RemoteAddr().String())
	client.Channel.RemoveClient(client)
	if err != nil {
		log.Printf("PROTOCOL(V2): messagePump error - %s", err.Error())
	}
}

func (p *ProtocolV2) SUB(client *ClientV2, params []string) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateInit {
		return nil, nsq.ClientErrInvalid
	}

	if len(params) < 3 {
		return nil, nsq.ClientErrInvalid
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return nil, nsq.ClientErrBadTopic
	}

	if len(topicName) > nsq.MaxTopicNameLength {
		return nil, nsq.ClientErrBadTopic
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return nil, nsq.ClientErrBadChannel
	}

	if len(channelName) > nsq.MaxChannelNameLength {
		return nil, nsq.ClientErrBadChannel
	}

	if len(params) == 5 {
		client.ShortIdentifier = params[3]
		client.LongIdentifier = params[4]
	}

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client)

	client.Channel = channel
	atomic.StoreInt32(&client.State, nsq.StateSubscribed)

	go p.messagePump(client)

	return nil, nil
}

func (p *ProtocolV2) RDY(client *ClientV2, params []string) ([]byte, error) {
	var err error

	state := atomic.LoadInt32(&client.State)

	if state == nsq.StateClosing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): ignoring RDY after CLS in state ClientStateV2Closing")
		return nil, nil
	}

	if state != nsq.StateSubscribed {
		return nil, nsq.ClientErrInvalid
	}

	count := 1
	if len(params) > 1 {
		count, err = strconv.Atoi(params[1])
		if err != nil {
			return nil, nsq.ClientErrInvalid
		}
	}

	if count > maxReadyCount {
		log.Printf("PROTOCOL(V2): client(%s) sent ready count %d. Thats over the max of %d",
			client, count, maxReadyCount)
		count = maxReadyCount
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *ProtocolV2) FIN(client *ClientV2, params []string) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, nsq.ClientErrInvalid
	}

	idStr := params[1]
	err := client.Channel.FinishMessage(client, []byte(idStr))
	if err != nil {
		return nil, nsq.ClientErrFinishFailed
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *ProtocolV2) REQ(client *ClientV2, params []string) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != nsq.StateSubscribed && state != nsq.StateClosing {
		return nil, nsq.ClientErrInvalid
	}

	if len(params) < 3 {
		return nil, nsq.ClientErrInvalid
	}

	idStr := params[1]
	timeoutMs, err := strconv.Atoi(params[2])
	if err != nil {
		return nil, nsq.ClientErrInvalid
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > maxTimeout {
		return nil, nsq.ClientErrInvalid
	}

	err = client.Channel.RequeueMessage(client, []byte(idStr), timeoutDuration)
	if err != nil {
		return nil, nsq.ClientErrRequeueFailed
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *ProtocolV2) CLS(client *ClientV2, params []string) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != nsq.StateSubscribed {
		return nil, nsq.ClientErrInvalid
	}

	if len(params) > 1 {
		return nil, nsq.ClientErrInvalid
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}
