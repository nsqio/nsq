package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"strconv"
	"strings"
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
	client.State = nsq.StateInit

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

			// TODO: these writes need to be synchronized
			_, err = nsq.SendResponse(client, clientData)
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

			// TODO: these writes need to be synchronized
			_, err = nsq.SendResponse(client, clientData)
			if err != nil {
				break
			}
		}
	}

	// TODO: gracefully send clients the close signal
	close(client.ExitChan)

	return err
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

func (p *ProtocolV2) PushMessages(client *ClientV2) {
	var err error

	for {
		if !client.IsReadyForMessages() {
			// wait for a change in state
			select {
			case <-client.ReadyStateChange:
			case <-client.ExitChan:
				goto exit
			}
		} else {
			select {
			case <-client.ReadyStateChange:
			case msg := <-client.Channel.clientMessageChan:
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

				// TODO: these writes need to be synchronized
				_, err = nsq.SendResponse(client, clientData)
				if err != nil {
					goto exit
				}
			case <-client.ExitChan:
				goto exit
			}
		}
	}

exit:
	client.Channel.RemoveClient(client)
	if err != nil {
		log.Printf("PROTOCOL(V2): PushMessages error - %s", err.Error())
	}
}

func (p *ProtocolV2) SUB(client *ClientV2, params []string) ([]byte, error) {
	if client.State != nsq.StateInit {
		return nil, nsq.ClientErrInvalid
	}

	if len(params) < 3 {
		return nil, nsq.ClientErrInvalid
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return nil, nsq.ClientErrBadTopic
	}

	if len(topicName) > MaxNameLength {
		return nil, nsq.ClientErrBadTopic
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return nil, nsq.ClientErrBadChannel
	}

	if len(channelName) > MaxNameLength {
		return nil, nsq.ClientErrBadChannel
	}

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client)

	client.Channel = channel
	client.State = nsq.StateSubscribed

	go p.PushMessages(client)

	return nil, nil
}

func (p *ProtocolV2) RDY(client *ClientV2, params []string) ([]byte, error) {
	var err error

	if client.State == nsq.StateClosing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): ignoring RDY after CLS in state ClientStateV2Closing")
		return nil, nil
	}

	if client.State != nsq.StateSubscribed {
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
	if client.State != nsq.StateSubscribed && client.State != nsq.StateClosing {
		return nil, nsq.ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, nsq.ClientErrInvalid
	}

	idStr := params[1]
	err := client.Channel.FinishMessage([]byte(idStr))
	if err != nil {
		return nil, nsq.ClientErrFinishFailed
	}

	client.FinishMessage()

	return nil, nil
}

func (p *ProtocolV2) REQ(client *ClientV2, params []string) ([]byte, error) {
	if client.State != nsq.StateSubscribed && client.State != nsq.StateClosing {
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

	err = client.Channel.RequeueMessage([]byte(idStr), timeoutDuration)
	if err != nil {
		return nil, nsq.ClientErrRequeueFailed
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *ProtocolV2) CLS(client *ClientV2, params []string) ([]byte, error) {
	if client.State != nsq.StateSubscribed {
		return nil, nsq.ClientErrInvalid
	}

	if len(params) > 1 {
		return nil, nsq.ClientErrInvalid
	}

	// Force the client into ready 0
	client.SetReadyCount(0)

	// mark this client as closing
	client.State = nsq.StateClosing

	// TODO: start a timer to actually close the channel (in case the client doesn't do it first)

	return []byte("CLOSE_WAIT"), nil
}
