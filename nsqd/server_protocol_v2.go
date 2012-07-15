package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"strconv"
	"strings"
	"time"
)

const maxTimeout = time.Hour
const maxReadyCount = 2500

type ServerProtocolV2 struct {
	nsq.Protocol
}

func init() {
	// BigEndian client byte sequence "  V2"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.ProtocolV2Magic))
	binary.Read(buf, binary.BigEndian, &magicInt)
	Protocols[magicInt] = &ServerProtocolV2{}
}

func (p *ServerProtocolV2) IOLoop(client *nsq.ServerClient) error {
	var err error
	var line string

	client.State = nsq.ClientStateV2Init

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
			log.Printf("PROTOCOL(V2): [%s] %#v", client.String(), params)
		}

		response, err := nsq.ProtocolExecute(p, client, params...)
		if err != nil {
			clientData, err := p.Frame(nsq.FrameTypeError, []byte(err.Error()))
			if err != nil {
				break
			}

			_, err = client.Write(clientData)
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			clientData, err := p.Frame(nsq.FrameTypeResponse, response)
			if err != nil {
				break
			}

			_, err = client.Write(clientData)
			if err != nil {
				break
			}
		}
	}

	// TODO: gracefully send clients the close signal
	close(client.ExitChan)

	return err
}

func (p *ServerProtocolV2) Frame(frameType int32, data []byte) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	err = binary.Write(&buf, binary.BigEndian, &frameType)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(data)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (p *ServerProtocolV2) PushMessages(client *nsq.ServerClient) {
	var err error

	channel := client.Channel.(*Channel)

	for {
		if !client.IsReadyForMessages() {
			// wait for a change in state
			select {
			case <-client.ReadyStateChange:
				continue
			case <-client.ExitChan:
				goto exit
			}
		} else {
			select {
			case <-client.ReadyStateChange:
			case msg := <-channel.clientMessageChan:

				if *verbose {
					log.Printf("PROTOCOL(V2): writing msg(%s) to client(%s) - %s",
						msg.Id, client.String(), msg.Body)
				}

				data, err := msg.Encode()
				if err != nil {
					goto exit
				}

				clientData, err := p.Frame(nsq.FrameTypeMessage, data)
				if err != nil {
					goto exit
				}

				channel.StartInFlightTimeout(msg, client)
				client.SendingMessage()

				_, err = client.Write(clientData)
				if err != nil {
					goto exit
				}
			case <-client.ExitChan:
				goto exit
			}
		}
	}

exit:
	channel.RemoveClient(client)
	if err != nil {
		log.Printf("PROTOCOL(V2): PushMessages error - %s", err.Error())
	}
}

func (p *ServerProtocolV2) SUB(client *nsq.ServerClient, params []string) ([]byte, error) {
	if client.State != nsq.ClientStateV2Init {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) < 3 {
		return nil, nsq.ClientErrV2Invalid
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return nil, nsq.ClientErrV2BadTopic
	}

	if len(topicName) > nsq.MaxNameLength {
		return nil, nsq.ClientErrV2BadTopic
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return nil, nsq.ClientErrV2BadChannel
	}

	if len(channelName) > nsq.MaxNameLength {
		return nil, nsq.ClientErrV2BadChannel
	}

	topic := nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client)

	client.Channel = channel
	client.State = nsq.ClientStateV2Subscribed

	go p.PushMessages(client)

	return nil, nil
}

func (p *ServerProtocolV2) RDY(client *nsq.ServerClient, params []string) ([]byte, error) {
	var err error

	if client.State == nsq.ClientStateV2Closing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): ignoring RDY after CLS in state ClientStateV2Closing")
		return nil, nil
	}

	if client.State != nsq.ClientStateV2Subscribed {
		return nil, nsq.ClientErrV2Invalid
	}

	count := 1
	if len(params) > 1 {
		count, err = strconv.Atoi(params[1])
		if err != nil {
			return nil, nsq.ClientErrV2Invalid
		}
	}

	if count > maxReadyCount {
		return nil, nsq.ClientErrV2Invalid
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *ServerProtocolV2) FIN(client *nsq.ServerClient, params []string) ([]byte, error) {
	if client.State != nsq.ClientStateV2Subscribed && client.State != nsq.ClientStateV2Closing {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) < 2 {
		return nil, nsq.ClientErrV2Invalid
	}

	idStr := params[1]
	err := client.Channel.(*Channel).FinishMessage([]byte(idStr))
	if err != nil {
		return nil, nsq.ClientErrV2FinishFailed
	}

	client.FinishMessage()

	return nil, nil
}

func (p *ServerProtocolV2) REQ(client *nsq.ServerClient, params []string) ([]byte, error) {
	if client.State != nsq.ClientStateV2Subscribed && client.State != nsq.ClientStateV2Closing {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) < 3 {
		return nil, nsq.ClientErrV2Invalid
	}

	idStr := params[1]
	timeoutMs, err := strconv.Atoi(params[2])
	if err != nil {
		return nil, nsq.ClientErrV2Invalid
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > maxTimeout {
		return nil, nsq.ClientErrV2Invalid
	}

	err = client.Channel.(*Channel).RequeueMessage([]byte(idStr), timeoutDuration)
	if err != nil {
		return nil, nsq.ClientErrV2RequeueFailed
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *ServerProtocolV2) CLS(client *nsq.ServerClient, params []string) ([]byte, error) {
	if client.State != nsq.ClientStateV2Subscribed {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) > 1 {
		return nil, nsq.ClientErrV2Invalid
	}

	// Force the client into ready 0
	client.SetReadyCount(0)

	// mark this client as closing
	client.State = nsq.ClientStateV2Closing

	//TODO: start a timer to actually close the channel (in case the client doesn't do it first)

	// send a FrameTypeCloseWait response
	clientData, err := p.Frame(nsq.FrameTypeCloseWait, nil)
	if err != nil {
		return nil, nsq.ClientErrV2CloseFailed
	}

	_, err = client.Write(clientData)
	if err != nil {
		return nil, nsq.ClientErrV2CloseFailed
	}

	return nil, nil
}
