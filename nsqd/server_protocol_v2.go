package main

import (
	"../nsq"
	"../util"
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"strconv"
	"strings"
)

type ServerProtocolV2 struct {
	nsq.ProtocolV2
}

func init() {
	// BigEndian client byte sequence "  V2"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.ProtocolV2Magic))
	binary.Read(buf, binary.BigEndian, &magicInt)
	Protocols[magicInt] = &ServerProtocolV2{}
}

func (p *ServerProtocolV2) IOLoop(client nsq.StatefulReadWriter) error {
	var err error
	var line string

	clientExitChan := make(chan int)
	client.SetState("state", nsq.ClientStateV2Init)
	client.SetState("exit_chan", clientExitChan)

	err = nil
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.Replace(line, "\n", "", -1)
		line = strings.Replace(line, "\r", "", -1)
		params := strings.Split(line, " ")

		log.Printf("PROTOCOL(V2): %#v", params)

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

	clientExitChan <- 1

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

func (p *ServerProtocolV2) PushMessages(client nsq.StatefulReadWriter) {
	var err error

	client.SetState("ready_count", 0)

	readyStateChanInterface, _ := client.GetState("ready_state_chan")
	readyStateChan := readyStateChanInterface.(chan int)

	clientExitChanInterface, _ := client.GetState("exit_chan")
	clientExitChan := clientExitChanInterface.(chan int)

	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*Channel)

	for {
		readyCountInterface, _ := client.GetState("ready_count")
		readyCount := readyCountInterface.(int)
		if readyCount > 0 {
			select {
			case count := <-readyStateChan:
				client.SetState("ready_count", count)
			case msg := <-channel.ClientMessageChan:
				client.SetState("ready_count", readyCount-1)

				log.Printf("PROTOCOL(V2): writing msg(%s) to client(%s) - %s",
					util.UuidToStr(msg.Uuid), client.String(), string(msg.Body))

				data, err := msg.Encode()
				if err != nil {
					goto exit
				}

				clientData, err := p.Frame(nsq.FrameTypeMessage, data)
				if err != nil {
					goto exit
				}

				client.Write(clientData)
			case <-clientExitChan:
				goto exit
			}
		} else {
			select {
			case count := <-readyStateChan:
				client.SetState("ready_count", count)
			case <-clientExitChan:
				goto exit
			}
		}
	}

exit:
	if err != nil {
		log.Printf("PROTOCOL(V2): PushMessages error - %s", err.Error())
	}
}

func (p *ServerProtocolV2) SUB(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	if state, _ := client.GetState("state"); state.(int) != nsq.ClientStateV2Init {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) < 3 {
		return nil, nsq.ClientErrV2Invalid
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return nil, nsq.ClientErrV2BadTopic
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return nil, nsq.ClientErrV2BadChannel
	}

	readyStateChan := make(chan int)
	client.SetState("ready_state_chan", readyStateChan)

	topic := GetTopic(topicName)
	client.SetState("channel", topic.GetChannel(channelName))

	client.SetState("state", nsq.ClientStateV2Subscribed)

	go p.PushMessages(client)

	return nil, nil
}

func (p *ServerProtocolV2) RDY(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	var err error

	state, _ := client.GetState("state")
	if state.(int) == nsq.ClientStateV2Closing {
		// just ignore ready changes on a closing channel
		log.Printf("PROTOCOL(V2): ignoring RDY after CLS in state ClientStateV2Closing")
		return nil, nil
	}

	if state.(int) != nsq.ClientStateV2Subscribed {
		return nil, nsq.ClientErrV2Invalid
	}

	count := 1
	if len(params) > 1 {
		count, err = strconv.Atoi(params[1])
		if err != nil {
			return nil, err
		}
	}

	if count > 1000 {
		return nil, nsq.ClientErrV2Invalid
	}

	readyStateChanInterface, _ := client.GetState("ready_state_chan")
	readyStateChan := readyStateChanInterface.(chan int)
	readyStateChan <- count

	return nil, nil
}

func (p *ServerProtocolV2) FIN(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	state, _ := client.GetState("state")
	if state.(int) != nsq.ClientStateV2Subscribed && state.(int) != nsq.ClientStateV2Closing {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) < 2 {
		return nil, nsq.ClientErrV2Invalid
	}

	uuidStr := params[1]
	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*Channel)
	err := channel.FinishMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (p *ServerProtocolV2) REQ(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	state, _ := client.GetState("state")
	if state.(int) != nsq.ClientStateV2Subscribed && state.(int) != nsq.ClientStateV2Closing {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) < 2 {
		return nil, nsq.ClientErrV2Invalid
	}

	uuidStr := params[1]
	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*Channel)
	err := channel.RequeueMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (p *ServerProtocolV2) CLS(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	if state, _ := client.GetState("state"); state.(int) != nsq.ClientStateV2Subscribed {
		return nil, nsq.ClientErrV2Invalid
	}

	if len(params) > 1 {
		return nil, nsq.ClientErrV2Invalid
	}

	// Force the client into ready 0
	readyStateChanInterface, _ := client.GetState("ready_state_chan")
	readyStateChan := readyStateChanInterface.(chan int)
	readyStateChan <- 0

	// mark this client as closing
	client.SetState("state", nsq.ClientStateV2Closing)

	// TODO start a timer to actually close the channel (in case the client doesn't do it first)

	// send a FrameTypeCloseWait response
	clientData, err := p.Frame(nsq.FrameTypeCloseWait, nil)
	if err != nil {
		log.Printf("error writing FrameTypeCloseWait")
	}
	client.Write(clientData)

	return nil, nil
}
