package main

import (
	"../nsq"
	"../util"
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"reflect"
	"strconv"
	"strings"
)

type ServerProtocolV1 struct {
	nsq.ProtocolV1
}

func init() {
	// BigEndian client byte sequence "  V1"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.ProtocolV1Magic))
	binary.Read(buf, binary.BigEndian, &magicInt)
	Protocols[magicInt] = &ServerProtocolV1{}
}

func (p *ServerProtocolV1) IOLoop(client nsq.StatefulReadWriter) error {
	var err error
	var line string

	client.SetState("state", nsq.ClientStateV1Init)

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

		log.Printf("PROTOCOL(V1): %#v", params)

		response, err := p.Execute(client, params...)
		if err != nil {
			_, err = client.Write([]byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			_, err = client.Write(response)
			if err != nil {
				break
			}
		}
	}

	return err
}

func (p *ServerProtocolV1) Execute(client nsq.StatefulReadWriter, params ...string) ([]byte, error) {
	var err error
	var response []byte

	typ := reflect.TypeOf(p)
	args := make([]reflect.Value, 3)
	args[0] = reflect.ValueOf(p)
	args[1] = reflect.ValueOf(client)

	cmd := strings.ToUpper(params[0])

	// use reflection to call the appropriate method for this 
	// command on the protocol object
	if method, ok := typ.MethodByName(cmd); ok {
		args[2] = reflect.ValueOf(params)
		returnValues := method.Func.Call(args)
		response = nil
		if !returnValues[0].IsNil() {
			response = returnValues[0].Interface().([]byte)
		}
		err = nil
		if !returnValues[1].IsNil() {
			err = returnValues[1].Interface().(error)
		}

		return response, err
	}

	return nil, nsq.ClientErrV1Invalid
}

func (p *ServerProtocolV1) SUB(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	if state, _ := client.GetState("state"); state.(int) != nsq.ClientStateV1Init {
		return nil, nsq.ClientErrV1Invalid
	}

	if len(params) < 3 {
		return nil, nsq.ClientErrV1Invalid
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return nil, nsq.ClientErrV1BadTopic
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return nil, nsq.ClientErrV1BadChannel
	}

	client.SetState("state", nsq.ClientStateV1WaitGet)

	topic := GetTopic(topicName)
	client.SetState("channel", topic.GetChannel(channelName))

	return nil, nil
}

func (p *ServerProtocolV1) GET(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	var err error
	var buf bytes.Buffer

	if state, _ := client.GetState("state"); state.(int) != nsq.ClientStateV1WaitGet {
		return nil, nsq.ClientErrV1Invalid
	}

	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*Channel)
	// this blocks until a message is ready
	msg := <-channel.ClientMessageChan
	if msg == nil {
		log.Printf("ERROR: msg == nil")
		return nil, nsq.ClientErrV1BadMessage
	}

	uuidStr := util.UuidToStr(msg.Uuid())

	log.Printf("PROTOCOL(V1): writing msg(%s) to client(%s) - %s", uuidStr, client.String(), string(msg.Body()))

	_, err = buf.Write([]byte(uuidStr))
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(msg.Body())
	if err != nil {
		return nil, err
	}

	client.SetState("state", nsq.ClientStateV1WaitResponse)

	return buf.Bytes(), nil
}

func (p *ServerProtocolV1) FIN(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	if state, _ := client.GetState("state"); state.(int) != nsq.ClientStateV1WaitResponse {
		return nil, nsq.ClientErrV1Invalid
	}

	if len(params) < 2 {
		return nil, nsq.ClientErrV1Invalid
	}

	uuidStr := params[1]
	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*Channel)
	err := channel.FinishMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState("state", nsq.ClientStateV1WaitGet)

	return nil, nil
}

func (p *ServerProtocolV1) REQ(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	if state, _ := client.GetState("state"); state.(int) != nsq.ClientStateV1WaitResponse {
		return nil, nsq.ClientErrV1Invalid
	}

	if len(params) < 2 {
		return nil, nsq.ClientErrV1Invalid
	}

	uuidStr := params[1]
	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*Channel)
	err := channel.RequeueMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState("state", nsq.ClientStateV1WaitGet)

	return nil, nil
}

func (p *ServerProtocolV1) PUB(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	if len(params) < 3 {
		return nil, nsq.ClientErrV1Invalid
	}

	topicName := params[1]
	messageSize, err := strconv.Atoi(params[2])
	if err != nil {
		return nil, err
	}

	messageBody := make([]byte, messageSize)
	_, err = io.ReadFull(client, messageBody)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(<-util.UuidChan)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(messageBody)
	if err != nil {
		return nil, err
	}

	topic := GetTopic(topicName)
	topic.PutMessage(nsq.NewMessage(buf.Bytes()))

	return []byte("OK"), nil
}
