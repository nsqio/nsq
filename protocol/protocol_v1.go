package protocol

import (
	"../message"
	"../util"
	"bufio"
	"bytes"
	"log"
	"reflect"
	"strings"
)

const (
	ClientInit         = 0
	ClientWaitGet      = 1
	ClientWaitAck      = 2
	ClientWaitResponse = 3
)

var (
	ClientErrInvalid    = ClientError{"E_INVALID"}
	ClientErrBadTopic   = ClientError{"E_BAD_TOPIC"}
	ClientErrBadChannel = ClientError{"E_BAD_CHANNEL"}
	ClientErrBadMessage = ClientError{"E_BAD_MESSAGE"}
)

func init() {
	// BigEndian client byte sequence "  V1"
	Protocols[538990129] = &ProtocolV1{}
}

type ProtocolV1 struct {
	channel *message.Channel
}

func (p *ProtocolV1) IOLoop(client StatefulReadWriter) error {
	var err error
	var line string

	client.SetState(ClientInit)

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

		log.Printf("PROTOCOL: %#v", params)

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

func (p *ProtocolV1) Execute(client StatefulReadWriter, params ...string) ([]byte, error) {
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

	return nil, ClientErrInvalid
}

func (p *ProtocolV1) SUB(client StatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientInit {
		return nil, ClientErrInvalid
	}

	if len(params) < 3 {
		return nil, ClientErrInvalid
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return nil, ClientErrBadTopic
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return nil, ClientErrBadChannel
	}

	client.SetState(ClientWaitGet)

	topic := message.GetTopic(topicName)
	p.channel = topic.GetChannel(channelName)

	return nil, nil
}

func (p *ProtocolV1) GET(client StatefulReadWriter, params []string) ([]byte, error) {
	var err error

	if client.GetState() != ClientWaitGet {
		return nil, ClientErrInvalid
	}

	// this blocks until a message is ready
	msg := p.channel.GetMessage()
	if msg == nil {
		log.Printf("ERROR: msg == nil")
		return nil, ClientErrBadMessage
	}

	uuidStr := util.UuidToStr(msg.Uuid())

	log.Printf("PROTOCOL: writing msg(%s) to client(%s) - %s", uuidStr, client.String(), string(msg.Body()))

	buf := bytes.NewBuffer([]byte(uuidStr))
	_, err = buf.Write(msg.Body())
	if err != nil {
		return nil, err
	}

	client.SetState(ClientWaitAck)

	return buf.Bytes(), nil
}

func (p *ProtocolV1) ACK(client StatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientWaitAck {
		return nil, ClientErrInvalid
	}

	client.SetState(ClientWaitResponse)

	return nil, nil
}

func (p *ProtocolV1) FIN(client StatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientWaitResponse {
		return nil, ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, ClientErrInvalid
	}

	uuidStr := params[1]
	err := p.channel.FinishMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState(ClientWaitGet)

	return nil, nil
}

func (p *ProtocolV1) REQ(client StatefulReadWriter, params []string) ([]byte, error) {
	if client.GetState() != ClientWaitResponse {
		return nil, ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, ClientErrInvalid
	}

	uuidStr := params[1]
	err := p.channel.RequeueMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState(ClientWaitGet)

	return nil, nil
}

func (p *ProtocolV1) PUB(client StatefulReadWriter, params []string) ([]byte, error) {
	var buf bytes.Buffer
	var err error

	// pub's are fake clients. They don't get to ClientInit
	if client.GetState() != -1 {
		return nil, ClientErrInvalid
	}

	if len(params) < 3 {
		return nil, ClientErrInvalid
	}

	topicName := params[1]
	body := []byte(params[2])

	_, err = buf.Write(<-util.UuidChan)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(body)
	if err != nil {
		return nil, err
	}

	topic := message.GetTopic(topicName)
	topic.PutMessage(message.NewMessage(buf.Bytes()))

	return []byte("OK"), nil
}
