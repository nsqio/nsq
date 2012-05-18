package protocol

import (
	"../message"
	"../util"
	"bufio"
	"log"
	"reflect"
	"strings"
	"bytes"
)

const (
	ClientInit         = 0
	ClientWaitGet      = 1
	ClientWaitResponse = 2
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

type ProtocolV1 struct {}

func (p *ProtocolV1) IOLoop(client StatefulReadWriter) error {
	var err error
	var line string

	client.SetState("state", ClientInit)

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
	if state, _ := client.GetState("state"); state.(int) != ClientInit {
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

	client.SetState("state", ClientWaitGet)

	topic := message.GetTopic(topicName)
	client.SetState("channel", topic.GetChannel(channelName))

	return nil, nil
}

func (p *ProtocolV1) GET(client StatefulReadWriter, params []string) ([]byte, error) {
	var err error
	var buf bytes.Buffer

	if state, _ := client.GetState("state"); state.(int) != ClientWaitGet {
		return nil, ClientErrInvalid
	}

	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*message.Channel)
	// this blocks until a message is ready
	msg := channel.GetMessage(true)
	if msg == nil {
		log.Printf("ERROR: msg == nil")
		return nil, ClientErrBadMessage
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

	client.SetState("state", ClientWaitResponse)

	return buf.Bytes(), nil
}

func (p *ProtocolV1) FIN(client StatefulReadWriter, params []string) ([]byte, error) {
	if state, _ := client.GetState("state"); state.(int) != ClientWaitResponse {
		return nil, ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, ClientErrInvalid
	}

	uuidStr := params[1]
	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*message.Channel)
	err := channel.FinishMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState("state", ClientWaitGet)

	return nil, nil
}

func (p *ProtocolV1) REQ(client StatefulReadWriter, params []string) ([]byte, error) {
	if state, _ := client.GetState("state"); state.(int) != ClientWaitResponse {
		return nil, ClientErrInvalid
	}

	if len(params) < 2 {
		return nil, ClientErrInvalid
	}

	uuidStr := params[1]
	channelInterface, _ := client.GetState("channel")
	channel := channelInterface.(*message.Channel)
	err := channel.RequeueMessage(uuidStr)
	if err != nil {
		return nil, err
	}

	client.SetState("state", ClientWaitGet)

	return nil, nil
}

func (p *ProtocolV1) PUB(client StatefulReadWriter, params []string) ([]byte, error) {
	var buf bytes.Buffer
	var err error

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
